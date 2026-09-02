#!/usr/bin/env bash
# End-to-end test for nabla. Runs inside the nabla-dev:17 container
# (scripts/dev.sh test): installs the extension, starts a throwaway cluster on
# port 5499 and drives the scenarios with psql. Exits non-zero on the first
# failed assertion.
set -u

PG_BIN=/usr/lib/postgresql/17/bin
PGDATA=/tmp/nabla-pg
PORT=5499
DB=nabla_test
LOG=/tmp/nabla-pg.log
export PGHOST=/tmp

WAIT_MS=15000
FAILED=0
POLLER_PID=""
WRITER_PID=""
CLIENT_PID=""

pass() { printf 'PASS  %s\n' "$1"; }
fail() { printf 'FAIL  %s\n' "$1"; printf '      %s\n' "$2"; FAILED=1; }
die()  { printf 'FATAL %s\n' "$1"; [ -s /tmp/nabla-last.err ] && printf '      last error: %s\n' "$(tr '\n' ' ' < /tmp/nabla-last.err)"; exit 1; }

# psql wrapper: unaligned, tuples only, stop on error.
q() { psql -X -q -A -t -v ON_ERROR_STOP=1 -p "$PORT" -d "$DB" -c "$1" 2> >(tee /tmp/nabla-last.err >&2); }
# Run a statement expected to fail; print its stderr.
q_err() { psql -X -q -A -t -p "$PORT" -d "$DB" -c "$1" 2>&1 >/dev/null; }

# SQLSTATE of a failing statement (psql verbose error format: ERROR:  XX000: ...).
sqlstate_of() { psql -X -q -A -t -v VERBOSITY=verbose -p "$PORT" -d "$DB" -c "$1" 2>&1 >/dev/null | grep -oE '^ERROR:  [A-Z0-9]{5}:' | head -n 1 | cut -c 9-13; }
# Wait until the worker has built (or rebuilt) a view; die with its status otherwise.
await_ready() { # name
  local r
  r=$(q "SELECT nabla.await_ready('$1', 60000)")
  [ "$r" = t ] || die "view $1 did not become ready: $(q "SELECT status || ': ' || coalesce(last_error, '-') FROM nabla.views WHERE name = '$1'")"
}
# nabla.changes(view, after_seq) with the view's current epoch.
CH() { echo "nabla.changes('$1', $2, (SELECT epoch FROM nabla.status('$1')))"; }

assert_eq() { # label expected actual
  if [ "$2" = "$3" ]; then pass "$1"; else fail "$1" "expected [$2], got [$3]"; fi
}
assert_contains() { # label needle haystack
  case "$3" in *"$2"*) pass "$1" ;; *) fail "$1" "expected text containing [$2], got [$3]" ;; esac
}

cleanup() {
  [ -n "$POLLER_PID" ] && kill "$POLLER_PID" 2>/dev/null
  [ -n "$WRITER_PID" ] && kill "$WRITER_PID" 2>/dev/null
  [ -n "$CLIENT_PID" ] && { kill -CONT "$CLIENT_PID" 2>/dev/null; kill "$CLIENT_PID" 2>/dev/null; }
  if [ -f "$PGDATA/postmaster.pid" ]; then
    "$PG_BIN/pg_ctl" -D "$PGDATA" -m immediate stop >/dev/null 2>&1
  fi
  # Keep the server log for post-mortem analysis (target/ is a named volume).
  cp "$LOG" /work/target/nabla-pg.log 2>/dev/null || true
}
trap cleanup EXIT

wait_view() { # name -> asserts frontier reached pg_current_wal_lsn()
  local r
  r=$(q "SELECT nabla.wait_for('$1', pg_current_wal_lsn(), $WAIT_MS)")
  [ "$r" = "t" ] || die "wait_for('$1') timed out; server log tail:
$(tail -n 40 "$LOG")"
}

# --- build and install -------------------------------------------------------
echo "== installing extension"
cargo pgrx install --sudo --pg-config "$PG_BIN/pg_config" >/tmp/nabla-install.log 2>&1 \
  || die "cargo pgrx install failed: $(tail -n 30 /tmp/nabla-install.log)"

# --- cluster -----------------------------------------------------------------
echo "== starting cluster"
rm -rf "$PGDATA"
"$PG_BIN/initdb" -D "$PGDATA" -U "$(whoami)" --auth=trust >/dev/null 2>&1 || die "initdb failed"
cat >> "$PGDATA/postgresql.conf" <<EOF
port = $PORT
unix_socket_directories = '/tmp'
listen_addresses = ''
wal_level = logical
shared_preload_libraries = 'nabla'
nabla.database = '$DB'
nabla.retain_deltas = 50
nabla.poll_interval_ms = 50
nabla.max_apply_failures = 3
max_replication_slots = 4
max_wal_senders = 4
log_min_messages = warning
EOF
"$PG_BIN/pg_ctl" -D "$PGDATA" -l "$LOG" -w start >/dev/null || die "pg_ctl start failed: $(tail -n 30 "$LOG")"
createdb -p "$PORT" "$DB" || die "createdb failed"
q "CREATE EXTENSION nabla" >/dev/null || die "CREATE EXTENSION failed: $(tail -n 30 "$LOG")"
# The worker's first attempt ran before the database existed; restart so it
# connects cleanly instead of waiting for its 5 s restart delay.
"$PG_BIN/pg_ctl" -D "$PGDATA" -l "$LOG" -w restart >/dev/null || die "pg_ctl restart failed"
sleep 1
WORKERS=$(q "SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'nabla worker'")
assert_eq "worker is running" "1" "$WORKERS"
[ "$WORKERS" = "1" ] || { echo "      server log tail:"; tail -n 30 "$LOG" | sed 's/^/      /'; }

# --- 1. projection view ------------------------------------------------------
echo "== 1. projection view"
q "CREATE TABLE orders (id bigserial PRIMARY KEY, k int, amount numeric, status text)" >/dev/null
q "SELECT nabla.create_view('public.paid_orders', 'SELECT id, k, amount FROM orders WHERE status = ''paid''')" >/dev/null \
  || die "create_view(paid_orders) failed"
await_ready public.paid_orders
q "INSERT INTO orders (k, amount, status)
   SELECT i % 5, i * 1.5, CASE WHEN i % 2 = 0 THEN 'paid' ELSE 'new' END FROM generate_series(1, 20) i" >/dev/null
for id in 1 3 5; do q "UPDATE orders SET status = 'paid' WHERE id = $id" >/dev/null; done
for id in 2 4; do q "DELETE FROM orders WHERE id = $id" >/dev/null; done
wait_view public.paid_orders
DIFF=$(q "SELECT count(*) FROM (
  (SELECT id, k, amount FROM paid_orders EXCEPT SELECT id, k, amount FROM orders WHERE status = 'paid')
  UNION ALL
  (SELECT id, k, amount FROM orders WHERE status = 'paid' EXCEPT SELECT id, k, amount FROM paid_orders)) d")
assert_eq "projection view equals its query" "0" "$DIFF"
assert_eq "projection view row count" "11" "$(q "SELECT count(*) FROM paid_orders")"

# --- 2. aggregate view -------------------------------------------------------
echo "== 2. aggregate view"
q "ALTER TABLE orders REPLICA IDENTITY FULL" >/dev/null
q "SELECT nabla.create_view('public.orders_by_k', 'SELECT k, count(*) AS n, sum(amount) AS total FROM orders WHERE status = ''paid'' GROUP BY k')" >/dev/null \
  || die "create_view(orders_by_k) failed"
await_ready public.orders_by_k
q "INSERT INTO orders (k, amount, status) VALUES (7, 100, 'paid'), (7, 50, 'new'), (8, 1, 'paid')" >/dev/null
q "UPDATE orders SET status = 'paid' WHERE k = 7 AND status = 'new'" >/dev/null
q "UPDATE orders SET amount = amount + 10 WHERE k = 8" >/dev/null
q "UPDATE orders SET k = 9 WHERE id = 6" >/dev/null
q "BEGIN; DELETE FROM orders WHERE k = 8; INSERT INTO orders (k, amount, status) VALUES (8, 5, 'paid'); UPDATE orders SET status = 'new' WHERE id = 10; COMMIT" >/dev/null
q "DELETE FROM orders WHERE k = 3" >/dev/null
wait_view public.orders_by_k
DIFF=$(q "SELECT count(*) FROM (
  (SELECT k, n, total FROM orders_by_k EXCEPT SELECT k, count(*), sum(amount) FROM orders WHERE status = 'paid' GROUP BY k)
  UNION ALL
  (SELECT k, count(*), sum(amount) FROM orders WHERE status = 'paid' GROUP BY k EXCEPT SELECT k, n, total FROM orders_by_k)) d")
assert_eq "aggregate view equals its query" "0" "$DIFF"
assert_eq "no empty groups" "0" "$(q "SELECT count(*) FROM orders_by_k WHERE n = 0")"
wait_view public.paid_orders
DIFF=$(q "SELECT count(*) FROM (
  (SELECT id, k, amount FROM paid_orders EXCEPT SELECT id, k, amount FROM orders WHERE status = 'paid')
  UNION ALL
  (SELECT id, k, amount FROM orders WHERE status = 'paid' EXCEPT SELECT id, k, amount FROM paid_orders)) d")
assert_eq "projection view still equals its query" "0" "$DIFF"

# --- 3. atomicity ------------------------------------------------------------
echo "== 3. atomicity"
SEQ0=$(q "SELECT nabla.current_seq('public.orders_by_k')")
POLL_OUT=/tmp/nabla-poll.txt
: > "$POLL_OUT"
( for _ in $(seq 1 300); do
    psql -X -q -A -t -p "$PORT" -d "$DB" -c "SELECT coalesce((SELECT n FROM orders_by_k WHERE k = 999), 0)" >> "$POLL_OUT" 2>/dev/null
  done ) &
POLLER_PID=$!
q "BEGIN; INSERT INTO orders (k, amount, status) SELECT 999, i, 'paid' FROM generate_series(1, 5) i; COMMIT" >/dev/null
wait_view public.orders_by_k
wait "$POLLER_PID" 2>/dev/null; POLLER_PID=""
SEEN=$(sort -u "$POLL_OUT" | tr '\n' ' ' | sed 's/ *$//')
case "$SEEN" in
  "0"|"0 5"|"5") pass "group count for k=999 was only ever 0 or 5 (observed: $SEEN)" ;;
  *) fail "atomicity" "observed intermediate counts: $SEEN" ;;
esac
assert_eq "k=999 final count" "5" "$(q "SELECT n FROM orders_by_k WHERE k = 999")"
# 5 inserts into a fresh group are one source transaction: its net effect is one I.
assert_eq "a source transaction nets to one delta per group, with one lsn and xid" "1|1|1" \
  "$(q "SELECT count(*) || '|' || count(DISTINCT lsn) || '|' || count(DISTINCT xid) FROM $(CH public.orders_by_k $SEQ0)")"
assert_eq "last delta of the transaction is the final group row" "I|5" \
  "$(q "SELECT op || '|' || (row->>'n') FROM $(CH public.orders_by_k $SEQ0) ORDER BY seq DESC LIMIT 1")"

# --- 4. writers are not blocked ---------------------------------------------
echo "== 4. writers are not blocked"
psql -X -q -p "$PORT" -d "$DB" -c "BEGIN; INSERT INTO orders (k, amount, status) VALUES (1, 10, 'paid'); SELECT pg_sleep(3); COMMIT" >/dev/null 2>&1 &
WRITER_PID=$!
sleep 0.5
START_NS=$(date +%s%N)
if q "SET lock_timeout = '1s'; INSERT INTO orders (k, amount, status) VALUES (2, 20, 'paid')" >/dev/null 2>/tmp/nabla-b.err; then
  ELAPSED_MS=$(( ($(date +%s%N) - START_NS) / 1000000 ))
  if [ "$ELAPSED_MS" -lt 1000 ]; then pass "concurrent writer committed in ${ELAPSED_MS} ms while another transaction was open"
  else fail "writers not blocked" "commit took ${ELAPSED_MS} ms"; fi
else
  fail "writers not blocked" "insert failed: $(cat /tmp/nabla-b.err)"
fi
wait "$WRITER_PID" 2>/dev/null; WRITER_PID=""
wait_view public.orders_by_k
DIFF=$(q "SELECT count(*) FROM (
  (SELECT k, n, total FROM orders_by_k EXCEPT SELECT k, count(*), sum(amount) FROM orders WHERE status = 'paid' GROUP BY k)
  UNION ALL
  (SELECT k, count(*), sum(amount) FROM orders WHERE status = 'paid' GROUP BY k EXCEPT SELECT k, n, total FROM orders_by_k)) d")
assert_eq "aggregate view consistent after concurrent writers" "0" "$DIFF"

# --- 5. subscription cursor --------------------------------------------------
echo "== 5. subscription cursor"
SEQ0=$(q "SELECT nabla.current_seq('public.paid_orders')")
for i in $(seq 1 10); do q "INSERT INTO orders (k, amount, status) VALUES (11, $i, 'paid')" >/dev/null; done
wait_view public.paid_orders
CHECK=$(q "SELECT count(*) || '|' || bool_and(seq = $SEQ0 + rn) || '|' || bool_and(lsn::pg_lsn > prev_lsn::pg_lsn) || '|' || bool_and(op = 'I')
  FROM (SELECT seq, lsn, op, row_number() OVER (ORDER BY seq) rn, lag(lsn, 1, '0/0') OVER (ORDER BY seq) prev_lsn
        FROM $(CH public.paid_orders $SEQ0)) c")
assert_eq "changes() returns the 10 transactions in order with increasing seq and lsn" "10|true|true|true" "$CHECK"
assert_eq "changes() past the cursor is empty" "0" "$(q "SELECT count(*) FROM $(CH public.paid_orders $((SEQ0 + 10)))")"
q "INSERT INTO orders (k, amount, status) SELECT 12, i, 'paid' FROM generate_series(1, 60) i" >/dev/null
wait_view public.paid_orders
ERR=$(q_err "SELECT count(*) FROM $(CH public.paid_orders 0)")
assert_contains "changes() from a cursor older than retention raises the lagged error" \
  'nabla: subscriber lagged behind retention for view "public.paid_orders"' "$ERR"
assert_contains "the lagged error names the oldest retained seq" 'DETAIL:  oldest retained seq is' "$ERR"
assert_eq "the lagged error carries SQLSTATE NB001" "NB001" "$(sqlstate_of "SELECT count(*) FROM $(CH public.paid_orders 0)")"
assert_eq "retention keeps at most 50 deltas" "t" "$(q "SELECT count(*) <= 50 FROM nabla.deltas d JOIN nabla.views v ON v.id = d.view_id WHERE v.name = 'public.paid_orders'")"

# --- 6. rejections -----------------------------------------------------------
echo "== 6. rejections"
q "CREATE TABLE customers (id int PRIMARY KEY, name text)" >/dev/null
ERR=$(q_err "SELECT nabla.create_view('public.bad1', 'SELECT o.id FROM orders o LEFT JOIN customers c ON c.id = o.k')")
assert_contains "LEFT JOIN is rejected" "nabla: unsupported view definition" "$ERR"
ERR=$(q_err "SELECT nabla.create_view('public.bad2', 'SELECT k, avg(amount) FROM orders GROUP BY k')")
assert_contains "avg() is rejected" "nabla: unsupported view definition" "$ERR"
ERR=$(q_err "SELECT nabla.create_view('public.bad3', 'SELECT id, k FROM orders ORDER BY id')")
assert_contains "ORDER BY is rejected" "nabla: unsupported view definition" "$ERR"
assert_eq "rejections carry SQLSTATE NB004" "NB004" "$(sqlstate_of "SELECT nabla.create_view('public.bad3', 'SELECT id, k FROM orders ORDER BY id')")"
ERR=$(q_err "SELECT nabla.create_view('public.bad4', 'SELECT k FROM orders')")
assert_contains "projection without the primary key is rejected" "missing id" "$ERR"
q "CREATE TABLE events (id int PRIMARY KEY, kind text, n int)" >/dev/null
ERR=$(q_err "SELECT nabla.create_view('public.bad5', 'SELECT kind, count(*) FROM events GROUP BY kind')")
assert_contains "aggregate on default replica identity is rejected" "ALTER TABLE public.events REPLICA IDENTITY FULL;" "$ERR"

# --- 7. direct write guard ---------------------------------------------------
echo "== 7. direct write guard"
ERR=$(q_err "INSERT INTO paid_orders VALUES (1, 1, 1)")
assert_contains "direct INSERT into a view is rejected" "cannot modify a nabla view directly" "$ERR"
ERR=$(q_err "DELETE FROM orders_by_k")
assert_contains "direct DELETE from a view is rejected" "cannot modify a nabla view directly" "$ERR"
assert_eq "direct writes carry SQLSTATE NB005" "NB005" "$(sqlstate_of "DELETE FROM orders_by_k")"

# --- 8. refresh --------------------------------------------------------------
echo "== 8. refresh"
EPOCH0=$(q "SELECT epoch FROM nabla.views WHERE name = 'public.orders_by_k'")
CURSOR=$(q "SELECT nabla.current_seq('public.orders_by_k')")
q "SELECT nabla.refresh('public.orders_by_k')" >/dev/null || die "refresh failed"
await_ready public.orders_by_k
EPOCH1=$(q "SELECT epoch FROM nabla.views WHERE name = 'public.orders_by_k'")
CURSOR1=$(q "SELECT current_seq FROM nabla.status('public.orders_by_k')")
assert_eq "refresh bumps the epoch" "$((EPOCH0 + 1))" "$EPOCH1"
q "INSERT INTO orders (k, amount, status) VALUES (13, 3, 'paid')" >/dev/null
wait_view public.orders_by_k
DIFF=$(q "SELECT count(*) FROM (
  (SELECT k, n, total FROM orders_by_k EXCEPT SELECT k, count(*), sum(amount) FROM orders WHERE status = 'paid' GROUP BY k)
  UNION ALL
  (SELECT k, count(*), sum(amount) FROM orders WHERE status = 'paid' GROUP BY k EXCEPT SELECT k, n, total FROM orders_by_k)) d")
assert_eq "aggregate view equals its query after refresh" "0" "$DIFF"
ERR=$(q_err "SELECT count(*) FROM nabla.changes('public.orders_by_k', $CURSOR, $EPOCH0)")
assert_contains "a cursor from before the refresh must resync (epoch changed, not lagged)" 'nabla: view "public.orders_by_k" epoch changed' "$ERR"
assert_eq "the epoch error carries SQLSTATE NB003" "NB003" "$(sqlstate_of "SELECT count(*) FROM nabla.changes('public.orders_by_k', $CURSOR, $EPOCH0)")"
# k=13 is a new group, so exactly one 'I' delta follows the refresh.
assert_eq "a fresh cursor after refresh works" "1" "$(q "SELECT count(*) FROM $(CH public.orders_by_k $CURSOR1)")"

# --- 9. idle worker generates no WAL -----------------------------------------
echo "== 9. idle worker"
sleep 1
QUIET=no
for _ in 1 2 3; do
  L1=$(q "SELECT pg_current_wal_lsn()"); sleep 1; L2=$(q "SELECT pg_current_wal_lsn()")
  if [ "$L1" = "$L2" ]; then QUIET=yes; break; fi
done
assert_eq "worker generates no WAL while idle (one quiet 1 s window out of 3)" "yes" "$QUIET"

# --- 10. identifiers, aliases, comments, literals ----------------------------
echo "== 10. parser: identifiers, aliases, comments, literals"
q 'CREATE TABLE "Orders" ("Id" bigserial PRIMARY KEY, "Amount" numeric, "Status" text)' >/dev/null
q "SELECT nabla.create_view('public.paid_mixed', 'SELECT \"Id\", \"Amount\" FROM \"Orders\" WHERE \"Status\" = ''paid''')" >/dev/null \
  || die "create_view(paid_mixed) failed"
await_ready public.paid_mixed
q "INSERT INTO \"Orders\" (\"Amount\", \"Status\") VALUES (1, 'paid'), (2, 'new'), (3, 'paid')" >/dev/null
q "UPDATE \"Orders\" SET \"Status\" = 'paid' WHERE \"Id\" = 2" >/dev/null
q 'DELETE FROM "Orders" WHERE "Id" = 1' >/dev/null
wait_view public.paid_mixed
DIFF=$(q "SELECT count(*) FROM ((SELECT \"Id\", \"Amount\" FROM paid_mixed EXCEPT SELECT \"Id\", \"Amount\" FROM \"Orders\" WHERE \"Status\" = 'paid')
  UNION ALL (SELECT \"Id\", \"Amount\" FROM \"Orders\" WHERE \"Status\" = 'paid' EXCEPT SELECT \"Id\", \"Amount\" FROM paid_mixed)) d")
assert_eq "quoted mixed-case identifiers" "0|2" "$DIFF|$(q 'SELECT count(*) FROM paid_mixed')"
assert_eq "catalog records the fully qualified base table" "t" \
  "$(q "SELECT (SELECT vr.relid FROM nabla.view_relations vr WHERE vr.view_id = nabla.views.id) = '\"Orders\"'::regclass AND spec->>'base_table' = 'public.\"Orders\"' FROM nabla.views WHERE name = 'public.paid_mixed'")"
assert_eq "unqualified name resolved through search_path is stored qualified" "t" \
  "$(q "SELECT (SELECT vr.relid FROM nabla.view_relations vr WHERE vr.view_id = nabla.views.id) = 'public.orders'::regclass AND spec->>'base_table' = 'public.orders' FROM nabla.views WHERE name = 'public.paid_orders'")"

q "SELECT nabla.create_view('public.paid_aliased', 'SELECT o.id AS order_id, /* keep the amount */ o.amount
   FROM public.orders AS o
   WHERE o.status = ''paid'' -- only paid rows')" >/dev/null || die "create_view(paid_aliased) failed"
await_ready public.paid_aliased
q "INSERT INTO orders (k, amount, status) VALUES (21, 7, 'paid'), (21, 8, 'new')" >/dev/null
wait_view public.paid_aliased
DIFF=$(q "SELECT count(*) FROM ((SELECT order_id, amount FROM paid_aliased EXCEPT SELECT id, amount FROM orders WHERE status = 'paid')
  UNION ALL (SELECT id, amount FROM orders WHERE status = 'paid' EXCEPT SELECT order_id, amount FROM paid_aliased)) d")
assert_eq "aliases, schema qualification and comments" "0" "$DIFF"

q "SELECT nabla.create_view('public.odd_status', 'SELECT id, k FROM orders WHERE status = ''order by limit''')" >/dev/null \
  || die "create_view(odd_status) failed"
await_ready public.odd_status
q "INSERT INTO orders (k, amount, status) VALUES (22, 1, 'order by limit'), (22, 2, 'paid')" >/dev/null
wait_view public.odd_status
assert_eq "keywords inside a string literal" "1" "$(q "SELECT count(*) FROM odd_status WHERE k = 22")"

# --- 11. expressions ---------------------------------------------------------
echo "== 11. parser: immutable expressions"
q "SELECT nabla.create_view('public.orders_expr', 'SELECT id, amount * 2 AS doubled, upper(status) AS s FROM orders WHERE status <> ''void''')" >/dev/null \
  || die "create_view(orders_expr) failed"
await_ready public.orders_expr
q "INSERT INTO orders (k, amount, status) VALUES (23, 5, 'paid'), (23, 6, 'void')" >/dev/null
q "UPDATE orders SET amount = amount + 1, status = 'paid' WHERE k = 23" >/dev/null
q "DELETE FROM orders WHERE k = 22" >/dev/null
wait_view public.orders_expr
DIFF=$(q "SELECT count(*) FROM ((SELECT id, doubled, s FROM orders_expr EXCEPT SELECT id, amount * 2, upper(status) FROM orders WHERE status <> 'void')
  UNION ALL (SELECT id, amount * 2, upper(status) FROM orders WHERE status <> 'void' EXCEPT SELECT id, doubled, s FROM orders_expr)) d")
assert_eq "projection with immutable expressions equals its query" "0" "$DIFF"

q "SELECT nabla.create_view('public.paid_parity', 'SELECT k % 2 AS parity, sum(amount * 2) AS total FROM orders WHERE status = ''paid'' GROUP BY k % 2')" >/dev/null \
  || die "create_view(paid_parity) failed"
await_ready public.paid_parity
assert_eq "hidden group count column is present" "t" "$(q "SELECT bool_and(_nabla_n > 0) AND count(*) = 2 FROM paid_parity")"
q "INSERT INTO orders (k, amount, status) VALUES (31, 10, 'paid'), (32, 20, 'paid')" >/dev/null
q "UPDATE orders SET status = 'new' WHERE k = 31" >/dev/null
wait_view public.paid_parity
DIFF=$(q "SELECT count(*) FROM ((SELECT parity, total, _nabla_n FROM paid_parity EXCEPT SELECT k % 2, sum(amount * 2), count(*) FROM orders WHERE status = 'paid' GROUP BY k % 2)
  UNION ALL (SELECT k % 2, sum(amount * 2), count(*) FROM orders WHERE status = 'paid' GROUP BY k % 2 EXCEPT SELECT parity, total, _nabla_n FROM paid_parity)) d")
assert_eq "aggregate with expression keys and hidden count equals its query" "0" "$DIFF"
q "DELETE FROM orders WHERE status = 'paid' AND k % 2 = 1" >/dev/null
wait_view public.paid_parity
assert_eq "empty group disappears without count(*) in the definition" "0|1" \
  "$(q "SELECT count(*) FILTER (WHERE parity = 1) || '|' || count(*) FROM paid_parity")"

# --- 12. rejections by the query-tree walker ---------------------------------
echo "== 12. parser: rejections"
q "CREATE TABLE orders2 (id int PRIMARY KEY, amount int, created_at timestamptz)" >/dev/null
q "CREATE TABLE nopk (a int, b int)" >/dev/null
q "CREATE TABLE parted (id int, k int) PARTITION BY RANGE (id)" >/dev/null
q "CREATE VIEW v_orders AS SELECT * FROM orders" >/dev/null
reject() { # label definition expected-substring
  local err
  err=$(q_err "SELECT nabla.create_view('public.rejected', \$def\$$2\$def\$)")
  assert_contains "$1" "$3" "$err"
}
reject "now() in WHERE" "SELECT id FROM orders2 WHERE created_at < now()" "the WHERE clause uses \"now\", which is not IMMUTABLE"
reject "random() in the select list" "SELECT id, random() AS r FROM orders" "column \"r\" uses \"random\", which is not IMMUTABLE"
reject "STABLE to_char() rejected" "SELECT id, to_char(created_at, 'YYYY') AS y FROM orders2" "column \"y\" uses \"to_char\", which is not IMMUTABLE"
q "SELECT nabla.create_view('public.lower_ok', 'SELECT id, lower(status) AS s FROM orders')" >/dev/null \
  || die "create_view(lower_ok) failed"
await_ready public.lower_ok
pass "lower() is accepted"
reject "subquery in WHERE" "SELECT id FROM orders WHERE k IN (SELECT a FROM nopk)" "subqueries are not supported"
reject "EXISTS" "SELECT id FROM orders WHERE EXISTS (SELECT 1 FROM nopk)" "subqueries are not supported"
reject "CTE" "WITH c AS (SELECT 1) SELECT id FROM orders" "common table expressions (WITH) are not supported"
reject "UNION" "SELECT id FROM orders UNION SELECT id FROM orders2" "UNION, INTERSECT and EXCEPT are not supported"
reject "window function" "SELECT id, row_number() OVER () AS rn FROM orders" "window functions are not supported"
reject "DISTINCT" "SELECT DISTINCT id FROM orders" "DISTINCT is not supported"
reject "ORDER BY" "SELECT id FROM orders ORDER BY id" "ORDER BY is not supported"
reject "LIMIT" "SELECT id FROM orders LIMIT 1" "LIMIT and OFFSET are not supported"
reject "HAVING" "SELECT k, count(*) FROM orders GROUP BY k HAVING count(*) > 1" "HAVING is not supported"
reject "GROUPING SETS" "SELECT k, count(*) FROM orders GROUP BY GROUPING SETS ((k), ())" "GROUPING SETS, CUBE and ROLLUP are not supported"
reject "FOR UPDATE" "SELECT id FROM orders FOR UPDATE" "FOR UPDATE and FOR SHARE are not supported"
reject "outer join" "SELECT o.id FROM orders o LEFT JOIN orders2 x ON x.id = o.k" "LEFT JOIN is not supported; only inner joins are"
reject "comma join to a table without a primary key" "SELECT o.id FROM orders o, nopk n" "public.nopk has no primary key; every table in a join view needs one"
reject "LATERAL" "SELECT o.id FROM orders o, LATERAL (SELECT o.k) l" "LATERAL is not supported"
reject "set-returning function" "SELECT g FROM generate_series(1, 3) g" "set-returning functions are not supported"
reject "SRF in select list" "SELECT id, generate_series(1, 2) AS g FROM orders" "set-returning functions are not supported"
reject "SELECT * without a primary key" "SELECT * FROM nopk" "public.nopk has no primary key"
reject "partitioned table" "SELECT id FROM parted" "public.parted is a partitioned table"
reject "plain view as source" "SELECT id FROM v_orders" "public.v_orders is a view"
reject "nabla view as source" "SELECT id FROM paid_orders" "public.paid_orders is a nabla view"
reject "count(DISTINCT)" "SELECT k, count(DISTINCT k) FROM orders GROUP BY k" "DISTINCT inside aggregates is not supported"
reject "FILTER" "SELECT k, count(*), sum(amount) FILTER (WHERE amount > 1) AS f FROM orders GROUP BY k" "FILTER on aggregates is not supported"
reject "avg()" "SELECT k, avg(amount) FROM orders GROUP BY k" "aggregate \"avg\" is not supported"
reject "max()" "SELECT k, max(amount) FROM orders GROUP BY k" "aggregate \"max\" is not supported"
reject "expression over an aggregate" "SELECT k, sum(amount) + 1 AS t FROM orders GROUP BY k" "is an expression over an aggregate"
reject "GROUP BY key not selected" "SELECT sum(amount) AS t FROM orders GROUP BY k" "every GROUP BY expression must also appear in the select list"
reject "syntax error surfaces PostgreSQL's text" "SELEC id FROM orders" "syntax error at or near \"SELEC\""
reject "missing column surfaces PostgreSQL's text" "SELECT nosuch FROM orders" "column \"nosuch\" does not exist"
reject "two statements" "SELECT id FROM orders; SELECT id FROM orders" "exactly one SELECT statement"
assert_eq "rejected definitions leave no view behind" "0" "$(q "SELECT count(*) FROM nabla.views WHERE name = 'public.rejected'")"

# --- 13. failure isolation ---------------------------------------------------
echo "== 13. failure isolation"
q "CREATE TABLE t (id bigserial PRIMARY KEY, k int, amount numeric)" >/dev/null
q "ALTER TABLE t REPLICA IDENTITY FULL" >/dev/null
q "SELECT nabla.create_view('public.bad', 'SELECT id, 100 / k AS ratio FROM t')" >/dev/null || die "create_view(bad) failed"
await_ready public.bad
q "SELECT nabla.create_view('public.good', 'SELECT k, count(*) AS n FROM t GROUP BY k')" >/dev/null || die "create_view(good) failed"
await_ready public.good
for k in 1 2 4; do q "INSERT INTO t (k, amount) VALUES ($k, 1)" >/dev/null; done
wait_view public.bad
wait_view public.good
bad_diff() { q "SELECT count(*) FROM ((SELECT id, ratio FROM bad EXCEPT SELECT id, 100 / k FROM t) UNION ALL (SELECT id, 100 / k FROM t EXCEPT SELECT id, ratio FROM bad)) d"; }
good_diff() { q "SELECT count(*) FROM ((SELECT k, n FROM good EXCEPT SELECT k, count(*) FROM t GROUP BY k) UNION ALL (SELECT k, count(*) FROM t GROUP BY k EXCEPT SELECT k, n FROM good)) d"; }
assert_eq "bad and good equal their queries before the failure" "0|0" "$(bad_diff)|$(good_diff)"
F0=$(q "SELECT nabla.frontier('public.bad')")
# Slow the worker down so the intermediate failure count is observable.
q "ALTER SYSTEM SET nabla.poll_interval_ms = 1000" >/dev/null
q "SELECT pg_reload_conf()" >/dev/null
sleep 0.5
q "INSERT INTO t (k, amount) VALUES (0, 1)" >/dev/null
L0=$(q "SELECT pg_current_wal_lsn()")
q "INSERT INTO t (k, amount) VALUES (5, 1)" >/dev/null
SEEN_LIVE_FAILURE=no
STATUS=live
for _ in $(seq 1 150); do
  ROW=$(q "SELECT status || '|' || apply_failures FROM nabla.views WHERE name = 'public.bad'")
  STATUS=${ROW%%|*}
  FAILS=${ROW##*|}
  if [ "$STATUS" = live ] && [ "$FAILS" -ge 1 ]; then SEEN_LIVE_FAILURE=yes; fi
  [ "$STATUS" = stale ] && break
  sleep 0.1
done
q "ALTER SYSTEM RESET nabla.poll_interval_ms" >/dev/null
q "SELECT pg_reload_conf()" >/dev/null
assert_eq "bad goes stale after repeated failures" "stale" "$STATUS"
assert_eq "an intermediate failure count was observed while still live" "yes" "$SEEN_LIVE_FAILURE"
assert_eq "failure bookkeeping" "3|true|true|true" \
  "$(q "SELECT apply_failures || '|' || (last_error LIKE '%division by zero%') || '|' || (stale_reason LIKE 'apply failed 3 times:%division by zero%') || '|' || (last_error_at IS NOT NULL) FROM nabla.views WHERE name = 'public.bad'")"
assert_eq "exactly one stale WARNING for bad in the server log" "1" "$(grep -c 'nabla worker: view public.bad marked stale' "$LOG")"
wait_view public.good
assert_eq "good was never blocked and includes k = 0 and k = 5" "0|1|1" "$(good_diff)|$(q "SELECT n FROM good WHERE k = 0")|$(q "SELECT n FROM good WHERE k = 5")"
assert_eq "good's frontier passed the failing transaction" "t" "$(q "SELECT nabla.frontier('public.good') >= '$L0'::pg_lsn")"
SLOT_OK=no
for _ in $(seq 1 50); do
  if [ "$(q "SELECT confirmed_flush_lsn >= '$L0'::pg_lsn FROM pg_replication_slots WHERE slot_name = 'nabla'")" = t ]; then SLOT_OK=yes; break; fi
  sleep 0.1
done
assert_eq "the slot advanced past the failing transaction" "yes" "$SLOT_OK"
ERR=$(q_err "SELECT count(*) FROM $(CH public.bad 0)")
assert_contains "changes() on a stale view raises the stale error" 'nabla: view "public.bad" is stale' "$ERR"
assert_contains "the stale error names the reason in DETAIL" 'DETAIL:  apply failed 3 times: division by zero' "$ERR"
assert_contains "changes() on a stale view carries the refresh hint" "run nabla.refresh('public.bad') after fixing the cause" "$ERR"
assert_eq "the stale error carries SQLSTATE NB002" "NB002" "$(sqlstate_of "SELECT count(*) FROM $(CH public.bad 0)")"
ERR=$(q_err "SELECT nabla.wait_for('public.bad', pg_current_wal_lsn(), 100)")
assert_contains "wait_for() on a stale view raises the stale error" 'nabla: view "public.bad" is stale' "$ERR"
assert_eq "frontier('bad') still returns the last absorbed LSN" "t" \
  "$(q "SELECT nabla.frontier('public.bad') >= '$F0'::pg_lsn AND nabla.frontier('public.bad') < '$L0'::pg_lsn")"
q "SELECT nabla.refresh('public.bad')" >/dev/null || die "refresh(bad) failed to start"
ERR=$(q_err "SELECT nabla.await_ready('public.bad', 60000)")
assert_contains "await_ready() reports the rebuild error while the bad row exists" "DETAIL:  division by zero" "$ERR"
assert_eq "a failed rebuild carries SQLSTATE NB006" "NB006" "$(sqlstate_of "SELECT nabla.await_ready('public.bad', 60000)")"
assert_eq "bad is failed after the rebuild error (old content kept)" "failed|t" "$(q "SELECT status FROM nabla.views WHERE name = 'public.bad'")|$(q "SELECT to_regclass('public.bad') IS NOT NULL")"
q "DELETE FROM t WHERE k = 0" >/dev/null
q "SELECT nabla.refresh('public.bad')" >/dev/null || die "refresh(bad) failed after removing the bad row"
await_ready public.bad
assert_eq "refresh() recovers the view" "live|0|true|true" \
  "$(q "SELECT status || '|' || apply_failures || '|' || (last_error IS NULL) || '|' || (stale_reason IS NULL) FROM nabla.views WHERE name = 'public.bad'")"
q "INSERT INTO t (k, amount) VALUES (7, 1)" >/dev/null
wait_view public.bad
assert_eq "bad is maintained again after recovery" "0|1" "$(bad_diff)|$(q "SELECT count(*) FROM bad WHERE ratio = 14")"

# --- 14. NULL semantics of sum() and count(expr) -----------------------------
echo "== 14. sum()/count(expr) NULL semantics"
q "CREATE TABLE s (id bigserial PRIMARY KEY, k int, v numeric, w int)" >/dev/null
q "ALTER TABLE s REPLICA IDENTITY FULL" >/dev/null
q "SELECT nabla.create_view('public.agg_s', 'SELECT k, count(*) AS n, sum(v) AS sv, count(w) AS cw, sum(w) AS sw FROM s GROUP BY k')" >/dev/null \
  || die "create_view(agg_s) failed"
await_ready public.agg_s
s_diff() { q "SELECT count(*) FROM ((SELECT k, n, sv, cw, sw FROM agg_s EXCEPT SELECT k, count(*), sum(v), count(w), sum(w) FROM s GROUP BY k)
  UNION ALL (SELECT k, count(*), sum(v), count(w), sum(w) FROM s GROUP BY k EXCEPT SELECT k, n, sv, cw, sw FROM agg_s)) d"; }
s_row() { q "SELECT n || '|' || coalesce(sv::text, 'NULL') || '|' || cw || '|' || coalesce(sw::text, 'NULL') FROM agg_s WHERE k = $1"; }
assert_eq "hidden sum counters exist after the user's columns" "k,n,sv,cw,sw,_nabla_nn_1,_nabla_nn_3" \
  "$(q "SELECT string_agg(column_name, ',' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_name = 'agg_s'")"
q "INSERT INTO s (k, v, w) VALUES (1, NULL, NULL), (1, NULL, NULL)" >/dev/null
wait_view public.agg_s
assert_eq "group of only NULLs: n=2, sums NULL, count(w)=0" "2|NULL|0|NULL|0" "$(s_row 1)|$(s_diff)"
q "INSERT INTO s (k, v, w) VALUES (1, 5, 7)" >/dev/null
wait_view public.agg_s
assert_eq "first non-NULL contribution" "3|5|1|7|0" "$(s_row 1)|$(s_diff)"
q "DELETE FROM s WHERE k = 1 AND v = 5" >/dev/null
wait_view public.agg_s
assert_eq "sums return to NULL when the last non-NULL row is deleted" "2|NULL|0|NULL|0" "$(s_row 1)|$(s_diff)"
q "UPDATE s SET v = 3 WHERE id = (SELECT min(id) FROM s WHERE k = 1)" >/dev/null
wait_view public.agg_s
assert_eq "update NULL -> 3 is followed" "2|3|0|NULL|0" "$(s_row 1)|$(s_diff)"
q "UPDATE s SET v = NULL WHERE k = 1" >/dev/null
wait_view public.agg_s
assert_eq "update 3 -> NULL is followed" "2|NULL|0|NULL|0" "$(s_row 1)|$(s_diff)"
q "INSERT INTO s (k, v, w) VALUES (2, NULL, NULL), (2, 1, 1), (2, 2, NULL), (2, NULL, 4)" >/dev/null
wait_view public.agg_s
assert_eq "mixed group" "4|3|2|5|0" "$(s_row 2)|$(s_diff)"
q "DELETE FROM s WHERE k = 2 AND (v IS NOT NULL OR w IS NOT NULL)" >/dev/null
wait_view public.agg_s
assert_eq "deleting the non-NULL rows leaves NULL sums with n > 0" "1|NULL|0|NULL|0" "$(s_row 2)|$(s_diff)"
q "DELETE FROM s WHERE k = 2" >/dev/null
wait_view public.agg_s
assert_eq "empty group disappears" "0|0" "$(q "SELECT count(*) FROM agg_s WHERE k = 2")|$(s_diff)"
q "INSERT INTO s (k, v, w) VALUES (3, 1.5, NULL), (3, NULL, 2), (1, 2.5, 9)" >/dev/null
wait_view public.agg_s
q "CREATE TABLE agg_s_snapshot AS SELECT * FROM agg_s" >/dev/null
q "SELECT nabla.refresh('public.agg_s')" >/dev/null || die "refresh(agg_s) failed"
await_ready public.agg_s
assert_eq "refresh reproduces the maintained table exactly, hidden counters included" "0" \
  "$(q "SELECT count(*) FROM ((SELECT * FROM agg_s_snapshot EXCEPT SELECT * FROM agg_s) UNION ALL (SELECT * FROM agg_s EXCEPT SELECT * FROM agg_s_snapshot)) d")"
reject "count(DISTINCT w) still rejected" "SELECT k, count(DISTINCT w) AS c FROM s GROUP BY k" "DISTINCT inside aggregates is not supported"
reject "sum(w) FILTER still rejected" "SELECT k, sum(w) FILTER (WHERE w > 1) AS f FROM s GROUP BY k" "FILTER on aggregates is not supported"
reject "count(expr) with a mutable expression rejected" "SELECT k, count(w + random()::int) AS c FROM s GROUP BY k" "the argument of count() in column \"c\" uses \"random\", which is not IMMUTABLE"
reject "count(w) without GROUP BY rejected" "SELECT count(w) AS c FROM s" "aggregates require a GROUP BY clause"
reject "reserved _nabla_ names rejected" "SELECT k AS _nabla_key, count(*) FROM s GROUP BY k" "column names starting with _nabla_ are reserved"

# --- 15. projection join view --------------------------------------------------
echo "== 15. projection join view"
q "CREATE SCHEMA shop" >/dev/null
q "CREATE TABLE shop.customers (id int PRIMARY KEY, name text, region text)" >/dev/null
q "CREATE TABLE shop.products (id int PRIMARY KEY, name text, price numeric)" >/dev/null
q "CREATE TABLE shop.orders (id bigserial PRIMARY KEY, customer_id int, product_id int, qty int, status text)" >/dev/null
q "INSERT INTO shop.customers VALUES (1, 'Alice', 'north'), (2, 'Bob', 'south'), (3, 'Carol', 'north')" >/dev/null
q "INSERT INTO shop.products VALUES (1, 'pen', 1.5), (2, 'book', 12), (3, 'lamp', 30)" >/dev/null
q "INSERT INTO shop.orders (customer_id, product_id, qty, status) VALUES (1, 1, 2, 'paid'), (2, 2, 1, 'paid'), (3, 3, 1, 'new')" >/dev/null
LINES_DEF="SELECT o.id AS order_id, c.name AS customer, p.name AS product, o.qty * p.price AS total FROM shop.orders o JOIN shop.customers c ON c.id = o.customer_id JOIN shop.products p ON p.id = o.product_id WHERE o.status = 'paid'"
q "SELECT nabla.create_view('public.order_lines', \$d\$$LINES_DEF\$d\$)" >/dev/null || die "create_view(order_lines) failed"
await_ready public.order_lines
lines_diff() { q "SELECT count(*) FROM ((SELECT order_id, customer, product, total FROM order_lines EXCEPT $LINES_DEF)
  UNION ALL ($LINES_DEF EXCEPT SELECT order_id, customer, product, total FROM order_lines)) d"; }
shadow_of() { q "SELECT table_name FROM nabla.shadows WHERE relid = '$1'::regclass"; }
shadow_cols_of() { q "SELECT array_to_string(columns, ', ') FROM nabla.shadows WHERE relid = '$1'::regclass"; }
shadow_diff() { # shadows hold only the columns their views need: compare those
  local cols; cols=$(shadow_cols_of "$1")
  q "SELECT count(*) FROM ((SELECT $cols FROM $(shadow_of "$1") EXCEPT SELECT $cols FROM $1) UNION ALL (SELECT $cols FROM $1 EXCEPT SELECT $cols FROM $(shadow_of "$1"))) d"
}
assert_eq "join view columns: visible then hidden keys" "order_id,customer,product,total,_nabla_pk1_id,_nabla_pk2_id,_nabla_pk3_id" \
  "$(q "SELECT string_agg(column_name, ',' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_name = 'order_lines'")"
q "INSERT INTO shop.orders (customer_id, product_id, qty, status) VALUES (1, 2, 3, 'paid'), (3, 1, 4, 'paid'), (2, 3, 1, 'new')" >/dev/null
wait_view public.order_lines
assert_eq "join view after inserting orders" "0|4" "$(lines_diff)|$(q "SELECT count(*) FROM order_lines")"
q "UPDATE shop.customers SET name = 'Robert' WHERE id = 2" >/dev/null
wait_view public.order_lines
assert_eq "customer rename propagates to every order" "0|1" "$(lines_diff)|$(q "SELECT count(*) FROM order_lines WHERE customer = 'Robert'")"
q "UPDATE shop.products SET price = 15 WHERE id = 2" >/dev/null
wait_view public.order_lines
assert_eq "price change updates totals" "0|45" "$(lines_diff)|$(q "SELECT total FROM order_lines WHERE order_id = 4")"
q "UPDATE shop.orders SET customer_id = 2 WHERE id = 4" >/dev/null
wait_view public.order_lines
assert_eq "order moves to another customer" "0|Robert" "$(lines_diff)|$(q "SELECT customer FROM order_lines WHERE order_id = 4")"
q "DELETE FROM shop.customers WHERE id = 3" >/dev/null
wait_view public.order_lines
assert_eq "deleting a customer removes its orders from the view" "0|0" "$(lines_diff)|$(q "SELECT count(*) FROM order_lines WHERE customer = 'Carol'")"
q "BEGIN; INSERT INTO shop.customers VALUES (4, 'Dave', 'east'); INSERT INTO shop.orders (customer_id, product_id, qty, status) VALUES (4, 1, 10, 'paid'), (4, 3, 1, 'paid'); COMMIT" >/dev/null
wait_view public.order_lines
assert_eq "one transaction inserting a customer and its orders" "0|2" "$(lines_diff)|$(q "SELECT count(*) FROM order_lines WHERE customer = 'Dave'")"
assert_eq "one shadow per base table, refcount 1" "3|3" \
  "$(q "SELECT count(*) FROM nabla.shadows WHERE relid IN ('shop.customers'::regclass, 'shop.products'::regclass, 'shop.orders'::regclass)")|$(q "SELECT count(*) FROM nabla.shadows WHERE refcount = 1")"
assert_eq "shadows equal their base tables" "0|0|0" "$(shadow_diff shop.customers)|$(shadow_diff shop.products)|$(shadow_diff shop.orders)"

# --- 16. aggregate join view ---------------------------------------------------
echo "== 16. aggregate join view"
REV_DEF="SELECT c.region, count(*) AS n, sum(o.qty * p.price) AS revenue FROM shop.orders o JOIN shop.customers c ON c.id = o.customer_id JOIN shop.products p ON p.id = o.product_id WHERE o.status = 'paid' GROUP BY c.region"
q "SELECT nabla.create_view('public.revenue_by_region', \$d\$$REV_DEF\$d\$)" >/dev/null || die "create_view(revenue_by_region) failed"
await_ready public.revenue_by_region
rev_diff() { q "SELECT count(*) FROM ((SELECT region, n, revenue FROM revenue_by_region EXCEPT $REV_DEF)
  UNION ALL ($REV_DEF EXCEPT SELECT region, n, revenue FROM revenue_by_region)) d"; }
assert_eq "aggregate join view equals its query at creation" "0" "$(rev_diff)"
q "INSERT INTO shop.orders (customer_id, product_id, qty, status) VALUES (2, 1, 6, 'paid'), (1, 3, 2, 'paid')" >/dev/null
wait_view public.revenue_by_region
assert_eq "aggregate join after inserting orders" "0" "$(rev_diff)"
q "UPDATE shop.customers SET region = 'west' WHERE id = 2" >/dev/null
wait_view public.revenue_by_region
assert_eq "region change moves revenue between groups" "0|0" "$(rev_diff)|$(q "SELECT count(*) FROM revenue_by_region WHERE region = 'south'")"
q "UPDATE shop.products SET price = 2 WHERE id = 1" >/dev/null
wait_view public.revenue_by_region
assert_eq "price change updates revenue" "0" "$(rev_diff)"
q "BEGIN; DELETE FROM shop.orders WHERE customer_id = 2; DELETE FROM shop.customers WHERE id = 4; COMMIT" >/dev/null
wait_view public.revenue_by_region
assert_eq "deletes across two tables in one transaction; empty regions disappear" "0|0" "$(rev_diff)|$(q "SELECT count(*) FROM revenue_by_region WHERE region IN ('west', 'east')")"
wait_view public.order_lines
assert_eq "projection join view still equals its query" "0" "$(lines_diff)"
assert_eq "shadows shared by two views have refcount 2" "3" "$(q "SELECT count(*) FROM nabla.shadows WHERE refcount = 2")"
q "SELECT nabla.drop_view('public.order_lines')" >/dev/null || die "drop_view(order_lines) failed"
assert_eq "dropping one view keeps the shadows with refcount 1" "3|3" \
  "$(q "SELECT count(*) FROM nabla.shadows WHERE refcount = 1")|$(q "SELECT count(*) FROM pg_tables WHERE schemaname = 'nabla_shadow'")"
q "SELECT nabla.drop_view('public.revenue_by_region')" >/dev/null || die "drop_view(revenue_by_region) failed"
assert_eq "dropping the last view drops the shadows" "0|0|0" \
  "$(q "SELECT count(*) FROM nabla.shadows")|$(q "SELECT count(*) FROM pg_tables WHERE schemaname = 'nabla_shadow'")|$(q "SELECT count(*) FROM pg_publication_tables WHERE pubname = 'nabla' AND schemaname = 'shop'")"

# --- 17. the shadow proof ------------------------------------------------------
echo "== 17. shadow proof"
q "SELECT nabla.create_view('public.order_lines', \$d\$$LINES_DEF\$d\$)" >/dev/null || die "create_view(order_lines) failed"
await_ready public.order_lines
q "ALTER SYSTEM SET nabla.poll_interval_ms = 5000" >/dev/null
q "SELECT pg_reload_conf()" >/dev/null
sleep 0.5
wait_view public.order_lines
SEQ0=$(q "SELECT nabla.current_seq('public.order_lines')")
q "INSERT INTO shop.orders (customer_id, product_id, qty, status) VALUES (1, 2, 7, 'paid')" >/dev/null
NEW_ORDER=$(q "SELECT max(id) FROM shop.orders")
q "UPDATE shop.customers SET name = 'Alicia' WHERE id = 1" >/dev/null
q "ALTER SYSTEM RESET nabla.poll_interval_ms" >/dev/null
q "SELECT pg_reload_conf()" >/dev/null
wait_view public.order_lines
assert_eq "final state shows the new customer name" "Alicia|0" "$(q "SELECT customer FROM order_lines WHERE order_id = $NEW_ORDER")|$(lines_diff)"
assert_eq "T1's delta carries the name as of T1 (read from the shadow), then T2's D/I pair" "I:Alice,D:Alice,I:Alicia" \
  "$(q "SELECT string_agg(op || ':' || (row->>'customer'), ',' ORDER BY seq) FROM $(CH public.order_lines $SEQ0) WHERE (row->>'order_id')::bigint = $NEW_ORDER")"
assert_eq "T1 and T2 deltas carry distinct increasing LSNs" "2|true" \
  "$(q "SELECT count(DISTINCT lsn) || '|' || (min(lsn::pg_lsn) < max(lsn::pg_lsn)) FROM $(CH public.order_lines $SEQ0)")"
assert_eq "T2 rewrote every order of the customer" "3" \
  "$(q "SELECT count(*) FROM $(CH public.order_lines $SEQ0) WHERE op = 'I' AND row->>'customer' = 'Alicia'")"
psql -X -q -p "$PORT" -d "$DB" -c "BEGIN; INSERT INTO shop.orders (customer_id, product_id, qty, status) VALUES (1, 1, 1, 'paid'); SELECT pg_sleep(3); COMMIT" >/dev/null 2>&1 &
WRITER_PID=$!
sleep 0.5
START_NS=$(date +%s%N)
if q "SET lock_timeout = '1s'; UPDATE shop.customers SET name = 'Bob' WHERE id = 2" >/dev/null 2>/tmp/nabla-j.err; then
  ELAPSED_MS=$(( ($(date +%s%N) - START_NS) / 1000000 ))
  if [ "$ELAPSED_MS" -lt 1000 ]; then pass "writer on a joined table committed in ${ELAPSED_MS} ms while another transaction was open"
  else fail "join thesis" "commit took ${ELAPSED_MS} ms"; fi
else
  fail "join thesis" "update failed: $(cat /tmp/nabla-j.err)"
fi
wait "$WRITER_PID" 2>/dev/null; WRITER_PID=""
wait_view public.order_lines
assert_eq "join view consistent after concurrent writers" "0" "$(lines_diff)"

# --- 18. join rejections -------------------------------------------------------
echo "== 18. join rejections"
reject "LEFT JOIN" "SELECT o.id FROM shop.orders o LEFT JOIN shop.customers c ON c.id = o.customer_id" "LEFT JOIN is not supported; only inner joins are"
reject "FULL JOIN" "SELECT o.id FROM shop.orders o FULL JOIN shop.customers c ON c.id = o.customer_id" "FULL JOIN is not supported; only inner joins are"
reject "self-join" "SELECT a.id FROM shop.orders a JOIN shop.orders b ON a.id = b.id" "table shop.orders is referenced twice; self-joins are not supported"
reject "join with a subquery in FROM" "SELECT o.id FROM shop.orders o JOIN (SELECT id FROM shop.customers) c ON c.id = o.customer_id" "subqueries in FROM are not supported"
reject "join to a partitioned table" "SELECT o.id FROM shop.orders o JOIN parted p ON p.id = o.customer_id" "public.parted is a partitioned table"
reject "join to a table without a primary key" "SELECT o.id FROM shop.orders o JOIN nopk n ON n.a = o.customer_id" "public.nopk has no primary key; every table in a join view needs one"
reject "SELECT * over a join with duplicate output names" "SELECT * FROM shop.customers c JOIN shop.products p ON p.id = c.id" "output column names must be unique"
reject "join over a nabla view" "SELECT o.id FROM shop.orders o JOIN order_lines l ON l.order_id = o.id" "is a nabla view"

# --- 19. reference client ------------------------------------------------------
echo "== 19. reference client (clients/rust/nabla-client)"
CLIENT_DIR=/work/clients/rust/nabla-client
(cd "$CLIENT_DIR" && cargo build --release --example follow >/tmp/nabla-client-build.log 2>&1) \
  || die "client build failed: $(tail -n 30 /tmp/nabla-client-build.log)"
FOLLOW="$CLIENT_DIR/target/release/examples/follow"
CONN="host=/tmp port=$PORT dbname=$DB user=$(whoami)"
assert_eq "nabla.status() reports the view in one row" "live|true|true|true" \
  "$(q "SELECT status || '|' || (epoch >= 1) || '|' || (current_seq >= 0) || '|' || (frontier_lsn > '0/0') FROM nabla.status('public.order_lines')")"
assert_contains "nabla.status() on a missing view is an error" 'view "public.nothing" does not exist' "$(q_err "SELECT * FROM nabla.status('public.nothing')")"
wait_view public.order_lines
ROWS0=$(q "SELECT count(*) FROM order_lines")
OUT=/tmp/follow.out
: > "$OUT"
"$FOLLOW" --rows "$CONN" public.order_lines > "$OUT" 2>/tmp/follow.err &
CLIENT_PID=$!
wait_line() { # pattern timeout_seconds
  local i
  for i in $(seq 1 $(( $2 * 10 ))); do grep -qE "$1" "$OUT" && return 0; sleep 0.1; done
  return 1
}
wait_line '^snapshot:' 20 || die "client produced no snapshot: $(cat /tmp/follow.err)"
assert_eq "snapshot row count" "rows=$ROWS0" "$(grep -m1 -oE 'rows=[0-9]+' "$OUT")"
q "INSERT INTO shop.orders (customer_id, product_id, qty, status) VALUES (1, 1, 50, 'paid')" >/dev/null
q "INSERT INTO shop.orders (customer_id, product_id, qty, status) VALUES (1, 2, 51, 'paid'), (1, 3, 52, 'paid'), (2, 1, 53, 'paid')" >/dev/null
q "BEGIN; UPDATE shop.customers SET name = 'Roberta' WHERE id = 2; INSERT INTO shop.orders (customer_id, product_id, qty, status) VALUES (2, 2, 54, 'paid'); COMMIT" >/dev/null
wait_view public.order_lines
wait_line 'Roberta' 20 || die "client did not receive the two-table transaction: $(tail -n 20 "$OUT")"
sleep 0.5
assert_eq "three transactions received" "3" "$(grep -cE '^tx ' "$OUT")"
assert_eq "the multi-row transaction is one line with 3 inserts" "deltas=3|3"   "$(grep -E '^tx ' "$OUT" | sed -n 2p | grep -oE 'deltas=[0-9]+')|$(awk '/^tx / { n++ } n == 2 && /^  [0-9]+ \+/ { c++ } END { print c + 0 }' "$OUT")"
assert_eq "the two-table transaction rewrites the customer's rows plus the new order" \
  "deltas=$(q "SELECT 2 * (SELECT count(*) FROM order_lines WHERE customer = 'Roberta') - 1")" \
  "$(grep -E '^tx ' "$OUT" | tail -n 1 | grep -oE 'deltas=[0-9]+')"
q "SELECT nabla.refresh('public.order_lines')" >/dev/null || die "refresh(order_lines) failed"
await_ready public.order_lines
# No write follows the refresh: the client must notice it through its fallback poll.
wait_line 'resync: epoch changed' 20 || die "no epoch resync after refresh: $(tail -n 20 "$OUT")"
L_RESYNC=$(grep -n -m1 'resync: epoch changed' "$OUT" | cut -d: -f1)
for _ in $(seq 1 100); do awk -v s="$L_RESYNC" 'NR > s && /^snapshot:/ { found = 1 } END { exit !found }' "$OUT" && break; sleep 0.1; done
L_SNAP2=$(awk -v s="$L_RESYNC" 'NR > s && /^snapshot:/ { print NR; exit }' "$OUT")
q "INSERT INTO shop.orders (customer_id, product_id, qty, status) VALUES (1, 1, 60, 'paid')" >/dev/null
wait_view public.order_lines
wait_line '^  [0-9]+ \+.*"total":120' 20 || die "transaction after the refresh not received: $(tail -n 20 "$OUT")"
L_TX2=$(awk -v s="${L_SNAP2:-0}" 'NR > s && /^tx / { print NR; exit }' "$OUT")
assert_eq "epoch resync is followed by a fresh snapshot and then the later transaction" "yes" \
  "$( [ -n "$L_SNAP2" ] && [ -n "$L_TX2" ] && echo yes || echo no )"
assert_eq "epoch resync names the epochs" "1" "$(grep -cE 'resync: epoch changed \([0-9]+ -> [0-9]+\)' "$OUT")"
# Lagged: freeze the client, push more deltas than nabla.retain_deltas (50), resume.
kill -STOP "$CLIENT_PID"
q "INSERT INTO shop.orders (customer_id, product_id, qty, status) SELECT 1, 1, 100 + i, 'paid' FROM generate_series(1, 61) i" >/dev/null
wait_view public.order_lines
kill -CONT "$CLIENT_PID"
wait_line 'resync: lagged' 20 || die "no lagged resync: $(tail -n 20 "$OUT")"
L_LAG=$(grep -n -m1 'resync: lagged' "$OUT" | cut -d: -f1)
for _ in $(seq 1 100); do awk -v s="$L_LAG" 'NR > s && /^snapshot:/ { found = 1 } END { exit !found }' "$OUT" && break; sleep 0.1; done
sleep 0.5
L_SNAP3=$(awk -v s="$L_LAG" 'NR > s && /^snapshot:/ { print NR; exit }' "$OUT")
ROWS_SQL=$(awk -v s="${L_SNAP3:-0}" 'NR > s && /^= /' "$OUT" | sed 's/^= //' | awk '{ printf "%s$j$%s$j$", (NR > 1 ? "," : ""), $0 }')
STRIP="to_jsonb(v) - ARRAY(SELECT k FROM jsonb_object_keys(to_jsonb(v)) k WHERE k LIKE '\_nabla\_%')"
SNAP_DIFF=$(q "SELECT count(*) FROM ((SELECT x FROM unnest(ARRAY[$ROWS_SQL]::jsonb[]) x EXCEPT SELECT $STRIP FROM order_lines v)
  UNION ALL (SELECT $STRIP FROM order_lines v EXCEPT SELECT x FROM unnest(ARRAY[$ROWS_SQL]::jsonb[]) x)) d")
assert_eq "snapshot after the lagged resync equals the view (hidden columns stripped)" "0|$(q "SELECT count(*) FROM order_lines")" \
  "$SNAP_DIFF|$(awk -v s="${L_SNAP3:-0}" 'NR > s && /^= /' "$OUT" | wc -l | tr -d ' ')"
LSNS=$(grep -oE '^tx lsn=[0-9A-F]+/[0-9A-F]+' "$OUT" | sed "s/^tx lsn=//" | awk '{ printf "%s'"'"'%s'"'"'", (NR > 1 ? "," : ""), $0 }')
assert_eq "transaction lsns strictly increase across the whole run" "0" \
  "$(q "SELECT count(*) FROM (SELECT l, lag(l) OVER (ORDER BY ord) prev FROM unnest(ARRAY[$LSNS]::pg_lsn[]) WITH ORDINALITY t(l, ord)) s WHERE prev IS NOT NULL AND l <= prev")"
assert_eq "no delta seq is printed twice" "0" "$(grep -oE '^  [0-9]+ ' "$OUT" | sort | uniq -d | wc -l | tr -d ' ')"
assert_eq "client stderr is empty" "" "$(cat /tmp/follow.err)"
kill -0 "$CLIENT_PID" 2>/dev/null && pass "client is still running at the end" || fail "client alive" "client exited early: $(tail -n 5 "$OUT")"
kill -INT "$CLIENT_PID"
wait "$CLIENT_PID"
CLIENT_RC=$?
CLIENT_PID=""
assert_eq "client exits cleanly on SIGINT" "0|1" "$CLIENT_RC|$(grep -c '^interrupted$' "$OUT")"

# --- 20. API contract ----------------------------------------------------------
echo "== 20. API contract (epochs, SQLSTATEs, whole transactions, hidden columns)"
CANON=$(q "SELECT nabla.create_view('Public.Canon_Test', 'SELECT id, k FROM orders')")
await_ready public.canon_test
assert_eq "create_view returns the canonical name and status() agrees" "public.canon_test|public.canon_test" \
  "$CANON|$(q "SELECT name FROM nabla.status('PUBLIC.canon_test')")"
wait_view public.canon_test
EPOCH_C=$(q "SELECT epoch FROM nabla.status('public.canon_test')")
CUR_C=$(q "SELECT current_seq FROM nabla.status('public.canon_test')")
q "INSERT INTO orders (k, amount, status) VALUES (701, 1, 'paid')" >/dev/null
q "INSERT INTO orders (k, amount, status) VALUES (702, 1, 'paid')" >/dev/null
q "INSERT INTO orders (k, amount, status) SELECT 700, i, 'paid' FROM generate_series(1, 30) i" >/dev/null
wait_view public.canon_test
assert_eq "changes() never splits a transaction (30 rows, max_rows = 10)" "30|1" \
  "$(q "SELECT count(*) || '|' || count(DISTINCT (xid, lsn)) FROM nabla.changes('public.canon_test', $((CUR_C + 2)), $EPOCH_C, 10)")"
assert_eq "a page ending inside a transaction is extended to its end (2 + 30 rows for max_rows = 10)" "32|3" \
  "$(q "SELECT count(*) || '|' || count(DISTINCT (xid, lsn)) FROM nabla.changes('public.canon_test', $CUR_C, $EPOCH_C, 10)")"
assert_eq "a result shorter than max_rows is drained" "2|0" \
  "$(q "SELECT count(*) FROM nabla.changes('public.canon_test', $CUR_C, $EPOCH_C, 2)")|$(q "SELECT count(*) FROM nabla.changes('public.canon_test', $((CUR_C + 32)), $EPOCH_C, 10)")"
assert_eq "changes() rows use driver-friendly types" "bigint,text,bigint,text,jsonb" \
  "$(q "SELECT string_agg(format_type(t.oid, NULL), ',' ORDER BY a.ord) FROM pg_proc p, unnest(p.proallargtypes) WITH ORDINALITY a(oid, ord) JOIN pg_type t ON t.oid = a.oid WHERE p.proname = 'changes' AND p.pronamespace = 'nabla'::regnamespace AND a.ord > 5")"
q "SELECT nabla.refresh('public.canon_test')" >/dev/null || die "refresh(canon_test) failed"
await_ready public.canon_test
assert_eq "a refresh with a live cursor is reported as NB003, never as lagged" "NB003" \
  "$(sqlstate_of "SELECT count(*) FROM nabla.changes('public.canon_test', $((CUR_C + 32)), $EPOCH_C)")"
assert_contains "NB003 carries the epochs in DETAIL" "DETAIL:  epoch $EPOCH_C -> $((EPOCH_C + 1))" \
  "$(q_err "SELECT count(*) FROM nabla.changes('public.canon_test', $((CUR_C + 32)), $EPOCH_C)")"
assert_eq "the new epoch works with the current cursor" "0" \
  "$(q "SELECT count(*) FROM nabla.changes('public.canon_test', (SELECT current_seq FROM nabla.status('public.canon_test')), $((EPOCH_C + 1)))")"
assert_eq "visible_columns() lists the definition's output names" "{id,k}|{k,n,total}|{order_id,customer,product,total}" \
  "$(q "SELECT nabla.visible_columns('public.canon_test')")|$(q "SELECT nabla.visible_columns('public.orders_by_k')")|$(q "SELECT nabla.visible_columns('public.order_lines')")"
SEQ_K=$(q "SELECT current_seq FROM nabla.status('public.orders_by_k')")
EPOCH_K=$(q "SELECT epoch FROM nabla.status('public.orders_by_k')")
q "INSERT INTO orders (k, amount, status) VALUES (703, 2, 'paid')" >/dev/null
wait_view public.orders_by_k
assert_eq "delta rows hide _nabla_* columns by default and expose them with include_hidden" "f|t" \
  "$(q "SELECT bool_or(row ? '_nabla_nn_1') FROM nabla.changes('public.orders_by_k', $SEQ_K, $EPOCH_K)")|$(q "SELECT bool_or(row ? '_nabla_nn_1') FROM nabla.changes('public.orders_by_k', $SEQ_K, $EPOCH_K, 1000, true)")"
# The first call waits on the WAL position after the worker's own bookkeeping
# commit: it is satisfied through the idle window, not through a user write.
assert_eq "wait_for() accepts the text LSN form" "t|t" \
  "$(q "SELECT nabla.wait_for('public.orders_by_k', pg_current_wal_lsn()::text, 5000)")|$(q "SELECT nabla.wait_for('public.orders_by_k', (SELECT frontier FROM nabla.status('public.orders_by_k')), 5000)")"
assert_eq "SQLSTATE NB004 for unsupported definitions" "NB004" "$(sqlstate_of "SELECT nabla.create_view('public.x', 'SELECT DISTINCT id FROM orders')")"
assert_eq "SQLSTATE NB005 for direct writes" "NB005" "$(sqlstate_of "DELETE FROM canon_test")"
assert_eq "the client branches on SQLSTATEs, never on message text" "0" \
  "$(grep -cE 'lagged behind|is stale' /work/clients/rust/nabla-client/src/lib.rs)"
q "SELECT nabla.drop_view('public.canon_test')" >/dev/null

# --- 21. non-blocking create and refresh ------------------------------------
echo "== 21. non-blocking create and refresh (consistent snapshot)"
q "CREATE TABLE shop.events (id bigserial PRIMARY KEY, k int, amount numeric)" >/dev/null
q "ALTER SYSTEM SET nabla.debug_populate_delay_ms = 2000" >/dev/null
q "SELECT pg_reload_conf()" >/dev/null
sleep 0.3
psql -X -q -p "$PORT" -d "$DB" -c "BEGIN; INSERT INTO shop.events (k, amount) VALUES (1, 10); SELECT pg_sleep(3); COMMIT" >/dev/null 2>&1 &
WRITER_PID=$!
sleep 0.4
START_NS=$(date +%s%N)
CREATED=$(q "SELECT nabla.create_view('public.events_view', 'SELECT id, k, amount FROM shop.events')")
CREATE_MS=$(( ($(date +%s%N) - START_NS) / 1000000 ))
assert_eq "create_view returns immediately while a writer holds the table" "public.events_view|yes" \
  "$CREATED|$( [ "$CREATE_MS" -lt 500 ] && echo yes || echo "no ($CREATE_MS ms)" )"
assert_eq "the view is initializing" "initializing" "$(q "SELECT status FROM nabla.status('public.events_view')")"
START_NS=$(date +%s%N)
if q "SET lock_timeout = '1s'; INSERT INTO shop.events (k, amount) VALUES (2, 20)" >/dev/null 2>/tmp/nabla-c.err; then
  ELAPSED_MS=$(( ($(date +%s%N) - START_NS) / 1000000 ))
  if [ "$ELAPSED_MS" -lt 1000 ]; then pass "a second writer committed in ${ELAPSED_MS} ms while the view initializes"
  else fail "no lock during create" "commit took ${ELAPSED_MS} ms"; fi
else
  fail "no lock during create" "insert failed: $(cat /tmp/nabla-c.err)"
fi
wait "$WRITER_PID" 2>/dev/null; WRITER_PID=""
# Let the consistent point be found without waiting for the bgwriter's periodic record.
q "SELECT pg_log_standby_snapshot()" >/dev/null
VIEW_ID=$(q "SELECT id FROM nabla.views WHERE name = 'public.events_view'")
INVARIANT=unseen
for _ in $(seq 1 300); do
  ROW=$(q "SELECT ((SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = 'nabla') <= t.confirmed_flush_lsn) || '|' || t.temporary
           FROM pg_replication_slots t WHERE t.slot_name = 'nabla_init_$VIEW_ID' AND t.confirmed_flush_lsn IS NOT NULL")
  if [ -n "$ROW" ]; then INVARIANT=$ROW; break; fi
  [ "$(q "SELECT status FROM nabla.status('public.events_view')")" = live ] && break
  sleep 0.1
done
assert_eq "main slot confirmed_flush <= temporary init slot's consistent point (slot is temporary)" "true|true" "$INVARIANT"
assert_eq "await_ready() returns true" "t" "$(q "SELECT nabla.await_ready('public.events_view', 60000)")"
assert_eq "the temporary init slot is gone" "0" "$(q "SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'nabla_init_%'")"
assert_eq "the view has both rows: the one in flight during create and the concurrent one" "1,2|0" \
  "$(q "SELECT string_agg(k::text, ',' ORDER BY k) FROM events_view")|$(q "SELECT count(*) FROM ((SELECT id, k, amount FROM events_view EXCEPT SELECT id, k, amount FROM shop.events) UNION ALL (SELECT id, k, amount FROM shop.events EXCEPT SELECT id, k, amount FROM events_view)) d")"
q "INSERT INTO shop.events (k, amount) VALUES (3, 30)" >/dev/null
wait_view public.events_view
assert_eq "a row committed after await_ready is applied through the normal path" "1,2,3" "$(q "SELECT string_agg(k::text, ',' ORDER BY k) FROM events_view")"
q "ALTER SYSTEM RESET nabla.debug_populate_delay_ms" >/dev/null
q "SELECT pg_reload_conf()" >/dev/null

# Refresh is non-disruptive: readers, writers and old-epoch subscribers are unaffected.
q "SELECT nabla.create_view('public.revenue_by_region', \$d\$$REV_DEF\$d\$)" >/dev/null || die "create_view(revenue_by_region) failed"
await_ready public.revenue_by_region
wait_view public.revenue_by_region
q "ALTER SYSTEM SET nabla.debug_populate_delay_ms = 3000" >/dev/null
q "SELECT pg_reload_conf()" >/dev/null
sleep 0.3
EPOCH_R=$(q "SELECT epoch FROM nabla.status('public.revenue_by_region')")
EPOCH_L=$(q "SELECT epoch FROM nabla.status('public.order_lines')")
CUR_R=$(q "SELECT current_seq FROM nabla.status('public.revenue_by_region')")
CONTENT_R=$(q "SELECT string_agg(region || ':' || n || ':' || revenue, ',' ORDER BY region) FROM revenue_by_region")
START_NS=$(date +%s%N)
q "SELECT nabla.refresh('public.revenue_by_region')" >/dev/null || die "refresh(revenue_by_region) failed to start"
REFRESH_MS=$(( ($(date +%s%N) - START_NS) / 1000000 ))
assert_eq "refresh returns immediately" "yes" "$( [ "$REFRESH_MS" -lt 500 ] && echo yes || echo "no ($REFRESH_MS ms)" )"
assert_eq "the view and the views sharing its shadows are refreshing" "refreshing|refreshing" \
  "$(q "SELECT status FROM nabla.status('public.revenue_by_region')")|$(q "SELECT status FROM nabla.status('public.order_lines')")"
sleep 1
assert_eq "readers still see the full old content and the old epoch while refreshing" "same|$EPOCH_R" \
  "$( [ "$(q "SELECT string_agg(region || ':' || n || ':' || revenue, ',' ORDER BY region) FROM revenue_by_region")" = "$CONTENT_R" ] && echo same || echo different )|$(q "SELECT epoch FROM nabla.status('public.revenue_by_region')")"
START_NS=$(date +%s%N)
if q "SET lock_timeout = '1s'; INSERT INTO shop.orders (customer_id, product_id, qty, status) VALUES (1, 1, 9, 'paid')" >/dev/null 2>/tmp/nabla-r.err; then
  ELAPSED_MS=$(( ($(date +%s%N) - START_NS) / 1000000 ))
  if [ "$ELAPSED_MS" -lt 1000 ]; then pass "a writer committed in ${ELAPSED_MS} ms while the view refreshes"
  else fail "refresh non-disruptive" "commit took ${ELAPSED_MS} ms"; fi
else
  fail "refresh non-disruptive" "insert failed: $(cat /tmp/nabla-r.err)"
fi
assert_eq "changes() with the old epoch still works while refreshing" "0" \
  "$(q "SELECT count(*) FROM nabla.changes('public.revenue_by_region', $CUR_R, $EPOCH_R)")"
assert_eq "await_ready() after the rebuild" "t" "$(q "SELECT nabla.await_ready('public.revenue_by_region', 60000)")"
wait_view public.revenue_by_region
wait_view public.order_lines
assert_eq "the epoch advanced by one on the view and on the cascaded view" "$((EPOCH_R + 1))|$((EPOCH_L + 1))" \
  "$(q "SELECT epoch FROM nabla.status('public.revenue_by_region')")|$(q "SELECT epoch FROM nabla.status('public.order_lines')")"
assert_eq "the rebuilt views equal their queries (including the write made during the rebuild)" "0|0" "$(rev_diff)|$(lines_diff)"
assert_eq "a cursor with the old epoch gets NB003 after the switch" "NB003" \
  "$(sqlstate_of "SELECT count(*) FROM nabla.changes('public.revenue_by_region', $CUR_R, $EPOCH_R)")"
q "ALTER SYSTEM RESET nabla.debug_populate_delay_ms" >/dev/null
q "SELECT pg_reload_conf()" >/dev/null

# Failure: the definition errors on existing data.
q "CREATE TABLE shop.divs (id serial PRIMARY KEY, k int)" >/dev/null
q "INSERT INTO shop.divs (k) VALUES (0), (4)" >/dev/null
q "SELECT nabla.create_view('public.bad_init', 'SELECT id, 100 / k AS ratio FROM shop.divs')" >/dev/null || die "create_view(bad_init) failed"
ERR=$(q_err "SELECT nabla.await_ready('public.bad_init', 60000)")
assert_contains "await_ready() raises the population error with PostgreSQL's message in DETAIL" "DETAIL:  division by zero" "$ERR"
assert_eq "the population error carries SQLSTATE NB006" "NB006" "$(sqlstate_of "SELECT nabla.await_ready('public.bad_init', 60000)")"
assert_eq "the view is failed and no table was left behind" "failed|t|0" \
  "$(q "SELECT status FROM nabla.status('public.bad_init')")|$(q "SELECT to_regclass('public.bad_init') IS NULL")|$(q "SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'nabla_init_%'")"
q "SELECT nabla.drop_view('public.bad_init')" >/dev/null || die "drop_view(bad_init) failed"
assert_eq "drop_view cleans a failed view" "0" "$(q "SELECT count(*) FROM nabla.views WHERE name = 'public.bad_init'")"

# --- 22. schema changes on base tables ------------------------------------------
echo "== 22. schema changes on base tables"
vdiff() { # view visible-columns definition
  q "SELECT count(*) FROM ((SELECT $2 FROM $1 EXCEPT $3) UNION ALL ($3 EXCEPT SELECT $2 FROM $1)) d"
}
shadow_cols() { q "SELECT array_to_string(columns, ',') FROM nabla.shadows WHERE relid = '$1'::regclass"; }
vstatus() { q "SELECT status FROM nabla.status('$1')"; }
warnings() { grep -c WARNING "$LOG"; }
wait_status() { # view status timeout_s
  local i
  for i in $(seq 1 $(( $3 * 10 ))); do [ "$(vstatus "$1")" = "$2" ] && return 0; sleep 0.1; done
  return 1
}
# Start from a clean shop: no join views over the shop tables.
q "SELECT nabla.drop_view('public.order_lines')" >/dev/null
q "SELECT nabla.drop_view('public.revenue_by_region')" >/dev/null
q "SELECT nabla.drop_view('public.events_view')" >/dev/null
q "ALTER TABLE shop.orders REPLICA IDENTITY FULL" >/dev/null
J1_DEF="SELECT o.id AS order_id, c.name AS customer, p.name AS product, o.qty * p.price AS total FROM shop.orders o JOIN shop.customers c ON c.id = o.customer_id JOIN shop.products p ON p.id = o.product_id WHERE o.status = 'paid'"
J2_DEF="SELECT o.id AS order_id, c.region FROM shop.orders o JOIN shop.customers c ON c.id = o.customer_id"
J3_DEF="SELECT o.id AS order_id, c.tier FROM shop.orders o JOIN shop.customers c ON c.id = o.customer_id"
A1_DEF="SELECT customer_id, count(*) AS n, sum(qty) AS q FROM shop.orders GROUP BY customer_id"
J1_COLS="order_id, customer, product, total"; J2_COLS="order_id, region"; J3_COLS="order_id, tier"; A1_COLS="customer_id, n, q"
q "SELECT nabla.create_view('public.j1', \$d\$$J1_DEF\$d\$)" >/dev/null || die "create_view(j1) failed"
await_ready public.j1
q "SELECT nabla.create_view('public.j2', \$d\$$J2_DEF\$d\$)" >/dev/null || die "create_view(j2) failed"
await_ready public.j2
q "SELECT nabla.create_view('public.a1', \$d\$$A1_DEF\$d\$)" >/dev/null || die "create_view(a1) failed"
await_ready public.a1
assert_eq "shadows hold only the primary key and the used columns" "id,name,region|id,customer_id,product_id,qty,status|id,name,price" \
  "$(shadow_cols shop.customers)|$(shadow_cols shop.orders)|$(shadow_cols shop.products)"
check_views() { # label
  wait_view public.j1; wait_view public.j2; wait_view public.a1
  assert_eq "$1: every live view equals its query" "0|0|0" "$(vdiff j1 "$J1_COLS" "$J1_DEF")|$(vdiff j2 "$J2_COLS" "$J2_DEF")|$(vdiff a1 "$A1_COLS" "$A1_DEF")"
}
W0=$(warnings)
# 1. an unused column is added
q "ALTER TABLE shop.customers ADD COLUMN email text" >/dev/null
q "UPDATE shop.customers SET email = 'a@example.com' WHERE id = 1" >/dev/null
q "INSERT INTO shop.customers (id, name, region, email) VALUES (5, 'Eve', 'west', 'e@example.com')" >/dev/null
q "INSERT INTO shop.orders (customer_id, product_id, qty, status) VALUES (5, 1, 2, 'paid')" >/dev/null
check_views "1. ADD COLUMN of an unused column"
assert_eq "1. views stay live, the shadow ignores the new column, no WARNING" "live|live|id,name,region|$W0" \
  "$(vstatus public.j1)|$(vstatus public.j2)|$(shadow_cols shop.customers)|$(warnings)"
# 2. the unused column is dropped; another is added and renamed
q "ALTER TABLE shop.customers DROP COLUMN email" >/dev/null
q "ALTER TABLE shop.customers ADD COLUMN nick text" >/dev/null
q "ALTER TABLE shop.customers RENAME COLUMN nick TO nick2" >/dev/null
q "UPDATE shop.customers SET nick2 = 'al', name = 'Alice' WHERE id = 1" >/dev/null
check_views "2. DROP/RENAME of unused columns"
assert_eq "2. views stay live, no WARNING" "live|live|$W0" "$(vstatus public.j1)|$(vstatus public.j2)|$(warnings)"
# 3. a column with a default is added and a new view starts using it
q "ALTER TABLE shop.customers ADD COLUMN tier text DEFAULT 'std'" >/dev/null
E1=$(q "SELECT epoch FROM nabla.status('public.j1')"); E2=$(q "SELECT epoch FROM nabla.status('public.j2')")
q "SELECT nabla.create_view('public.j3', \$d\$$J3_DEF\$d\$)" >/dev/null || die "create_view(j3) failed"
await_ready public.j3
assert_eq "3. the shared shadow gained the new column, backfilled from the base" "id,name,region,tier|0|std" \
  "$(shadow_cols shop.customers)|$(q "SELECT count(*) FROM j3 WHERE tier IS NULL")|$(q "SELECT DISTINCT tier FROM j3")"
assert_eq "3. the old views were never rebuilt" "live|live|$E1|$E2" \
  "$(vstatus public.j1)|$(vstatus public.j2)|$(q "SELECT epoch FROM nabla.status('public.j1')")|$(q "SELECT epoch FROM nabla.status('public.j2')")"
S1=$(q "SELECT current_seq FROM nabla.status('public.j1')"); S2=$(q "SELECT current_seq FROM nabla.status('public.j2')")
q "UPDATE shop.customers SET tier = 'vip' WHERE id = 1" >/dev/null
wait_view public.j3
assert_eq "3. a change of the new column reaches only the view that uses it" "0|vip|$S1|$S2" \
  "$(vdiff j3 "$J3_COLS" "$J3_DEF")|$(q "SELECT DISTINCT tier FROM j3 WHERE order_id IN (SELECT id FROM shop.orders WHERE customer_id = 1)")|$(q "SELECT current_seq FROM nabla.status('public.j1')")|$(q "SELECT current_seq FROM nabla.status('public.j2')")"
# 4. an update that touches only an unused column
S1=$(q "SELECT current_seq FROM nabla.status('public.j1')"); S2=$(q "SELECT current_seq FROM nabla.status('public.j2')")
S3=$(q "SELECT current_seq FROM nabla.status('public.j3')"); SA=$(q "SELECT current_seq FROM nabla.status('public.a1')")
q "UPDATE shop.customers SET nick2 = 'bobby' WHERE id = 2" >/dev/null
wait_view public.j1
assert_eq "4. an update of an unused column produces no delta and no WARNING" "$S1|$S2|$S3|$SA|$W0" \
  "$(q "SELECT current_seq FROM nabla.status('public.j1')")|$(q "SELECT current_seq FROM nabla.status('public.j2')")|$(q "SELECT current_seq FROM nabla.status('public.j3')")|$(q "SELECT current_seq FROM nabla.status('public.a1')")|$(warnings)"
# 5. a column used by one of two views sharing the shadow is dropped
q "ALTER TABLE shop.customers DROP COLUMN region" >/dev/null
q "UPDATE shop.customers SET name = 'Bobby' WHERE id = 2" >/dev/null
wait_status public.j2 stale 15 || die "j2 did not go stale: $(vstatus public.j2)"
assert_contains "5. the view using the dropped column is stale with a precise reason" \
  'column "region" of shop.customers was dropped, renamed or changed type' "$(q "SELECT stale_reason FROM nabla.status('public.j2')")"
wait_view public.j1; wait_view public.j3
assert_eq "5. the other views sharing the shadow stay live and receive deltas" "live|live|0|0|id,name,tier" \
  "$(vstatus public.j1)|$(vstatus public.j3)|$(vdiff j1 "$J1_COLS" "$J1_DEF")|$(vdiff j3 "$J3_COLS" "$J3_DEF")|$(shadow_cols shop.customers)"
q "SELECT nabla.refresh('public.j2')" >/dev/null || die "refresh(j2) failed to start"
ERR=$(q_err "SELECT nabla.await_ready('public.j2', 60000)")
assert_contains "5. refresh of the stale view fails with PostgreSQL's error" "does not exist" "$ERR"
assert_eq "5. the failed refresh carries NB006" "NB006" "$(sqlstate_of "SELECT nabla.await_ready('public.j2', 60000)")"
q "SELECT nabla.drop_view('public.j2')" >/dev/null || die "drop_view(j2) failed"
assert_eq "5. drop_view cleans the failed view" "0" "$(q "SELECT count(*) FROM nabla.views WHERE name = 'public.j2'")"
# 6. a used column changes type
q "ALTER TABLE shop.products ALTER COLUMN price TYPE double precision" >/dev/null
q "UPDATE shop.products SET price = price + 0.5 WHERE id = 1" >/dev/null
wait_status public.j1 stale 15 || die "j1 did not go stale after the type change: $(vstatus public.j1)"
assert_contains "6. a type change of a used column marks the views using it stale" \
  'column "price" of shop.products was dropped, renamed or changed type' "$(q "SELECT stale_reason FROM nabla.status('public.j1')")"
wait_view public.j3; wait_view public.a1
assert_eq "6. views not using the column are unaffected" "live|live" "$(vstatus public.j3)|$(vstatus public.a1)"
q "SELECT nabla.refresh('public.j1')" >/dev/null || die "refresh(j1) failed to start"
await_ready public.j1
wait_view public.j1
assert_eq "6. refresh recovers the view with the new type" "live|0|id,name,price" \
  "$(vstatus public.j1)|$(vdiff j1 "$J1_COLS" "$J1_DEF")|$(shadow_cols shop.products)"
# 7. dropping a base table without CASCADE is refused
ERR=$(q_err "DROP TABLE shop.customers")
assert_contains "7. DROP TABLE of a base table is refused because nabla tables depend on it" "other objects depend on it" "$ERR"
assert_contains "7. the error names the dependent view" "j1" "$ERR"
q "INSERT INTO shop.orders (customer_id, product_id, qty, status) VALUES (1, 2, 3, 'paid')" >/dev/null
wait_view public.j1; wait_view public.j3; wait_view public.a1
assert_eq "7. views are still live and maintained" "0|0|0" "$(vdiff j1 "$J1_COLS" "$J1_DEF")|$(vdiff j3 "$J3_COLS" "$J3_DEF")|$(vdiff a1 "$A1_COLS" "$A1_DEF")"
# 8. dropping a base table with CASCADE cleans everything that depended on it
W1=$(warnings)
q "DROP TABLE shop.customers CASCADE" >/dev/null || die "DROP TABLE CASCADE failed"
assert_eq "8. views over the dropped table and their shadows are gone" "0|0|0|0|0" \
  "$(q "SELECT count(*) FROM nabla.views WHERE name IN ('public.j1', 'public.j3')")|$(q "SELECT count(*) FROM nabla.view_relations vr WHERE NOT EXISTS (SELECT 1 FROM nabla.views v WHERE v.id = vr.view_id)")|$(q "SELECT count(*) FROM nabla.shadows")|$(q "SELECT count(*) FROM pg_tables WHERE schemaname = 'nabla_shadow'")|$(q "SELECT count(*) FROM pg_publication_tables WHERE pubname = 'nabla' AND tablename = 'customers'")"
assert_eq "8. the view tables are gone from disk" "t|t" "$(q "SELECT to_regclass('public.j1') IS NULL")|$(q "SELECT to_regclass('public.j3') IS NULL")"
q "INSERT INTO shop.orders (customer_id, product_id, qty, status) VALUES (2, 2, 4, 'paid')" >/dev/null
wait_view public.a1
assert_eq "8. the single-table view is unaffected, no WARNING, worker alive" "live|0|$W1|1" \
  "$(vstatus public.a1)|$(vdiff a1 "$A1_COLS" "$A1_DEF")|$(warnings)|$(q "SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'nabla worker'")"
# 9. dropping a view table directly
q "SELECT nabla.create_view('public.j4', 'SELECT o.id AS order_id, p.name AS product FROM shop.orders o JOIN shop.products p ON p.id = o.product_id')" >/dev/null || die "create_view(j4) failed"
await_ready public.j4
assert_eq "9. the new join view created its shadows" "2" "$(q "SELECT count(*) FROM nabla.shadows WHERE refcount = 1")"
q "DROP TABLE public.j4" >/dev/null || die "DROP TABLE j4 failed"
assert_eq "9. dropping the view table cleans the catalog and the orphan shadows" "0|0|0" \
  "$(q "SELECT count(*) FROM nabla.views WHERE name = 'public.j4'")|$(q "SELECT count(*) FROM nabla.shadows")|$(q "SELECT count(*) FROM pg_tables WHERE schemaname = 'nabla_shadow'")"
# Replay of the hands-on session that found these bugs.
q "CREATE TABLE public.play_customers (id int PRIMARY KEY, name text, region text)" >/dev/null
q "CREATE TABLE public.play_orders (id serial PRIMARY KEY, customer_id int, amount numeric)" >/dev/null
q "INSERT INTO public.play_customers VALUES (1, 'Alice', 'north'), (2, 'Bob', 'south')" >/dev/null
q "INSERT INTO public.play_orders (customer_id, amount) VALUES (1, 10), (2, 20)" >/dev/null
SBR_DEF="SELECT c.region, count(*) AS n, sum(o.amount) AS total FROM public.play_orders o JOIN public.play_customers c ON c.id = o.customer_id GROUP BY c.region"
q "SELECT nabla.create_view('public.sales_by_region', \$d\$$SBR_DEF\$d\$)" >/dev/null || die "create_view(sales_by_region) failed"
await_ready public.sales_by_region
W2=$(warnings)
q "ALTER TABLE public.play_customers ADD COLUMN email text" >/dev/null
q "UPDATE public.play_customers SET name = 'Alicia' WHERE id = 1" >/dev/null
wait_view public.sales_by_region
assert_eq "replay: ADD COLUMN plus a write keeps the view live and equal, no WARNING" "live|0|$W2" \
  "$(vstatus public.sales_by_region)|$(vdiff sales_by_region "region, n, total" "$SBR_DEF")|$(warnings)"
ERR=$(q_err "DROP TABLE public.play_customers")
assert_contains "replay: DROP TABLE is refused" "other objects depend on it" "$ERR"
q "DROP TABLE public.play_customers CASCADE" >/dev/null || die "DROP TABLE play_customers CASCADE failed"
assert_eq "replay: CASCADE leaves no dangling catalog rows or shadows" "0|0|0" \
  "$(q "SELECT count(*) FROM nabla.views WHERE name = 'public.sales_by_region'")|$(q "SELECT count(*) FROM nabla.shadows")|$(q "SELECT count(*) FROM nabla.view_relations vr WHERE NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.oid = vr.relid)")"
assert_contains "replay: drop_view of the vanished view reports it cleanly" 'view "public.sales_by_region" does not exist' "$(q_err "SELECT nabla.drop_view('public.sales_by_region')")"
q "DROP TABLE public.play_orders" >/dev/null
# 10. dropping the whole schema
W3=$(warnings)
q "DROP SCHEMA shop CASCADE" >/dev/null || die "DROP SCHEMA shop CASCADE failed"
assert_eq "10. DROP SCHEMA CASCADE leaves no view over shop tables, no shadows, no shop tables in the publication" "0|0|0|0" \
  "$(q "SELECT count(*) FROM nabla.views WHERE name IN ('public.a1')")|$(q "SELECT count(*) FROM nabla.view_relations vr WHERE NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.oid = vr.relid)")|$(q "SELECT count(*) FROM nabla.shadows")|$(q "SELECT count(*) FROM pg_publication_tables WHERE pubname = 'nabla' AND schemaname = 'shop'")"
sleep 1
assert_eq "10. worker alive and quiet, no WARNING" "1|$W3" \
  "$(q "SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'nabla worker'")|$(warnings)"
wait_view public.paid_orders
assert_eq "10. views over other tables keep working" "0" \
  "$(q "SELECT count(*) FROM ((SELECT id, k, amount FROM paid_orders EXCEPT SELECT id, k, amount FROM orders WHERE status = 'paid') UNION ALL (SELECT id, k, amount FROM orders WHERE status = 'paid' EXCEPT SELECT id, k, amount FROM paid_orders)) d")"

# --- 23. netted deltas per source transaction ------------------------------------
echo "== 23. netted deltas per source transaction"
q "CREATE SCHEMA net" >/dev/null
q "CREATE TABLE net.customers (id int PRIMARY KEY, name text, region text)" >/dev/null
q "CREATE TABLE net.products (id int PRIMARY KEY, name text, price numeric)" >/dev/null
q "CREATE TABLE net.orders (id serial PRIMARY KEY, customer_id int, product_id int, qty int, status text)" >/dev/null
q "ALTER TABLE net.orders REPLICA IDENTITY FULL" >/dev/null
q "INSERT INTO net.customers VALUES (1, 'Alice', 'AR'), (2, 'Bob', 'BR'), (4, 'Dora', 'AR')" >/dev/null
q "INSERT INTO net.products VALUES (1, 'pen', 10), (2, 'book', 20)" >/dev/null
q "INSERT INTO net.orders (customer_id, product_id, qty, status) VALUES (1, 1, 2, 'paid'), (2, 1, 3, 'paid'), (2, 2, 1, 'paid'), (1, 2, 1, 'paid'), (4, 1, 1, 'paid')" >/dev/null
REV_DEF="SELECT c.region, count(*) AS n, sum(o.qty * p.price) AS revenue FROM net.orders o JOIN net.customers c ON c.id = o.customer_id JOIN net.products p ON p.id = o.product_id WHERE o.status = 'paid' GROUP BY c.region"
LIN_DEF="SELECT o.id AS order_id, c.region, o.qty FROM net.orders o JOIN net.customers c ON c.id = o.customer_id WHERE o.status = 'paid'"
AGG_DEF="SELECT customer_id, count(*) AS n, sum(qty) AS q FROM net.orders GROUP BY customer_id"
for spec in "public.net_rev|$REV_DEF" "public.net_lines|$LIN_DEF" "public.net_agg|$AGG_DEF"; do
  q "SELECT nabla.create_view('${spec%%|*}', \$d\$${spec#*|}\$d\$)" >/dev/null || die "create_view(${spec%%|*}) failed"
  await_ready "${spec%%|*}"
done
net_check() { # label
  wait_view public.net_rev; wait_view public.net_lines; wait_view public.net_agg
  assert_eq "$1: views equal their queries" "0|0|0" \
    "$(vdiff net_rev "region, n, revenue" "$REV_DEF")|$(vdiff net_lines "order_id, region, qty" "$LIN_DEF")|$(vdiff net_agg "customer_id, n, q" "$AGG_DEF")"
}
# netted deltas of a view since a cursor, as op:key:value in seq order
rev_deltas() { q "SELECT coalesce(string_agg(op || ':' || (row->>'region') || ':' || (row->>'n'), ',' ORDER BY seq), '') FROM $(CH public.net_rev "$1")"; }
lin_deltas() { q "SELECT coalesce(string_agg(op || ':' || (row->>'order_id') || ':' || (row->>'qty'), ',' ORDER BY seq), '') FROM $(CH public.net_lines "$1")"; }
agg_deltas() { q "SELECT coalesce(string_agg(op || ':' || (row->>'customer_id') || ':' || (row->>'n'), ',' ORDER BY seq), '') FROM $(CH public.net_agg "$1")"; }
cur() { q "SELECT current_seq FROM nabla.status('$1')"; }
q "INSERT INTO net.customers VALUES (3, 'Cleo', 'CL')" >/dev/null
net_check "setup"
# the reference client follows the aggregate view through steps 1-6
NET_OUT=/tmp/follow-net.out; : > "$NET_OUT"
"$FOLLOW" "$CONN" public.net_rev > "$NET_OUT" 2>/tmp/follow-net.err &
CLIENT_PID=$!
for i in $(seq 1 100); do grep -q '^snapshot:' "$NET_OUT" && break; sleep 0.1; done
# 1. three paid orders for a new region in one transaction
R0=$(cur public.net_rev); L0=$(cur public.net_lines); A0=$(cur public.net_agg)
q "INSERT INTO net.orders (customer_id, product_id, qty, status) VALUES (3, 1, 1, 'paid'), (3, 1, 2, 'paid'), (3, 2, 1, 'paid')" >/dev/null
net_check "1. new region"
assert_eq "1. one I with the final group row; the projection gets three I" "I:CL:3|3|I:3:3" \
  "$(rev_deltas "$R0")|$(q "SELECT count(*) FROM $(CH public.net_lines "$L0") WHERE op = 'I'")|$(agg_deltas "$A0")"
# 2. insert two rows and delete them again in one transaction
R0=$(cur public.net_rev); L0=$(cur public.net_lines); A0=$(cur public.net_agg); F0=$(q "SELECT nabla.frontier('public.net_rev')")
q "BEGIN; INSERT INTO net.orders (customer_id, product_id, qty, status) VALUES (1, 1, 99, 'paid'), (1, 2, 99, 'paid'); DELETE FROM net.orders WHERE qty = 99; COMMIT" >/dev/null
net_check "2. insert then delete"
assert_eq "2. zero deltas on every view, cursors unchanged, frontier advanced" "|||$R0|$L0|$A0|t" \
  "$(rev_deltas "$R0")|$(lin_deltas "$L0")|$(agg_deltas "$A0")|$(cur public.net_rev)|$(cur public.net_lines)|$(cur public.net_agg)|$(q "SELECT nabla.frontier('public.net_rev') > '$F0'::pg_lsn")"
# 3. qty 2 -> 5 -> 2 in one transaction
R0=$(cur public.net_rev); L0=$(cur public.net_lines); A0=$(cur public.net_agg)
q "BEGIN; UPDATE net.orders SET qty = 5 WHERE id = 1; UPDATE net.orders SET qty = 2 WHERE id = 1; COMMIT" >/dev/null
net_check "3. update and revert"
assert_eq "3. zero deltas" "||" "$(rev_deltas "$R0")|$(lin_deltas "$L0")|$(agg_deltas "$A0")"
# 4. every order of a region deleted in one transaction
R0=$(cur public.net_rev); L0=$(cur public.net_lines); A0=$(cur public.net_agg)
q "DELETE FROM net.orders WHERE customer_id = 3" >/dev/null
net_check "4. region emptied"
assert_eq "4. one D carrying the pre-transaction group row" "D:CL:3|3|D:3:3" \
  "$(rev_deltas "$R0")|$(q "SELECT count(*) FROM $(CH public.net_lines "$L0") WHERE op = 'D'")|$(agg_deltas "$A0")"
# 5. two orders move from AR to BR through a customer update
R0=$(cur public.net_rev)
AR_BEFORE=$(q "SELECT n FROM net_rev WHERE region = 'AR'"); BR_BEFORE=$(q "SELECT n FROM net_rev WHERE region = 'BR'")
q "UPDATE net.customers SET region = 'BR' WHERE id = 1" >/dev/null
net_check "5. region move"
assert_eq "5. D(AR before), I(AR after), D(BR before), I(BR after)" "D:AR:$AR_BEFORE,I:AR:$((AR_BEFORE - 2)),D:BR:$BR_BEFORE,I:BR:$((BR_BEFORE + 2))" "$(rev_deltas "$R0")"
# 6. multi-table transaction: a new customer and its orders
R0=$(cur public.net_rev); L0=$(cur public.net_lines); A0=$(cur public.net_agg)
q "BEGIN; INSERT INTO net.customers VALUES (5, 'Eve', 'EV'); INSERT INTO net.orders (customer_id, product_id, qty, status) VALUES (5, 1, 1, 'paid'), (5, 2, 2, 'paid'); COMMIT" >/dev/null
net_check "6. multi-table"
assert_eq "6. one I per affected group, no intermediate rows" "I:EV:2|2|I:5:2" \
  "$(rev_deltas "$R0")|$(q "SELECT count(*) FROM $(CH public.net_lines "$L0") WHERE op = 'I'")|$(agg_deltas "$A0")"
sleep 1.5
kill -INT "$CLIENT_PID"; wait "$CLIENT_PID" 2>/dev/null; CLIENT_PID=""
assert_eq "9. the client saw one tx line per netted transaction with the netted counts" "deltas=1,deltas=1,deltas=4,deltas=1" \
  "$(grep -E '^tx ' "$NET_OUT" | grep -oE 'deltas=[0-9]+' | paste -sd, -)"
assert_eq "9. the client never printed an intermediate group row" "0" \
  "$(grep -cE '"n":(1|2),"region":"CL"|"n":1,"region":"EV"' "$NET_OUT")"
# 7. hidden-only change on an aggregate without count(*)
q "CREATE TABLE net.hv (id serial PRIMARY KEY, k int, v int)" >/dev/null
q "ALTER TABLE net.hv REPLICA IDENTITY FULL" >/dev/null
q "INSERT INTO net.hv (k, v) VALUES (1, 5)" >/dev/null
q "SELECT nabla.create_view('public.net_sumv', 'SELECT k, sum(v) AS sv FROM net.hv GROUP BY k')" >/dev/null || die "create_view(net_sumv) failed"
await_ready public.net_sumv
S0=$(cur public.net_sumv); E0=$(q "SELECT epoch FROM nabla.status('public.net_sumv')")
q "INSERT INTO net.hv (k, v) VALUES (1, NULL)" >/dev/null
wait_view public.net_sumv
assert_eq "7. a change of hidden counters only is silent and the view stays correct" "$S0|0|1:5" \
  "$(cur public.net_sumv)|$(vdiff net_sumv "k, sv" "SELECT k, sum(v) FROM net.hv GROUP BY k")|$(q "SELECT k || ':' || sv FROM net_sumv")"
q "INSERT INTO net.hv (k, v) VALUES (1, 1)" >/dev/null
wait_view public.net_sumv
assert_eq "7. a later delta nets to D(before), I(after) and exposes the updated hidden counters with include_hidden" "D|2|1,I|3|2" \
  "$(q "SELECT string_agg(op || '|' || (row->>'_nabla_n') || '|' || (row->>'_nabla_nn_0'), ',' ORDER BY seq) FROM nabla.changes('public.net_sumv', $S0, $E0, 1000, true)")"
# 8. projection: insert then update twice; flip status back and forth
L0=$(cur public.net_lines)
q "BEGIN; INSERT INTO net.orders (id, customer_id, product_id, qty, status) VALUES (900, 2, 1, 1, 'paid'); UPDATE net.orders SET qty = 7 WHERE id = 900; UPDATE net.orders SET qty = 9 WHERE id = 900; COMMIT" >/dev/null
net_check "8a. insert and update twice"
assert_eq "8a. one I with the final row" "I:900:9" "$(lin_deltas "$L0")"
L0=$(cur public.net_lines)
q "BEGIN; UPDATE net.orders SET status = 'new' WHERE id = 900; UPDATE net.orders SET status = 'paid' WHERE id = 900; COMMIT" >/dev/null
net_check "8b. flip and flip back"
assert_eq "8b. zero deltas" "" "$(lin_deltas "$L0")"
# replay of the hands-on session
R0=$(cur public.net_rev)
q "BEGIN; INSERT INTO net.customers VALUES (6, 'Dani', 'NR'); INSERT INTO net.orders (customer_id, product_id, qty, status) VALUES (6, 1, 5, 'paid'), (6, 2, 1, 'paid'); COMMIT" >/dev/null
net_check "replay: Dani"
assert_eq "replay: Dani plus two orders nets to one I" "I:NR:2" "$(rev_deltas "$R0")"
R0=$(cur public.net_rev); BR_N=$(q "SELECT n FROM net_rev WHERE region = 'BR'")
q "DELETE FROM net.orders WHERE customer_id IN (SELECT id FROM net.customers WHERE region = 'BR')" >/dev/null
net_check "replay: BR emptied"
assert_eq "replay: deleting every BR row nets to one D with the pre-transaction row" "D:BR:$BR_N" "$(rev_deltas "$R0")"

# --- summary -----------------------------------------------------------------
echo "== server log (warnings and errors)"
grep -E 'WARNING|ERROR|FATAL|PANIC' "$LOG" | tail -n 20 || true
if [ "$FAILED" -ne 0 ]; then
  echo "RESULT: FAIL"
  exit 1
fi
echo "RESULT: PASS"
