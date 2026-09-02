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

pass() { printf 'PASS  %s\n' "$1"; }
fail() { printf 'FAIL  %s\n' "$1"; printf '      %s\n' "$2"; FAILED=1; }
die()  { printf 'FATAL %s\n' "$1"; exit 1; }

# psql wrapper: unaligned, tuples only, stop on error.
q() { psql -X -q -A -t -v ON_ERROR_STOP=1 -p "$PORT" -d "$DB" -c "$1"; }
# Run a statement expected to fail; print its stderr.
q_err() { psql -X -q -A -t -p "$PORT" -d "$DB" -c "$1" 2>&1 >/dev/null; }

assert_eq() { # label expected actual
  if [ "$2" = "$3" ]; then pass "$1"; else fail "$1" "expected [$2], got [$3]"; fi
}
assert_contains() { # label needle haystack
  case "$3" in *"$2"*) pass "$1" ;; *) fail "$1" "expected text containing [$2], got [$3]" ;; esac
}

cleanup() {
  [ -n "$POLLER_PID" ] && kill "$POLLER_PID" 2>/dev/null
  [ -n "$WRITER_PID" ] && kill "$WRITER_PID" 2>/dev/null
  if [ -f "$PGDATA/postmaster.pid" ]; then
    "$PG_BIN/pg_ctl" -D "$PGDATA" -m immediate stop >/dev/null 2>&1
  fi
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
# 5 inserts into a fresh group produce 1 + 4*2 = 9 deltas, all from one source transaction.
assert_eq "deltas of one source transaction share lsn and xid" "9|1|1" \
  "$(q "SELECT count(*) || '|' || count(DISTINCT lsn) || '|' || count(DISTINCT xid) FROM nabla.changes('public.orders_by_k', $SEQ0)")"
assert_eq "last delta of the transaction is the final group row" "I|5" \
  "$(q "SELECT op::text || '|' || (row->>'n') FROM nabla.changes('public.orders_by_k', $SEQ0) ORDER BY seq DESC LIMIT 1")"

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
CHECK=$(q "SELECT count(*) || '|' || bool_and(seq = $SEQ0 + rn) || '|' || bool_and(lsn > prev_lsn) || '|' || bool_and(op = 'I')
  FROM (SELECT seq, lsn, op, row_number() OVER (ORDER BY seq) rn, lag(lsn, 1, '0/0') OVER (ORDER BY seq) prev_lsn
        FROM nabla.changes('public.paid_orders', $SEQ0)) c")
assert_eq "changes() returns the 10 transactions in order with increasing seq and lsn" "10|true|true|true" "$CHECK"
assert_eq "changes() past the cursor is empty" "0" "$(q "SELECT count(*) FROM nabla.changes('public.paid_orders', $SEQ0 + 10)")"
q "INSERT INTO orders (k, amount, status) SELECT 12, i, 'paid' FROM generate_series(1, 60) i" >/dev/null
wait_view public.paid_orders
ERR=$(q_err "SELECT count(*) FROM nabla.changes('public.paid_orders', 0)")
assert_contains "changes() from a cursor older than retention raises the lagged error" \
  'nabla: subscriber lagged behind retention for view "public.paid_orders"' "$ERR"
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

# --- 8. refresh --------------------------------------------------------------
echo "== 8. refresh"
EPOCH0=$(q "SELECT epoch FROM nabla.views WHERE name = 'public.orders_by_k'")
CURSOR=$(q "SELECT nabla.current_seq('public.orders_by_k')")
q "SELECT nabla.refresh('public.orders_by_k')" >/dev/null || die "refresh failed"
EPOCH1=$(q "SELECT epoch FROM nabla.views WHERE name = 'public.orders_by_k'")
assert_eq "refresh bumps the epoch" "$((EPOCH0 + 1))" "$EPOCH1"
q "INSERT INTO orders (k, amount, status) VALUES (13, 3, 'paid')" >/dev/null
wait_view public.orders_by_k
DIFF=$(q "SELECT count(*) FROM (
  (SELECT k, n, total FROM orders_by_k EXCEPT SELECT k, count(*), sum(amount) FROM orders WHERE status = 'paid' GROUP BY k)
  UNION ALL
  (SELECT k, count(*), sum(amount) FROM orders WHERE status = 'paid' GROUP BY k EXCEPT SELECT k, n, total FROM orders_by_k)) d")
assert_eq "aggregate view equals its query after refresh" "0" "$DIFF"
ERR=$(q_err "SELECT count(*) FROM nabla.changes('public.orders_by_k', $CURSOR)")
assert_contains "a cursor from before the refresh must resync" "lagged behind retention" "$ERR"
# k=13 is a new group, so exactly one 'I' delta follows the refresh.
assert_eq "a fresh cursor after refresh works" "1" "$(q "SELECT count(*) FROM nabla.changes('public.orders_by_k', (SELECT resync_seq FROM nabla.views WHERE name = 'public.orders_by_k'))")"

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
q "INSERT INTO \"Orders\" (\"Amount\", \"Status\") VALUES (1, 'paid'), (2, 'new'), (3, 'paid')" >/dev/null
q "UPDATE \"Orders\" SET \"Status\" = 'paid' WHERE \"Id\" = 2" >/dev/null
q 'DELETE FROM "Orders" WHERE "Id" = 1' >/dev/null
wait_view public.paid_mixed
DIFF=$(q "SELECT count(*) FROM ((SELECT \"Id\", \"Amount\" FROM paid_mixed EXCEPT SELECT \"Id\", \"Amount\" FROM \"Orders\" WHERE \"Status\" = 'paid')
  UNION ALL (SELECT \"Id\", \"Amount\" FROM \"Orders\" WHERE \"Status\" = 'paid' EXCEPT SELECT \"Id\", \"Amount\" FROM paid_mixed)) d")
assert_eq "quoted mixed-case identifiers" "0|2" "$DIFF|$(q 'SELECT count(*) FROM paid_mixed')"
assert_eq "catalog records the fully qualified base table" "t" \
  "$(q "SELECT base_table = '\"Orders\"'::regclass AND spec->>'base_table' = 'public.\"Orders\"' FROM nabla.views WHERE name = 'public.paid_mixed'")"
assert_eq "unqualified name resolved through search_path is stored qualified" "t" \
  "$(q "SELECT base_table = 'public.orders'::regclass AND spec->>'base_table' = 'public.orders' FROM nabla.views WHERE name = 'public.paid_orders'")"

q "SELECT nabla.create_view('public.paid_aliased', 'SELECT o.id AS order_id, /* keep the amount */ o.amount
   FROM public.orders AS o
   WHERE o.status = ''paid'' -- only paid rows')" >/dev/null || die "create_view(paid_aliased) failed"
q "INSERT INTO orders (k, amount, status) VALUES (21, 7, 'paid'), (21, 8, 'new')" >/dev/null
wait_view public.paid_aliased
DIFF=$(q "SELECT count(*) FROM ((SELECT order_id, amount FROM paid_aliased EXCEPT SELECT id, amount FROM orders WHERE status = 'paid')
  UNION ALL (SELECT id, amount FROM orders WHERE status = 'paid' EXCEPT SELECT order_id, amount FROM paid_aliased)) d")
assert_eq "aliases, schema qualification and comments" "0" "$DIFF"

q "SELECT nabla.create_view('public.odd_status', 'SELECT id, k FROM orders WHERE status = ''order by limit''')" >/dev/null \
  || die "create_view(odd_status) failed"
q "INSERT INTO orders (k, amount, status) VALUES (22, 1, 'order by limit'), (22, 2, 'paid')" >/dev/null
wait_view public.odd_status
assert_eq "keywords inside a string literal" "1" "$(q "SELECT count(*) FROM odd_status WHERE k = 22")"

# --- 11. expressions ---------------------------------------------------------
echo "== 11. parser: immutable expressions"
q "SELECT nabla.create_view('public.orders_expr', 'SELECT id, amount * 2 AS doubled, upper(status) AS s FROM orders WHERE status <> ''void''')" >/dev/null \
  || die "create_view(orders_expr) failed"
q "INSERT INTO orders (k, amount, status) VALUES (23, 5, 'paid'), (23, 6, 'void')" >/dev/null
q "UPDATE orders SET amount = amount + 1, status = 'paid' WHERE k = 23" >/dev/null
q "DELETE FROM orders WHERE k = 22" >/dev/null
wait_view public.orders_expr
DIFF=$(q "SELECT count(*) FROM ((SELECT id, doubled, s FROM orders_expr EXCEPT SELECT id, amount * 2, upper(status) FROM orders WHERE status <> 'void')
  UNION ALL (SELECT id, amount * 2, upper(status) FROM orders WHERE status <> 'void' EXCEPT SELECT id, doubled, s FROM orders_expr)) d")
assert_eq "projection with immutable expressions equals its query" "0" "$DIFF"

q "SELECT nabla.create_view('public.paid_parity', 'SELECT k % 2 AS parity, sum(amount * 2) AS total FROM orders WHERE status = ''paid'' GROUP BY k % 2')" >/dev/null \
  || die "create_view(paid_parity) failed"
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
q "SELECT nabla.create_view('public.good', 'SELECT k, count(*) AS n FROM t GROUP BY k')" >/dev/null || die "create_view(good) failed"
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
ERR=$(q_err "SELECT count(*) FROM nabla.changes('public.bad', 0)")
assert_contains "changes() on a stale view names the reason" 'nabla: view "public.bad" is stale: apply failed 3 times: division by zero' "$ERR"
assert_contains "changes() on a stale view carries the refresh hint" "Run nabla.refresh('public.bad') after fixing the cause" "$ERR"
ERR=$(q_err "SELECT nabla.wait_for('public.bad', pg_current_wal_lsn(), 100)")
assert_contains "wait_for() on a stale view raises the stale error" 'nabla: view "public.bad" is stale: apply failed 3 times' "$ERR"
assert_eq "frontier('bad') still returns the last absorbed LSN" "t" \
  "$(q "SELECT nabla.frontier('public.bad') >= '$F0'::pg_lsn AND nabla.frontier('public.bad') < '$L0'::pg_lsn")"
ERR=$(q_err "SELECT nabla.refresh('public.bad')")
assert_contains "refresh() fails with PostgreSQL's error while the bad row exists" "division by zero" "$ERR"
assert_eq "bad stays stale after a failed refresh" "stale" "$(q "SELECT status FROM nabla.views WHERE name = 'public.bad'")"
q "DELETE FROM t WHERE k = 0" >/dev/null
q "SELECT nabla.refresh('public.bad')" >/dev/null || die "refresh(bad) failed after removing the bad row"
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
lines_diff() { q "SELECT count(*) FROM ((SELECT order_id, customer, product, total FROM order_lines EXCEPT $LINES_DEF)
  UNION ALL ($LINES_DEF EXCEPT SELECT order_id, customer, product, total FROM order_lines)) d"; }
shadow_of() { q "SELECT table_name FROM nabla.shadows WHERE relid = '$1'::regclass"; }
shadow_diff() { q "SELECT count(*) FROM ((SELECT * FROM $(shadow_of "$1") EXCEPT SELECT * FROM $1) UNION ALL (SELECT * FROM $1 EXCEPT SELECT * FROM $(shadow_of "$1"))) d"; }
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
  "$(q "SELECT string_agg(op::text || ':' || (row->>'customer'), ',' ORDER BY seq) FROM nabla.changes('public.order_lines', $SEQ0) WHERE (row->>'order_id')::bigint = $NEW_ORDER")"
assert_eq "T1 and T2 deltas carry distinct increasing LSNs" "2|true" \
  "$(q "SELECT count(DISTINCT lsn) || '|' || (min(lsn) < max(lsn)) FROM nabla.changes('public.order_lines', $SEQ0)")"
assert_eq "T2 rewrote every order of the customer" "3" \
  "$(q "SELECT count(*) FROM nabla.changes('public.order_lines', $SEQ0) WHERE op = 'I' AND row->>'customer' = 'Alicia'")"
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

# --- summary -----------------------------------------------------------------
echo "== server log (warnings and errors)"
grep -E 'WARNING|ERROR|FATAL|PANIC' "$LOG" | tail -n 20 || true
if [ "$FAILED" -ne 0 ]; then
  echo "RESULT: FAIL"
  exit 1
fi
echo "RESULT: PASS"
