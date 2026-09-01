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
ERR=$(q_err "SELECT nabla.create_view('public.bad1', 'SELECT o.id FROM orders o JOIN customers c ON c.id = o.k')")
assert_contains "JOIN is rejected" "nabla: unsupported view definition" "$ERR"
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

# --- summary -----------------------------------------------------------------
echo "== server log (warnings and errors)"
grep -E 'WARNING|ERROR|FATAL|PANIC' "$LOG" | tail -n 20 || true
if [ "$FAILED" -ne 0 ]; then
  echo "RESULT: FAIL"
  exit 1
fi
echo "RESULT: PASS"
