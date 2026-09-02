#!/usr/bin/env bash
# Worker throughput benchmark. Runs INSIDE the nabla-dev:17 container
# (scripts/dev.sh bench): installs the extension, starts a throwaway cluster
# on port 5498, loads 200k orders / 1000 customers / 500 products, creates an
# aggregate join view and a projection join view over the three tables, then
#
#   1. runs pgbench single-row INSERT transactions at 1, 4 and 16 clients for
#      5 s each against a control table (no views) and against orders, and
#      prints the retained writer throughput;
#   2. measures the worker's drain rate (source transactions per second, from
#      current_seq of the projection view sampled over 10 s) while draining a
#      backlog;
#   3. measures the time to drain a 20k single-row backlog (capped).
#
# Nothing here asserts; the numbers are printed for the report.
set -u

PG_BIN=/usr/lib/postgresql/17/bin
PGDATA=/tmp/nabla-bench-pg
PORT=5498
DB=bench
LOG=/tmp/nabla-bench-pg.log
export PGHOST=/tmp
BACKLOG=${BACKLOG:-20000}
DRAIN_CAP_S=${DRAIN_CAP_S:-120}

q() { psql -X -q -A -t -v ON_ERROR_STOP=1 -p "$PORT" -d "$DB" -c "$1"; }
die() { printf 'FATAL %s\n' "$1"; exit 1; }
cleanup() {
  [ -f "$PGDATA/postmaster.pid" ] && "$PG_BIN/pg_ctl" -D "$PGDATA" -m immediate stop >/dev/null 2>&1
  cp "$LOG" /work/target/nabla-bench-pg.log 2>/dev/null || true
}
trap cleanup EXIT

echo "== installing extension"
cargo pgrx install --sudo --pg-config "$PG_BIN/pg_config" >/tmp/nabla-bench-install.log 2>&1 \
  || die "cargo pgrx install failed: $(tail -n 30 /tmp/nabla-bench-install.log)"

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
nabla.poll_interval_ms = 50
max_replication_slots = 4
max_wal_senders = 4
shared_buffers = 512MB
log_min_messages = warning
EOF
"$PG_BIN/pg_ctl" -D "$PGDATA" -l "$LOG" -w start >/dev/null || die "pg_ctl start failed: $(tail -n 30 "$LOG")"
createdb -p "$PORT" "$DB" || die "createdb failed"
q "CREATE EXTENSION nabla" >/dev/null || die "CREATE EXTENSION failed"
"$PG_BIN/pg_ctl" -D "$PGDATA" -l "$LOG" -w restart >/dev/null || die "pg_ctl restart failed"
sleep 1

echo "== loading dataset"
q "CREATE TABLE customers (id int PRIMARY KEY, name text, region text)" >/dev/null
q "CREATE TABLE products (id int PRIMARY KEY, name text, price numeric)" >/dev/null
q "CREATE TABLE orders (id bigserial PRIMARY KEY, customer_id int, product_id int, qty int, status text)" >/dev/null
q "CREATE TABLE control_orders (id bigserial PRIMARY KEY, customer_id int, product_id int, qty int, status text)" >/dev/null
q "INSERT INTO customers SELECT i, 'customer ' || i, 'region ' || (i % 20) FROM generate_series(1, 1000) i" >/dev/null
q "INSERT INTO products SELECT i, 'product ' || i, (i % 100) + 1 FROM generate_series(1, 500) i" >/dev/null
q "INSERT INTO orders (customer_id, product_id, qty, status) SELECT 1 + (random() * 999)::int, 1 + (random() * 499)::int, 1 + (random() * 4)::int, CASE WHEN random() < 0.8 THEN 'paid' ELSE 'new' END FROM generate_series(1, 200000)" >/dev/null
q "INSERT INTO control_orders SELECT * FROM orders" >/dev/null
q "SELECT setval('control_orders_id_seq', (SELECT max(id) FROM orders))" >/dev/null
q "ANALYZE" >/dev/null
q "SELECT nabla.create_view('revenue_by_region', 'SELECT c.region, count(*) AS n, sum(o.qty * p.price) AS revenue FROM orders o JOIN customers c ON c.id = o.customer_id JOIN products p ON p.id = o.product_id WHERE o.status = ''paid'' GROUP BY c.region')" >/dev/null || die "create_view(revenue_by_region) failed"
q "SELECT nabla.create_view('paid_orders', 'SELECT o.id AS order_id, c.name AS customer, p.name AS product, o.qty * p.price AS total FROM orders o JOIN customers c ON c.id = o.customer_id JOIN products p ON p.id = o.product_id WHERE o.status = ''paid''')" >/dev/null || die "create_view(paid_orders) failed"
[ "$(q "SELECT nabla.await_ready('revenue_by_region', 300000)")" = t ] || die "revenue_by_region not ready"
[ "$(q "SELECT nabla.await_ready('paid_orders', 300000)")" = t ] || die "paid_orders not ready"
echo "views ready: $(q "SELECT count(*) FROM paid_orders") paid orders, $(q "SELECT count(*) FROM revenue_by_region") regions"

cat > /tmp/bench-orders.sql <<'EOF'
\set c random(1, 1000)
\set p random(1, 500)
\set qn random(1, 5)
INSERT INTO orders (customer_id, product_id, qty, status) VALUES (:c, :p, :qn, 'paid');
EOF
sed 's/INTO orders/INTO control_orders/' /tmp/bench-orders.sql > /tmp/bench-control.sql

tps_of() { grep -oE 'tps = [0-9.]+' "$1" | head -n 1 | grep -oE '[0-9.]+'; }
# Wait until paid_orders' frontier reaches an LSN, polling with short
# statements. A single long nabla.wait_for() call would hold its transaction
# snapshot for the whole wait, which keeps the storage rows' dead versions
# from being pruned and makes every aggregate upsert walk a longer chain:
# the benchmark would then measure the cost of its own observer.
poll_frontier() { # target_lsn timeout_s -> prints ms taken or "timeout"
  local start deadline
  start=$(date +%s%N)
  deadline=$(( start + $2 * 1000000000 ))
  while [ "$(date +%s%N)" -lt "$deadline" ]; do
    if [ "$(q "SELECT nabla.frontier('paid_orders') >= '$1'::pg_lsn")" = t ]; then
      echo "$(( ($(date +%s%N) - start) / 1000000 ))"; return
    fi
    sleep 0.1
  done
  echo timeout
}
wait_drained() { # timeout_s -> prints ms taken or "timeout"
  poll_frontier "$(q "SELECT pg_current_wal_lsn()")" "$1"
}

echo "== writer throughput (5 s per run)"
printf '%-8s %-12s %-12s %s\n' clients control_tps nabla_tps retained
for c in 1 4 16; do
  "$PG_BIN/pgbench" -n -p "$PORT" -c "$c" -j "$c" -T 5 -f /tmp/bench-control.sql "$DB" > /tmp/bench-control-$c.out 2>&1 || die "pgbench control failed: $(tail -n 12 /tmp/bench-control-$c.out)"
  "$PG_BIN/pgbench" -n -p "$PORT" -c "$c" -j "$c" -T 5 -f /tmp/bench-orders.sql "$DB" > /tmp/bench-orders-$c.out 2>&1 || die "pgbench orders failed: $(tail -n 12 /tmp/bench-orders-$c.out)"
  CT=$(tps_of /tmp/bench-control-$c.out); NT=$(tps_of /tmp/bench-orders-$c.out)
  printf '%-8s %-12s %-12s %s%%\n' "$c" "$CT" "$NT" "$(q "SELECT round(100.0 * $NT / $CT)")"
done

echo "== worker drain rate (source transactions per second over 10 s, backlog from the runs above)"
S0=$(q "SELECT current_seq FROM nabla.status('paid_orders')")
sleep 10
S1=$(q "SELECT current_seq FROM nabla.status('paid_orders')")
echo "drain rate: $(( (S1 - S0) / 10 )) tx/s (current_seq $S0 -> $S1)"
echo "waiting for the pgbench backlog to drain (cap ${DRAIN_CAP_S}s): $(wait_drained "$DRAIN_CAP_S") ms"

echo "== time to drain a $BACKLOG single-row backlog"
q "ALTER SYSTEM SET nabla.poll_interval_ms = 60000" >/dev/null; q "SELECT pg_reload_conf()" >/dev/null
sleep 0.3
"$PG_BIN/pgbench" -n -p "$PORT" -c 8 -j 8 -t $(( BACKLOG / 8 )) -f /tmp/bench-orders.sql "$DB" > /tmp/bench-backlog.out 2>&1 || die "pgbench backlog failed"
PENDING_TARGET=$(q "SELECT pg_current_wal_lsn()")
q "ALTER SYSTEM RESET nabla.poll_interval_ms" >/dev/null; q "SELECT pg_reload_conf()" >/dev/null
S0=$(q "SELECT current_seq FROM nabla.status('paid_orders')")
MS=$(poll_frontier "$PENDING_TARGET" "$DRAIN_CAP_S")
if [ "$MS" != timeout ]; then
  S1=$(q "SELECT current_seq FROM nabla.status('paid_orders')")
  echo "drained $BACKLOG transactions in ${MS} ms ($(( BACKLOG * 1000 / (MS > 0 ? MS : 1) )) tx/s; deltas $S0 -> $S1)"
else
  S1=$(q "SELECT current_seq FROM nabla.status('paid_orders')")
  echo "NOT drained within ${DRAIN_CAP_S} s: $(( S1 - S0 )) of $BACKLOG applied ($(( (S1 - S0) / DRAIN_CAP_S )) tx/s)"
fi
echo "views consistent: $(q "SELECT count(*) = 0 FROM ((SELECT region, n, revenue FROM revenue_by_region EXCEPT SELECT c.region, count(*), sum(o.qty * p.price) FROM orders o JOIN customers c ON c.id = o.customer_id JOIN products p ON p.id = o.product_id WHERE o.status = 'paid' GROUP BY c.region) UNION ALL (SELECT c.region, count(*), sum(o.qty * p.price) FROM orders o JOIN customers c ON c.id = o.customer_id JOIN products p ON p.id = o.product_id WHERE o.status = 'paid' GROUP BY c.region EXCEPT SELECT region, n, revenue FROM revenue_by_region)) d")"
echo "== done"
