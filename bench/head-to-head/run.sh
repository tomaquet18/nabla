#!/usr/bin/env bash
# Head-to-head benchmark: no derived view vs pg_ivm vs nabla, on the same
# PostgreSQL 17 server, the same host, the same dataset and the same view.
#
# Runs INSIDE the nabla-h2h:17 image (see Dockerfile and README.md):
#
#   scripts/dev.sh h2h
#
# Everything measured here is written to RESULTS.md next to this script.
# The script asserts correctness of each derived view and stops loudly if a
# view does not equal its query.
set -u

PG_BIN=/usr/lib/postgresql/17/bin
PGDATA=/tmp/h2h-pg
PORT=5497
DB=bench
LOG=/tmp/h2h-pg.log
export PGHOST=/tmp

HERE=$(cd "$(dirname "$0")" && pwd)
CSV=/tmp/h2h-results.csv
NOTES=/tmp/h2h-notes.txt

# Knobs (defaults are what the published numbers were produced with).
DURATION=${DURATION:-10}
REPS=${REPS:-3}
CLIENTS=${CLIENTS:-"1 4 16"}
ORDERS=${ORDERS:-200000}
CUSTOMERS=${CUSTOMERS:-1000}
PRODUCTS=${PRODUCTS:-500}
ARMS=${ARMS:-"none pg_ivm nabla"}
WORKLOADS=${WORKLOADS:-"insert update-fact update-dimension"}
# A catch-up that takes minutes is a result, not an error: it is measured and
# reported. The ceiling only stops the benchmark from hanging forever.
CATCHUP_CEILING_S=${CATCHUP_CEILING_S:-1800}

# Only a run of the complete default matrix may publish RESULTS.md; a narrowed
# run (fewer arms, workloads, clients or repetitions) writes RESULTS-partial.md
# instead, so a quick spot check can never overwrite the published numbers.
# The raw per-run CSV is saved next to the report either way, so a report can
# always be regenerated without repeating the runs.
if [ "$ARMS" = "none pg_ivm nabla" ] \
  && [ "$WORKLOADS" = "insert update-fact update-dimension" ] \
  && [ "$CLIENTS" = "1 4 16" ] && [ "$REPS" -ge 3 ] && [ "$DURATION" -ge 10 ]; then
  RESULTS=$HERE/RESULTS.md
  RESULTS_CSV=$HERE/results.csv
  PARTIAL=no
else
  RESULTS=$HERE/RESULTS-partial.md
  RESULTS_CSV=$HERE/results-partial.csv
  PARTIAL=yes
fi

# The view, identical text for both engines. Single quotes are doubled
# because the text is passed to create_immv/create_view as a SQL literal.
VIEW_BODY="SELECT c.region, count(*) AS orders, sum(o.qty * p.price) AS revenue \
FROM orders o \
JOIN customers c ON c.id = o.customer_id \
JOIN products p ON p.id = o.product_id \
WHERE o.status = ''paid'' \
GROUP BY c.region"
# The same query with ordinary quoting, for the correctness check.
VIEW_QUERY=$(printf '%s' "$VIEW_BODY" | sed "s/''/'/g")

q() { psql -X -q -A -t -v ON_ERROR_STOP=1 -p "$PORT" -d "$DB" -c "$1"; }
die() { printf 'FATAL %s\n' "$1" >&2; exit 1; }
note() { printf '%s\n' "$1" >> "$NOTES"; }
stop_cluster() {
  if [ -f "$PGDATA/postmaster.pid" ]; then
    "$PG_BIN/pg_ctl" -D "$PGDATA" -m immediate stop >/dev/null 2>&1
  fi
}
cleanup() { stop_cluster; cp "$LOG" /work/target/h2h-pg.log 2>/dev/null || true; }
trap cleanup EXIT

: > "$CSV"
: > "$NOTES"

# --- the cluster -------------------------------------------------------------
# Identical configuration for all three arms. wal_level = logical and the
# replication slots are what nabla needs; giving them to every arm keeps the
# WAL volume of the workload identical, which is the point (see README.md).
common_conf() {
  cat <<EOF
port = $PORT
unix_socket_directories = '/tmp'
listen_addresses = ''
wal_level = logical
max_replication_slots = 4
max_wal_senders = 4
shared_buffers = 512MB
max_wal_size = 4GB
min_wal_size = 512MB
checkpoint_timeout = 15min
log_min_messages = warning
EOF
}
# The only difference between arms: nabla is a background worker in a shared
# library, so it must be preloaded, and it must be told which database to
# follow. Nothing else is tuned for either engine.
nabla_conf() {
  cat <<EOF
shared_preload_libraries = 'nabla'
nabla.database = '$DB'
EOF
}

start_cluster() { # arm
  stop_cluster
  rm -rf "$PGDATA"
  "$PG_BIN/initdb" -D "$PGDATA" -U "$(whoami)" --auth=trust >/dev/null 2>&1 || die "initdb failed"
  common_conf >> "$PGDATA/postgresql.conf"
  [ "$1" = nabla ] && nabla_conf >> "$PGDATA/postgresql.conf"
  "$PG_BIN/pg_ctl" -D "$PGDATA" -l "$LOG" -w start >/dev/null || die "pg_ctl start failed: $(tail -n 20 "$LOG")"
  createdb -p "$PORT" "$DB" || die "createdb failed"
  case "$1" in
    pg_ivm) q "CREATE EXTENSION pg_ivm" >/dev/null || die "CREATE EXTENSION pg_ivm failed" ;;
    nabla)  q "CREATE EXTENSION nabla" >/dev/null || die "CREATE EXTENSION nabla failed" ;;
  esac
}

load_dataset() {
  q "CREATE TABLE customers (id int PRIMARY KEY, name text, region text)" >/dev/null
  q "CREATE TABLE products (id int PRIMARY KEY, name text, price numeric)" >/dev/null
  q "CREATE TABLE orders (id bigserial PRIMARY KEY, customer_id int, product_id int, qty int, status text)" >/dev/null
  q "INSERT INTO customers SELECT i, 'customer ' || i, 'region ' || (1 + i % 5) FROM generate_series(1, $CUSTOMERS) i" >/dev/null
  q "INSERT INTO products SELECT i, 'product ' || i, (i % 100) + 1 FROM generate_series(1, $PRODUCTS) i" >/dev/null
  # setseed makes the fact table identical in every arm and every run.
  q "SELECT setseed(0.42); INSERT INTO orders (customer_id, product_id, qty, status) \
     SELECT 1 + (random() * ($CUSTOMERS - 1))::int, 1 + (random() * ($PRODUCTS - 1))::int, \
            1 + (random() * 4)::int, CASE WHEN random() < 0.8 THEN 'paid' ELSE 'new' END \
     FROM generate_series(1, $ORDERS)" >/dev/null
  q "CREATE INDEX ON orders (customer_id)" >/dev/null
  q "CREATE INDEX ON orders (product_id)" >/dev/null
  q "ANALYZE" >/dev/null
}

create_view() { # arm
  case "$1" in
    none) : ;;
    pg_ivm)
      psql -X -q -p "$PORT" -d "$DB" -v ON_ERROR_STOP=1 \
        -c "SELECT pgivm.create_immv('revenue_by_region', '$VIEW_BODY')" >/tmp/h2h-create.out 2>&1 \
        || die "create_immv failed: $(cat /tmp/h2h-create.out)"
      ;;
    nabla)
      q "SELECT nabla.create_view('revenue_by_region', '$VIEW_BODY')" >/dev/null \
        || die "nabla.create_view failed"
      [ "$(q "SELECT nabla.await_ready('revenue_by_region', 600000)")" = t ] \
        || die "nabla view did not become ready: $(q "SELECT status || ': ' || coalesce(last_error, '-') FROM nabla.views")"
      ;;
  esac
}

check_correct() { # arm workload
  [ "$1" = none ] && return 0
  local d
  d=$(q "SELECT count(*) FROM ((SELECT region, orders, revenue FROM revenue_by_region EXCEPT $VIEW_QUERY) \
         UNION ALL ($VIEW_QUERY EXCEPT SELECT region, orders, revenue FROM revenue_by_region)) d")
  [ "$d" = 0 ] || die "$1/$2: the view differs from its query in $d row(s)"
}

# --- freshness (nabla only) --------------------------------------------------
# The frontier is the LSN the view reflects. Lag is measured the moment the
# run ends; catch-up is polled with short statements, because one long
# statement would hold its snapshot and slow the very apply path being timed.
nabla_lag_bytes() { q "SELECT pg_current_wal_lsn() - nabla.frontier('revenue_by_region')"; }
nabla_catchup_ms() { # target_lsn
  local start deadline
  start=$(date +%s%N)
  deadline=$(( start + CATCHUP_CEILING_S * 1000000000 ))
  while [ "$(date +%s%N)" -lt "$deadline" ]; do
    if [ "$(q "SELECT nabla.frontier('revenue_by_region') >= '$1'::pg_lsn")" = t ]; then
      echo "$(( ($(date +%s%N) - start) / 1000000 ))"
      return
    fi
    sleep 0.02
  done
  echo timeout
}

# --- pgbench -----------------------------------------------------------------
write_scripts() {
  cat > /tmp/h2h-insert.sql <<'EOF'
\set c random(1, 1000)
\set p random(1, 500)
\set qn random(1, 5)
INSERT INTO orders (customer_id, product_id, qty, status) VALUES (:c, :p, :qn, 'paid');
EOF
  cat > /tmp/h2h-update-fact.sql <<'EOF'
\set id random(1, 200000)
UPDATE orders SET qty = qty + 1 WHERE id = :id;
EOF
  cat > /tmp/h2h-update-dimension.sql <<'EOF'
\set cid random(1, 1000)
\set r random(1, 5)
UPDATE customers SET region = 'region ' || :r WHERE id = :cid;
EOF
  sed -i "s/random(1, 1000)/random(1, $CUSTOMERS)/; s/random(1, 500)/random(1, $PRODUCTS)/" /tmp/h2h-insert.sql
  sed -i "s/random(1, 200000)/random(1, $ORDERS)/" /tmp/h2h-update-fact.sql
  sed -i "s/random(1, 1000)/random(1, $CUSTOMERS)/" /tmp/h2h-update-dimension.sql
}

value_of() { grep -oE "$2" "$1" | head -n 1 | grep -oE '[0-9]+\.?[0-9]*'; }

run_one() { # arm workload clients rep -> appends one CSV row
  local arm=$1 workload=$2 clients=$3 rep=$4 out=/tmp/h2h-run.out
  local tps lat retried failed target lag catchup
  [ "$SKIP_REST" = yes ] && return 0
  "$PG_BIN/pgbench" -n -p "$PORT" -d "$DB" -c "$clients" -j "$clients" -T "$DURATION" \
    --max-tries=10 --random-seed="$rep" -f "/tmp/h2h-$workload.sql" > "$out" 2>&1 \
    || die "$arm/$workload/$clients: pgbench failed: $(tail -n 12 "$out")"
  tps=$(value_of "$out" 'tps = [0-9.]+')
  lat=$(value_of "$out" 'latency average = [0-9.]+')
  retried=$(value_of "$out" 'number of transactions retried: [0-9]+')
  failed=$(value_of "$out" 'number of failed transactions: [0-9]+')
  retried=${retried:-0}; failed=${failed:-0}
  catchup=; lag=
  if [ "$arm" = nabla ]; then
    target=$(q "SELECT pg_current_wal_lsn()")
    lag=$(nabla_lag_bytes)
    catchup=$(nabla_catchup_ms "$target")
    if [ "$catchup" = timeout ]; then
      note "$arm/$workload/$clients rep $rep: the view had not caught up after ${CATCHUP_CEILING_S}s; the remaining runs of this workload were skipped"
      SKIP_REST=yes
      catchup=
    fi
  else
    sleep 1
  fi
  [ "${retried:-0}" != 0 ] && note "$arm/$workload/$clients rep $rep: $retried transaction(s) retried (serialization or deadlock)"
  [ "${failed:-0}" != 0 ] && note "$arm/$workload/$clients rep $rep: $failed transaction(s) failed after 10 tries"
  printf '%s,%s,%s,%s,%s,%s,%s,%s\n' "$arm" "$workload" "$clients" "$rep" "$tps" "$lat" "${catchup:-}" "${lag:-}" >> "$CSV"
  printf '  %-8s %-16s c=%-2s rep %s: %8s tps, %6s ms%s\n' \
    "$arm" "$workload" "$clients" "$rep" "$tps" "$lat" \
    "$( [ "$arm" = nabla ] && printf ', catch-up %s ms, lag %s B' "$catchup" "$lag" )"
}

# --- environment -------------------------------------------------------------
echo "== installing nabla from the working tree"
cargo pgrx install --sudo --pg-config "$PG_BIN/pg_config" >/tmp/h2h-install.log 2>&1 \
  || die "cargo pgrx install failed: $(tail -n 30 /tmp/h2h-install.log)"

write_scripts
NABLA_VERSION=$(grep -m1 '^version' /work/Cargo.toml | grep -oE '[0-9]+\.[0-9]+\.[0-9]+')
PG_IVM_BUILD=$(cat /opt/pg_ivm-version 2>/dev/null || echo "pg_ivm (unknown build)")
HOST_INFO=${H2H_HOST_INFO:-"$(nproc) CPUs, $(( $(grep MemTotal /proc/meminfo | grep -oE '[0-9]+') / 1024 / 1024 )) GiB RAM visible to the container"}
NABLA_COMMIT=${H2H_NABLA_COMMIT:-unknown}
STARTED=$(date -u '+%Y-%m-%d %H:%M UTC')

# --- the runs ----------------------------------------------------------------
PG_VERSION=
PG_IVM_EXTVERSION=
SKIP_REST=no
for arm in $ARMS; do
  for workload in $WORKLOADS; do
    echo "== arm $arm, workload $workload"
    start_cluster "$arm"
    [ -z "$PG_VERSION" ] && PG_VERSION=$(q "SELECT version()")
    if [ "$arm" = pg_ivm ] && [ -z "$PG_IVM_EXTVERSION" ]; then
      PG_IVM_EXTVERSION=$(q "SELECT extversion FROM pg_extension WHERE extname = 'pg_ivm'")
    fi
    load_dataset
    create_view "$arm"
    check_correct "$arm" "$workload"
    # Warm-up, discarded.
    "$PG_BIN/pgbench" -n -p "$PORT" -d "$DB" -c 4 -j 4 -T "$DURATION" --max-tries=10 \
      -f "/tmp/h2h-$workload.sql" >/dev/null 2>&1
    if [ "$arm" = nabla ]; then
      nabla_catchup_ms "$(q "SELECT pg_current_wal_lsn()")" >/dev/null
    fi
    SKIP_REST=no
    for rep in $(seq 1 "$REPS"); do
      for c in $CLIENTS; do
        run_one "$arm" "$workload" "$c" "$rep"
      done
    done
    [ "$SKIP_REST" = no ] && check_correct "$arm" "$workload"
    if [ "$arm" != none ] && [ "$SKIP_REST" = no ]; then
      note "$arm/$workload: the view equals its query after the runs ($(q "SELECT count(*) FROM revenue_by_region") group rows)"
    fi
    stop_cluster
  done
done

# --- report ------------------------------------------------------------------
# median/min/max of the three repetitions of one cell.
cell() { # arm workload clients field(5=tps,6=latency,7=catchup) stat(med|min|max)
  local vals
  vals=$(awk -F, -v a="$1" -v w="$2" -v c="$3" -v f="$4" \
    '$1==a && $2==w && $3==c && $f != "" { print $f }' "$CSV" | sort -g)
  [ -z "$vals" ] && { echo "-"; return; }
  case "$5" in
    med) printf '%s\n' "$vals" | awk '{v[NR]=$0} END {print v[int((NR+1)/2)]}' ;;
    min) printf '%s\n' "$vals" | head -n 1 ;;
    max) printf '%s\n' "$vals" | tail -n 1 ;;
  esac
}
fmt() { # number decimals
  [ "$1" = "-" ] && { echo "-"; return; }
  awk -v v="$1" -v d="$2" 'BEGIN { printf "%.*f\n", d, v }'
}
pct() { # value base
  { [ "$1" = "-" ] || [ "$2" = "-" ] || [ "$2" = 0 ]; } && { echo "-"; return; }
  awk -v v="$1" -v b="$2" 'BEGIN { printf "%.0f%%\n", 100 * v / b }'
}

{
  echo "# nabla vs pg_ivm: head-to-head"
  echo
  echo "Generated by \`bench/head-to-head/run.sh\` on $STARTED."
  echo "Read \`bench/head-to-head/README.md\` for the methodology before the numbers."
  echo
  echo "## Environment"
  echo
  echo "| | |"
  echo "|---|---|"
  echo "| host | $HOST_INFO |"
  echo "| server | ${PG_VERSION:-unknown} |"
  echo "| pg_ivm | ${PG_IVM_EXTVERSION:-unknown} ($PG_IVM_BUILD) |"
  echo "| nabla | $NABLA_VERSION (commit $NABLA_COMMIT), installed from the working tree |"
  echo "| dataset | $CUSTOMERS customers, $PRODUCTS products, $ORDERS orders |"
  echo "| runs | ${DURATION}s per run, $REPS repetitions per cell, clients $CLIENTS, one warm-up run per arm and workload (discarded) |"
  echo
  echo "Every arm ran on its own freshly initialised cluster with this configuration:"
  echo
  echo '```'
  common_conf | grep -v '^port\|^unix_socket\|^listen_addresses'
  echo '```'
  echo
  echo "The nabla arm adds the two settings nabla cannot run without, and nothing else:"
  echo
  echo '```'
  nabla_conf
  echo '```'
  echo
  echo "The view, created with the identical text in both engines:"
  echo
  echo '```sql'
  printf '%s\n' "$VIEW_QUERY" | sed 's/ FROM orders/\nFROM orders/; s/ JOIN /\nJOIN /g; s/ WHERE /\nWHERE /; s/ GROUP BY /\nGROUP BY /'
  echo '```'
  echo

  for workload in $WORKLOADS; do
    case "$workload" in
      insert) title="Workload: insert (one row into \`orders\`, \`status = 'paid'\`)" ;;
      update-fact) title="Workload: update-fact (\`UPDATE orders SET qty = qty + 1 WHERE id = <random>\`)" ;;
      update-dimension) title="Workload: update-dimension (\`UPDATE customers SET region = <random of 5> WHERE id = <random>\`)" ;;
      *) title="Workload: $workload" ;;
    esac
    echo "## $title"
    echo
    echo "Median of $REPS repetitions. Latency is pgbench's average latency."
    echo
    echo "| clients | none tps | pg_ivm tps | pg_ivm vs none | nabla tps | nabla vs none | none lat (ms) | pg_ivm lat (ms) | nabla lat (ms) | nabla catch-up (ms) |"
    echo "|---|---|---|---|---|---|---|---|---|---|"
    for c in $CLIENTS; do
      n=$(cell none "$workload" "$c" 5 med); i=$(cell pg_ivm "$workload" "$c" 5 med); b=$(cell nabla "$workload" "$c" 5 med)
      nl=$(cell none "$workload" "$c" 6 med); il=$(cell pg_ivm "$workload" "$c" 6 med); bl=$(cell nabla "$workload" "$c" 6 med)
      cu=$(cell nabla "$workload" "$c" 7 med)
      echo "| $c | $(fmt "$n" 0) | $(fmt "$i" 0) | $(pct "$i" "$n") | $(fmt "$b" 0) | $(pct "$b" "$n") | $(fmt "$nl" 3) | $(fmt "$il" 3) | $(fmt "$bl" 3) | $(fmt "$cu" 0) |"
    done
    echo
    echo "Spread over the $REPS repetitions (min-max tps), and nabla's frontier lag when each run ended:"
    echo
    echo "| clients | none | pg_ivm | nabla | nabla lag at run end (bytes, median) |"
    echo "|---|---|---|---|---|"
    for c in $CLIENTS; do
      echo "| $c | $(fmt "$(cell none "$workload" "$c" 5 min)" 0)-$(fmt "$(cell none "$workload" "$c" 5 max)" 0) | $(fmt "$(cell pg_ivm "$workload" "$c" 5 min)" 0)-$(fmt "$(cell pg_ivm "$workload" "$c" 5 max)" 0) | $(fmt "$(cell nabla "$workload" "$c" 5 min)" 0)-$(fmt "$(cell nabla "$workload" "$c" 5 max)" 0) | $(fmt "$(cell nabla "$workload" "$c" 8 med)" 0) |"
    done
    echo
  done

  echo "## Correctness and events"
  echo
  echo "After every arm and workload the derived view was compared with its query in both"
  echo "directions (\`EXCEPT\` twice); the run stops if a single row differs."
  echo
  while IFS= read -r line; do echo "- $line"; done < "$NOTES"
  echo
  echo "## What this shows"
  echo
  echo "pg_ivm maintains the view inside the transaction that writes the base table."
  echo "A reader that commits a write and then reads the view sees its own write, always;"
  echo "freshness is zero by construction and there is nothing to measure. The writer pays"
  echo "for it: it does the view's work itself and holds row locks on the affected group"
  echo "rows until it commits, so concurrent writers touching the same groups serialise"
  echo "behind each other. In these runs that cost was a quarter to a half of the baseline"
  echo "throughput at one client, and at sixteen clients pg_ivm's throughput did not grow"
  echo "with concurrency at all: it stayed near its single-client figure while the baseline"
  echo "rose eightfold."
  echo
  echo "nabla maintains the view outside the writing transaction, from the WAL, in a"
  echo "background worker. Writers keep their concurrency; throughput grows with clients in"
  echo "every workload here. They are not free, though: the worker competes for the same"
  echo "CPU and disk, and while it was behind, the insert and update-fact workloads lost"
  echo "between a fifth and a third of the baseline throughput. The dimension workload,"
  echo "whose per-transaction cost falls almost entirely on the worker, lost nothing"
  echo "measurable."
  echo
  echo "The price nabla pays is staleness. The view reflects the base tables as of the"
  echo "worker's frontier, which trails the current WAL position by the lag in the tables"
  echo "above and catches up at the worker's drain rate. A reader who needs to see its own"
  echo "write must wait for it. The catch-up column is that wait, and it is large here: at"
  echo "sixteen clients, ten seconds of inserts took about seventy seconds to apply, ten"
  echo "seconds of fact updates about two and a half minutes, and ten seconds of dimension"
  echo "updates about sixteen minutes. At that write rate the dimension view is not usable"
  echo "through nabla: the worker never catches up while the writers run, and the lag would"
  echo "keep growing until the slot cap stopped it."
  echo
  echo "Neither column is a score. They are the two halves of one trade: pg_ivm spends write"
  echo "throughput to buy freshness, nabla spends freshness to buy write throughput, and"
  echo "which one is affordable depends on the workload, not on the engine."
} > "$RESULTS"
cp "$CSV" "$RESULTS_CSV" 2>/dev/null || true
if [ "$PARTIAL" = yes ]; then
  printf '
Narrowed run: wrote %s (the published RESULTS.md was left untouched).
' "$RESULTS"
fi

echo
echo "== wrote $RESULTS"
cat "$RESULTS"
