#!/bin/bash
# Start a throwaway PostgreSQL cluster with nabla installed, for hands-on use.
#
# Runs INSIDE the nabla-dev:17 container (see scripts/dev.sh). It installs the
# extension from the current sources, creates a cluster on port 5499 with a
# database named "play", and then keeps the container alive so psql and the
# reference client can be used against it:
#
#   scripts/dev.sh playground             # start (detached container "nabla-play")
#   docker exec -it nabla-play psql -p 5499 -d play
#   docker exec -it nabla-play /work/clients/rust/nabla-client/target/release/examples/follow \
#       "host=/tmp port=5499 dbname=play user=dev" public.my_view
#   docker rm -f nabla-play               # stop
set -eu

PG=/usr/lib/postgresql/17/bin
PGDATA=/tmp/pg
PORT=5499
DB=play
export PGHOST=/tmp

sudo chown -R dev:dev /work/target /usr/local/cargo/registry \
  /work/clients/rust/nabla-client/target

echo "== installing extension"
cargo pgrx install --sudo --pg-config "$PG/pg_config" > /tmp/install.log 2>&1 \
  || { tail -n 30 /tmp/install.log; exit 1; }

echo "== building reference client"
( cd /work/clients/rust/nabla-client && cargo build --release --example follow > /tmp/client.log 2>&1 ) \
  || { tail -n 30 /tmp/client.log; exit 1; }

echo "== starting cluster on port $PORT"
rm -rf "$PGDATA"
"$PG/initdb" -D "$PGDATA" -U dev --auth=trust > /dev/null
cat >> "$PGDATA/postgresql.conf" <<EOF
port = $PORT
unix_socket_directories = '/tmp'
listen_addresses = ''
wal_level = logical
shared_preload_libraries = 'nabla'
nabla.database = '$DB'
max_replication_slots = 4
max_wal_senders = 4
log_min_messages = warning
EOF
"$PG/pg_ctl" -D "$PGDATA" -l /tmp/pg.log -w start > /dev/null
createdb -p "$PORT" "$DB"
psql -p "$PORT" -d "$DB" -qc "CREATE EXTENSION nabla"
# The worker's first attempt ran before the database existed; restart so it
# connects immediately instead of after its retry delay.
"$PG/pg_ctl" -D "$PGDATA" -l /tmp/pg.log -w restart > /dev/null

echo READY > /tmp/ready
echo "== ready: psql -p $PORT -d $DB"
exec sleep infinity
