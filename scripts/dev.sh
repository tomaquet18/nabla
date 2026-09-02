#!/bin/sh
# Development helper: every build and test runs inside the nabla-dev:17 image.
#
# Usage:
#   scripts/dev.sh build    compile the extension (cargo build, debug profile)
#   scripts/dev.sh test     install the extension and run tests/integration.sh
#   scripts/dev.sh unit     run the Rust unit tests (cargo test --lib)
#   scripts/dev.sh client   build the reference client (clients/rust/nabla-client, release)
#   scripts/dev.sh shell    interactive shell inside the container
#   scripts/dev.sh run CMD  run an arbitrary command inside the container
#
# target/ and the cargo registry live in named volumes: a bind-mounted target
# directory on Windows is unusably slow.
set -eu

# Git Bash on Windows needs the Windows-style path for docker -v (pwd -W);
# elsewhere plain pwd is right.
REPO_DIR=${NABLA_REPO_DIR:-"$(cd "$(dirname "$0")/.." && (pwd -W 2>/dev/null || pwd))"}
IMAGE=${NABLA_IMAGE:-nabla-dev:17}
PG_CONFIG=/usr/lib/postgresql/17/bin/pg_config

run_in_container() {
  MSYS_NO_PATHCONV=1 docker run --rm "$@"
}

common_args() {
  printf '%s\n' \
    -v "$REPO_DIR:/work" \
    -v nabla-target:/work/target \
    -v nabla-cargo-registry:/usr/local/cargo/registry \
    -v nabla-client-target:/work/clients/rust/nabla-client/target \
    -w /work
}

cmd=${1:-build}
shift || true

case "$cmd" in
  build)
    run_in_container $(common_args) "$IMAGE" bash -c \
      "sudo chown -R dev:dev /work/target /usr/local/cargo/registry && cargo build"
    ;;
  test)
    run_in_container $(common_args) "$IMAGE" bash -c \
      "sudo chown -R dev:dev /work/target /usr/local/cargo/registry /work/clients/rust/nabla-client/target && bash tests/integration.sh"
    ;;
  unit)
    # Plain Rust unit tests. cargo pgrx test (the #[pg_test] harness) does not
    # run reliably in this container; SQL-level coverage lives in tests/integration.sh.
    run_in_container $(common_args) "$IMAGE" bash -c       "sudo chown -R dev:dev /work/target /usr/local/cargo/registry && cargo test --lib"
    ;;
  client)
    run_in_container $(common_args) "$IMAGE" bash -c \
      "sudo chown -R dev:dev /work/clients/rust/nabla-client/target /usr/local/cargo/registry && cd clients/rust/nabla-client && cargo build --release --example follow"
    ;;
  shell)
    run_in_container -it $(common_args) "$IMAGE" bash
    ;;
  run)
    run_in_container $(common_args) "$IMAGE" bash -c "$*"
    ;;
  *)
    echo "usage: $0 {build|test|unit|client|shell|run CMD}" >&2
    exit 2
    ;;
esac
