#!/bin/sh
# Development helper: every build and test runs inside the nabla-dev:17 image.
#
# Usage:
#   scripts/dev.sh build    compile the extension (cargo build, debug profile)
#   scripts/dev.sh test     install the extension and run tests/integration.sh
#   scripts/dev.sh unit     run the Rust unit tests (cargo test --lib)
#   scripts/dev.sh fmt      rustfmt --check on both crates (the CI gate)
#   scripts/dev.sh clippy   clippy with -D warnings on both crates (the CI gate)
#   scripts/dev.sh licenses SPDX header and manifest check (scripts/check-licenses.sh)
#   scripts/dev.sh bench    worker throughput benchmark (scripts/bench.sh)
#   scripts/dev.sh h2h      head-to-head benchmark against pg_ivm (bench/head-to-head)
#   scripts/dev.sh client   build the reference client (clients/rust/nabla-client, release)
#   scripts/dev.sh shell    interactive shell inside the container
#   scripts/dev.sh run CMD  run an arbitrary command inside the container
#   scripts/dev.sh playground  start a detached cluster with nabla for hands-on use
#
# target/ and the cargo registry live in named volumes: a bind-mounted target
# directory on Windows is unusably slow.
# shellcheck disable=SC2046  # $(common_args) is split on purpose: one docker argument per line
set -eu

# Git Bash on Windows needs the Windows-style path for docker -v (pwd -W);
# elsewhere plain pwd is right.
REPO_DIR=${NABLA_REPO_DIR:-"$(cd "$(dirname "$0")/.." && (pwd -W 2>/dev/null || pwd))"}
IMAGE=${NABLA_IMAGE:-nabla-dev:17}

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
    run_in_container $(common_args) -e NABLA_TEST_TIME_SCALE "$IMAGE" bash -c \
      "sudo chown -R dev:dev /work/target /usr/local/cargo/registry /work/clients/rust/nabla-client/target && bash tests/integration.sh"
    ;;
  bench)
    run_in_container $(common_args) -e BACKLOG -e DRAIN_CAP_S "$IMAGE" bash -c "sudo chown -R dev:dev /work/target /usr/local/cargo/registry && bash scripts/bench.sh"
    ;;
  fmt)
    run_in_container $(common_args) "$IMAGE" bash -c "rustup component list --installed | grep -q rustfmt || rustup component add rustfmt; cargo fmt --check && (cd clients/rust/nabla-client && cargo fmt --check)"
    ;;
  clippy)
    run_in_container $(common_args) "$IMAGE" bash -c "sudo chown -R dev:dev /work/target /usr/local/cargo/registry /work/clients/rust/nabla-client/target && (rustup component list --installed | grep -q clippy || rustup component add clippy) && cargo clippy --all-targets -- -D warnings && (cd clients/rust/nabla-client && cargo clippy -- -D warnings)"
    ;;
  licenses)
    bash "$(dirname "$0")/check-licenses.sh"
    ;;
  h2h)
    # Head-to-head benchmark against pg_ivm. Its image is the dev image plus
    # pg_ivm, built from bench/head-to-head/Dockerfile.
    MSYS_NO_PATHCONV=1 docker build -t nabla-h2h:17 "$REPO_DIR/bench/head-to-head" >/dev/null       || { echo "docker build of nabla-h2h:17 failed" >&2; exit 1; }
    HOST_INFO=$(docker info --format '{{.ServerVersion}} on {{.OperatingSystem}}, {{.NCPU}} CPUs, {{.MemTotal}} bytes RAM' 2>/dev/null || echo unknown)
    NABLA_COMMIT=$(git -C "$(dirname "$0")/.." rev-parse --short HEAD 2>/dev/null || echo unknown)
    run_in_container $(common_args)       -e "H2H_HOST_INFO=$HOST_INFO" -e "H2H_NABLA_COMMIT=$NABLA_COMMIT"       -e DURATION -e REPS -e CLIENTS -e ORDERS -e ARMS -e WORKLOADS       nabla-h2h:17 bash -c       "sudo chown -R dev:dev /work/target /usr/local/cargo/registry && bash bench/head-to-head/run.sh"
    ;;
  unit)
    # Plain Rust unit tests. cargo pgrx test (the #[pg_test] harness) does not
    # run reliably in this container; SQL-level coverage lives in tests/integration.sh.
    run_in_container $(common_args) "$IMAGE" bash -c "sudo chown -R dev:dev /work/target /usr/local/cargo/registry && cargo test --lib"
    ;;
  client)
    run_in_container $(common_args) "$IMAGE" bash -c \
      "sudo chown -R dev:dev /work/clients/rust/nabla-client/target /usr/local/cargo/registry && cd clients/rust/nabla-client && cargo build --release --example follow"
    ;;
  shell)
    run_in_container -it $(common_args) "$IMAGE" bash
    ;;
  playground)
    # Detached cluster for hands-on use; see scripts/playground.sh for usage.
    docker rm -f nabla-play >/dev/null 2>&1 || true
    MSYS_NO_PATHCONV=1 docker run -d --name nabla-play $(common_args) "$IMAGE" \
      bash scripts/playground.sh >/dev/null
    echo "starting nabla-play; follow with: docker logs -f nabla-play"
    ;;
  run)
    run_in_container $(common_args) "$IMAGE" bash -c "$*"
    ;;
  *)
    echo "usage: $0 {build|test|unit|fmt|clippy|licenses|bench|h2h|client|shell|run CMD}" >&2
    exit 2
    ;;
esac
