#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

RUN_ID="${RUN_ID:-$(date +%Y%m%d-%H%M%S)}"
RESULTS_DIR="${RESULTS_DIR:-$ROOT_DIR/results/$RUN_ID}"
DATA_DIR="${DATA_DIR:-$ROOT_DIR/data-$RUN_ID}"
CHANGESET_DIR="${CHANGESET_DIR:-$ROOT_DIR/changesets-sample}"
TARGET_VERSION="${TARGET_VERSION:-0}"
DISABLE_BG="${DISABLE_BG:-0}"

if [[ ! -d "$CHANGESET_DIR" ]]; then
  echo "changeset dir not found: $CHANGESET_DIR" >&2
  echo "tip: set CHANGESET_DIR=... or generate sample changesets with:" >&2
  echo "  (cd bench && go run ./cmd/gen-changesets/main.go --profile sample --versions 20 --scale 1 ../changesets-sample)" >&2
  exit 1
fi

mkdir -p "$RESULTS_DIR" "$DATA_DIR"

target_args=()
if [[ "$TARGET_VERSION" != "0" ]]; then
  target_args=(--target-version "$TARGET_VERSION")
fi

echo "Building benches..."
(cd iavl-v1 && go build -o iavl-v1-bench .)
(cd iavl-v1-memdb && go build -o iavl-v1-memdb-bench .)
(cd treedb-v1 && go build -o treedb-v1-bench .)

run_iavl_v1_leveldb() {
  local db_dir="$DATA_DIR/iavl-v1-leveldb"
  local log_file="$RESULTS_DIR/iavl-v1-leveldb.jsonl"
  echo
  echo "=== iavl/v1 (leveldb) ==="
  mkdir -p "$db_dir"
  ./iavl-v1/iavl-v1-bench bench \
    --db-dir "$db_dir" \
    --changeset-dir "$CHANGESET_DIR" \
    --log-file "$log_file" \
    --log-type json \
    ${target_args[@]+"${target_args[@]}"}
}

run_iavl_v1_memdb() {
  local db_dir="$DATA_DIR/iavl-v1-memdb"
  local log_file="$RESULTS_DIR/iavl-v1-memdb.jsonl"
  echo
  echo "=== iavl/v1 (memdb) ==="
  mkdir -p "$db_dir"
  ./iavl-v1-memdb/iavl-v1-memdb-bench bench \
    --db-dir "$db_dir" \
    --changeset-dir "$CHANGESET_DIR" \
    --log-file "$log_file" \
    --log-type json \
    ${target_args[@]+"${target_args[@]}"}
}

run_treedb_v1() {
  local name="$1"
  shift
  local db_dir="$DATA_DIR/treedb-v1-$name"
  local log_file="$RESULTS_DIR/treedb-v1-$name.jsonl"
  mkdir -p "$db_dir"

  # Optional experiment toggles (apply to both mode3/mode4 unless you override).
  local -a extra_env
  extra_env=()
  if [[ -n "${TREEDB_SLAB_COMPRESSION:-}" ]]; then
    extra_env+=("TREEDB_SLAB_COMPRESSION=${TREEDB_SLAB_COMPRESSION}")
  fi
  if [[ -n "${TREEDB_LEAF_PREFIX_COMPRESSION:-}" ]]; then
    extra_env+=("TREEDB_LEAF_PREFIX_COMPRESSION=${TREEDB_LEAF_PREFIX_COMPRESSION}")
  fi
  if [[ -n "${TREEDB_BENCH_VLOG_DICT_TRAIN_BYTES:-}" ]]; then
    extra_env+=("TREEDB_BENCH_VLOG_DICT_TRAIN_BYTES=${TREEDB_BENCH_VLOG_DICT_TRAIN_BYTES}")
  fi

  echo
  echo "=== treedb/v1 (${name}) ==="
  env \
    TREEDB_BENCH_MODE="${TREEDB_BENCH_MODE:-cached}" \
    TREEDB_BENCH_DISABLE_BG="$DISABLE_BG" \
    ${extra_env[@]+"${extra_env[@]}"} \
    "$@" \
    ./treedb-v1/treedb-v1-bench bench \
      --db-dir "$db_dir" \
      --changeset-dir "$CHANGESET_DIR" \
      --log-file "$log_file" \
      --log-type json \
      ${target_args[@]+"${target_args[@]}"}
}

# Baselines:
# - mode4: WAL off throughput (unsafe)
# - mode3: WAL on, strict sync + checksums (durable-ish)
run_treedb_v1 mode4-wal-off \
  TREEDB_BENCH_PROFILE=fast \
  TREEDB_BENCH_DISABLE_WAL=1 \
  TREEDB_BENCH_IAVL_SYNC=0 \
  TREEDB_BENCH_RELAXED_SYNC=1 \
  TREEDB_BENCH_DISABLE_READ_CHECKSUM=1 \
  TREEDB_BENCH_ALLOW_UNSAFE=1

run_treedb_v1 mode3-wal-on \
  TREEDB_BENCH_PROFILE=durable \
  TREEDB_BENCH_DISABLE_WAL=0 \
  TREEDB_BENCH_IAVL_SYNC=1 \
  TREEDB_BENCH_RELAXED_SYNC=0 \
  TREEDB_BENCH_DISABLE_READ_CHECKSUM=0 \
  TREEDB_BENCH_ALLOW_UNSAFE=0

run_iavl_v1_leveldb
run_iavl_v1_memdb

echo
echo "Done."
echo "results: $RESULTS_DIR"
echo "data:    $DATA_DIR"
