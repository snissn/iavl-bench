#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

RUN_ID="${RUN_ID:-$(date +%Y%m%d-%H%M%S)}"
RESULTS_DIR="${RESULTS_DIR:-$ROOT_DIR/results/$RUN_ID}"
DATA_DIR="${DATA_DIR:-$ROOT_DIR/data-$RUN_ID}"
DATASET="${DATASET:-sample}" # sample (checked-in), dev (generated), or custom (via CHANGESET_DIR)
CHANGESET_DIR="${CHANGESET_DIR:-}"
TARGET_VERSION="${TARGET_VERSION:-0}"
DISABLE_BG="${DISABLE_BG:-0}"
DASHBOARD="${DASHBOARD:-0}"
INCLUDE_MODE3="${INCLUDE_MODE3:-}"

GEN_PROFILE="${GEN_PROFILE:-sample}"
GEN_VERSIONS="${GEN_VERSIONS:-200}"
GEN_SCALE="${GEN_SCALE:-10}"
REGEN="${REGEN:-0}"

if [[ -z "$CHANGESET_DIR" ]]; then
  case "$DATASET" in
  sample)
    CHANGESET_DIR="$ROOT_DIR/changesets-sample"
    ;;
  dev)
    CHANGESET_DIR="$ROOT_DIR/changesets-dev"
    ;;
  *)
    echo "unknown DATASET=$DATASET (use DATASET=dev|sample or set CHANGESET_DIR=...)" >&2
    exit 1
    ;;
  esac
fi

if [[ -z "$INCLUDE_MODE3" ]]; then
  if [[ "$DATASET" == "sample" ]]; then
    INCLUDE_MODE3=1
  else
    INCLUDE_MODE3=0
  fi
fi

maybe_generate_changesets() {
  if [[ "$DATASET" != "dev" ]]; then
    return 0
  fi

  if [[ "$REGEN" == "1" && -d "$CHANGESET_DIR" ]]; then
    rm -rf "$CHANGESET_DIR"
  fi

  if [[ -d "$CHANGESET_DIR" ]]; then
    return 0
  fi

  echo "Generating changesets (DATASET=$DATASET) into $CHANGESET_DIR..."
  (cd bench && go run ./cmd/gen-changesets/main.go --profile "$GEN_PROFILE" --versions "$GEN_VERSIONS" --scale "$GEN_SCALE" "$CHANGESET_DIR")
}

mkdir -p "$RESULTS_DIR" "$DATA_DIR"

target_args=()
if [[ "$TARGET_VERSION" != "0" ]]; then
  target_args=(--target-version "$TARGET_VERSION")
fi

maybe_generate_changesets

if [[ ! -d "$CHANGESET_DIR" ]]; then
  echo "changeset dir not found: $CHANGESET_DIR" >&2
  echo "tip: set CHANGESET_DIR=... or use DATASET=sample for the small checked-in dataset" >&2
  exit 1
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
  if [[ -n "${TREEDB_LEAF_PREFIX_COMPRESSION:-}" ]]; then
    extra_env+=("TREEDB_LEAF_PREFIX_COMPRESSION=${TREEDB_LEAF_PREFIX_COMPRESSION}")
  fi
  if [[ -n "${TREEDB_BENCH_VLOG_DICT_TRAIN_BYTES:-}" ]]; then
    extra_env+=("TREEDB_BENCH_VLOG_DICT_TRAIN_BYTES=${TREEDB_BENCH_VLOG_DICT_TRAIN_BYTES}")
  fi

  echo
  echo "=== treedb/v1 (${name}) ==="
  env \
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
# - mode4: journal/WAL off throughput (unsafe)
# - mode3: journal/WAL on, strict sync + checksums (durable-ish)
run_treedb_v1 mode4-wal-off \
  TREEDB_BENCH_PROFILE=fast \
  TREEDB_BENCH_DISABLE_WAL=1 \
  TREEDB_BENCH_IAVL_SYNC=0 \
  TREEDB_BENCH_RELAXED_SYNC=1 \
  TREEDB_BENCH_DISABLE_READ_CHECKSUM=1 \
  TREEDB_BENCH_ALLOW_UNSAFE=1

if [[ "$INCLUDE_MODE3" == "1" ]]; then
  run_treedb_v1 mode3-wal-on \
    TREEDB_BENCH_PROFILE=durable \
    TREEDB_BENCH_DISABLE_WAL=0 \
    TREEDB_BENCH_IAVL_SYNC=1 \
    TREEDB_BENCH_RELAXED_SYNC=0 \
    TREEDB_BENCH_DISABLE_READ_CHECKSUM=0 \
    TREEDB_BENCH_ALLOW_UNSAFE=0
else
  echo
  echo "=== treedb/v1 (mode3-wal-on) ==="
  echo "skipped (set INCLUDE_MODE3=1 to run; WAL-on is more experimental and may fail on larger datasets)"
fi

run_iavl_v1_leveldb
run_iavl_v1_memdb

echo
echo "Done."
echo "results: $RESULTS_DIR"
echo "data:    $DATA_DIR"

if [[ "$DASHBOARD" == "1" ]]; then
  echo
  echo "Starting dashboard for $RESULTS_DIR..."
  exec ./scripts/run_dashboard.sh "$RESULTS_DIR"
fi

echo
echo "Dashboard:"
echo "  ./scripts/run_dashboard.sh \"$RESULTS_DIR\""
echo "tip: next time you can auto-start it via: DASHBOARD=1 ./scripts/run_matrix.sh"
