#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

RESULTS_DIR="${1:-}"

if [[ -z "$RESULTS_DIR" ]]; then
  if [[ -d "$ROOT_DIR/results" ]]; then
    RESULTS_DIR="$(ls -1dt "$ROOT_DIR/results"/* 2>/dev/null | head -n 1 || true)"
  fi
fi

if [[ -z "$RESULTS_DIR" || ! -d "$RESULTS_DIR" ]]; then
  echo "usage: $0 <results-dir>" >&2
  echo "example: $0 ./results/20260126-084312" >&2
  exit 1
fi

if command -v uv >/dev/null 2>&1; then
  BENCHMARK_RESULTS="$RESULTS_DIR" uv run streamlit run analysis/dashboard.py
  exit 0
fi

if command -v python3 >/dev/null 2>&1; then
  BENCHMARK_RESULTS="$RESULTS_DIR" python3 -m streamlit run analysis/dashboard.py
  exit 0
fi

echo "could not find 'uv' or 'python3' to run streamlit" >&2
exit 1
