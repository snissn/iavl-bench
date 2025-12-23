#!/bin/bash
set -e

echo "Cleaning up previous benchmark data..."
rm -rf data-treedb data-leveldb-v1 data-memdb-v1 results
mkdir -p data-treedb data-leveldb-v1 data-memdb-v1 results

echo "Building benchmarks..."
(cd treedb && go build -o treedb-bench .)
(cd iavl-v1 && go build -o iavl-v1-bench .)
(cd iavl-v1-memdb && go build -o iavl-v1-memdb-bench .)

echo "Running TreeDB Benchmark..."
./treedb/treedb-bench bench \
    --db-dir ./data-treedb \
    --changeset-dir ./changesets \
    --log-file ./results/treedb.jsonl \
    --log-type json

echo "Running IAVL v1 LevelDB Benchmark..."
./iavl-v1/iavl-v1-bench bench \
    --db-dir ./data-leveldb-v1 \
    --changeset-dir ./changesets \
    --log-file ./results/iavl-v1-leveldb.jsonl \
    --log-type json

echo "Running iavl/v1 MemDB Benchmark..."
./iavl-v1-memdb/iavl-v1-memdb-bench bench \
    --db-dir ./data-memdb-v1 \
    --changeset-dir ./changesets \
    --log-file ./results/iavl-v1-memdb.jsonl \
    --log-type json

echo "Done! Results saved to ./results/"
ls -lh results/
