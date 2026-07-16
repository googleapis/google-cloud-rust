#!/bin/bash
set -euo pipefail

export RUSTFLAGS="--cfg google_cloud_unstable_storage_bidi"

# Parse args passed directly to script
EXTRA_ARGS="$@"

echo "Building benchmark..."
cargo build --release -p storage-benchmark-appendable-object

echo "Running Scenario 1: 4 KiB chunks"
cargo run --release -p storage-benchmark-appendable-object -- \
  --chunk-size 4096 \
  $EXTRA_ARGS

echo "Running Scenario 1: 64 KiB chunks"
cargo run --release -p storage-benchmark-appendable-object -- \
  --chunk-size 65536 \
  $EXTRA_ARGS

echo "Running Scenario 1: 256 KiB chunks"
cargo run --release -p storage-benchmark-appendable-object -- \
  --chunk-size 262144 \
  $EXTRA_ARGS

echo "Running Scenario 1: 1 MiB chunks"
cargo run --release -p storage-benchmark-appendable-object -- \
  --chunk-size 1048576 \
  $EXTRA_ARGS

echo "Benchmark complete."
