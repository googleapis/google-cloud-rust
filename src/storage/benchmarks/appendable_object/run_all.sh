#!/usr/bin/env bash
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

export RUSTFLAGS="--cfg google_cloud_unstable_storage_bidi"

# Parse args passed directly to script
EXTRA_ARGS=( "$@" )

echo "Building benchmark..."
cargo build --release -p storage-benchmark-appendable-object

echo "Running Scenario 1: 4 KiB chunks"
cargo run --release -p storage-benchmark-appendable-object -- \
  --chunk-size 4096 \
  "${EXTRA_ARGS[@]}"

echo "Running Scenario 1: 64 KiB chunks"
cargo run --release -p storage-benchmark-appendable-object -- \
  --chunk-size 65536 \
  "${EXTRA_ARGS[@]}"

echo "Running Scenario 1: 256 KiB chunks"
cargo run --release -p storage-benchmark-appendable-object -- \
  --chunk-size 262144 \
  "${EXTRA_ARGS[@]}"

echo "Running Scenario 1: 1 MiB chunks"
cargo run --release -p storage-benchmark-appendable-object -- \
  --chunk-size 1048576 \
  "${EXTRA_ARGS[@]}"

echo "Benchmark complete."
