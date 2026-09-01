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

# Parse extra args passed directly to script
EXTRA_ARGS=( "$@" )

echo "Building benchmark..."
cargo build --release -p storage-benchmark-write-object

echo "================================================================="
echo "Running Tier 1: 12 MiB (Single-Shot Multipart Upload)"
echo "================================================================="
cargo run --release -p storage-benchmark-write-object -- \
  --object-size 12582912 \
  "${EXTRA_ARGS[@]}"

echo "================================================================="
echo "Running Tier 2: 64 MiB (Small Resumable Upload - 8 chunks)"
echo "================================================================="
cargo run --release -p storage-benchmark-write-object -- \
  --object-size 67108864 \
  "${EXTRA_ARGS[@]}"

echo "================================================================="
echo "Running Tier 3: 512 MiB (Medium Resumable Upload - 64 chunks)"
echo "================================================================="
cargo run --release -p storage-benchmark-write-object -- \
  --object-size 536870912 \
  "${EXTRA_ARGS[@]}"

echo "================================================================="
echo "Running Tier 4: 2 GiB (Large Resumable Upload - 256 chunks)"
echo "================================================================="
cargo run --release -p storage-benchmark-write-object -- \
  --object-size 2147483648 \
  "${EXTRA_ARGS[@]}"

echo "================================================================="
echo "Running Tier 5: 8 GiB (Stress Resumable Upload - 1,024 chunks)"
echo "================================================================="
cargo run --release -p storage-benchmark-write-object -- \
  --object-size 8589934592 \
  "${EXTRA_ARGS[@]}"

echo "================================================================="
echo "All benchmarks complete!"
echo "================================================================="
