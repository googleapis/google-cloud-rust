#!/usr/bin/env bash
# Copyright 2025 Google LLC
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

set -ev

rustup component add clippy
cargo version
rustup show active-toolchain -v

set +v

echo "==== cargo clippy ===="
cargo clippy --all-features --all-targets --profile=test --workspace -- --deny warnings

echo "==== cargo clippy strict (handwritten crates non-test mode) ===="
cargo clippy --all-features --no-deps -p google-cloud-auth -p google-cloud-bigquery -p google-cloud-bigtable -p google-cloud-datastore -p google-cloud-firestore -p google-cloud-gax -p google-cloud-lro -p google-cloud-pubsub -p google-cloud-storage -p google-cloud-wkt -- -D missing_docs -D clippy::exhaustive_enums

echo "==== DONE ===="
/workspace/.bin/sccache --show-stats
