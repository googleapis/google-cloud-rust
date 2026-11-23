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

set -ev

export RUSTDOCFLAGS="${RUSTDOCFLAGS:-} -D warnings"

rustup component add clippy
cargo version
rustup show active-toolchain -v

cargo clean
echo "==== google-cloud-spanner ===="
cargo check --profile=ci --lib --package google-cloud-spanner --no-default-features
cargo test --profile=ci --package google-cloud-spanner --no-default-features
cargo doc  --profile=ci --package google-cloud-spanner --no-default-features --no-deps
for feature in unstable-metrics unstable-stream unstable-time builtin-metrics default-rustls-provider; do
  echo "==== google-cloud-spanner + ${feature} ===="
  cargo check --profile=ci --lib --package google-cloud-spanner --no-default-features --features "${feature}"
done
cargo test --profile=ci --package google-cloud-spanner --all-features
cargo doc  --profile=ci --package google-cloud-spanner --all-features --no-deps
cargo clippy --profile=ci --no-deps --package google-cloud-spanner --no-default-features --all-targets -- --deny warnings
cargo clippy --profile=ci --no-deps --package google-cloud-spanner --all-targets -- --deny warnings
cargo clippy --profile=ci --no-deps --package google-cloud-spanner --all-features --all-targets -- --deny warnings

echo "==== DONE ===="

/workspace/.bin/sccache --show-stats
