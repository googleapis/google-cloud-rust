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

rustup toolchain install nightly
rustup default nightly
rustup component add clippy rustfmt
cargo version
rustup show active-toolchain -v

set -e
echo "RUSTFLAGS in test-unstable-cfg: $RUSTFLAGS"

# Key crates
crates=(
    "google-cloud-gax"
    "google-cloud-gax-internal"
    "google-cloud-test-utils"
)
for crate in "${crates[@]}"; do
  echo "==== ${crate} (UNSTABLE - No Default Features) ===="
  cargo test --package ${crate} --no-default-features

  echo "==== ${crate} (UNSTABLE - All Features) ===="
  cargo test --package ${crate} --all-features
done

# Integration tests
echo "==== integration-tests (UNSTABLE) ===="
cargo test -p integration-tests --features run-showcase-tests

echo "==== DONE ===="

/workspace/.bin/sccache --show-stats
