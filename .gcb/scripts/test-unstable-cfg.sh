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
echo "RUSTFLAGS in test-unstable-cfg: ${RUSTFLAGS:-}"
echo "RUSTDOCFLAGS in test-unstable-cfg: ${RUSTDOCFLAGS:-}"

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
echo "==== Install go compiler ===="
curl -fsSL --retry 5 --retry-delay 15 https://go.dev/dl/go1.25.4.linux-amd64.tar.gz -o /tmp/go.tar.gz
sha256sum -c <(echo 9fa5ffeda4170de60f67f3aa0f824e426421ba724c21e133c1e35d6159ca1bec /tmp/go.tar.gz)
tar -C /usr/local -xzf /tmp/go.tar.gz
export PATH=${PATH}:/usr/local/go/bin

echo "==== integration-tests (UNSTABLE) ===="
cargo test -p integration-tests-showcase --features run-showcase-tests

echo "==== DONE ===="

/workspace/.bin/sccache --show-stats
