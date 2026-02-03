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

cargo clean
echo "==== google-cloud-gax-internal ===="
cargo test --package google-cloud-gax-internal --no-default-features
cargo doc  --package google-cloud-gax-internal --no-default-features --no-deps
for feature in _internal-common _internal-http-client _internal-grpc-client _internal-http-multipart _internal-http-stream; do
    cargo test --package google-cloud-gax-internal --no-default-features --features "${feature}"
    cargo doc  --package google-cloud-gax-internal --no-default-features --features "${feature}" --no-deps
done
cargo test --package google-cloud-gax-internal --all-features
cargo doc  --package google-cloud-gax-internal --all-features --no-deps
cargo clippy --no-deps --package google-cloud-gax-internal --all-targets -- --deny warnings
cargo clippy --no-deps --package google-cloud-gax-internal --all-features --all-targets --profile=test -- --deny warnings

echo "==== DONE ===="

/workspace/.bin/sccache --show-stats
