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

# We use `--all-features` which triggers the Tonic+Prost code generation.
echo "==== Install protoc ===="
curl -fsSL --retry 5 --retry-delay 15 -o /tmp/protoc.zip https://github.com/protocolbuffers/protobuf/releases/download/v28.3/protoc-28.3-linux-x86_64.zip
sha256sum -c <(echo 0ad949f04a6a174da83cdcbdb36dee0a4925272a5b6d83f79a6bf9852076d53f  /tmp/protoc.zip)
env -C /usr/local unzip -x /tmp/protoc.zip
protoc --version

rustup component add clippy
cargo version
rustup show active-toolchain -v

echo "RUSTFLAGS in test-ignored-samples: ${RUSTFLAGS:-}"
echo "RUSTDOCFLAGS in test-ignored-samples: ${RUSTDOCFLAGS:-}"

cargo test --workspace --doc --all-features -- --ignored --show-output

echo "==== DONE ===="

/workspace/.bin/sccache --show-stats
