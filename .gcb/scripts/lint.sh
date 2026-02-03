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

rustup component add clippy rustfmt
cargo version
rustup show active-toolchain -v
time cargo install --locked cargo-deny@0.19.0

set +v

echo "==== cargo deny ===="
cargo deny check
echo "==== cargo clippy ===="
cargo clippy --workspace --all-targets --profile=test -- --deny warnings
echo "==== cargo fmt ===="
cargo fmt
git diff --exit-code

echo "==== DONE ===="

/workspace/.bin/sccache --show-stats
