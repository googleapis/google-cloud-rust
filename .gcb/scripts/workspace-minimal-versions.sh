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

cargo install cargo-workspaces@0.4.2 cargo-hack@0.6.37 cargo-minimal-versions@0.1.31 --locked
cargo version
rustup show active-toolchain -v

set -e
echo "Prepare workspace to run minimal version tool."
cargo run --release --package minimal-version-helper prepare
for PKG in $(cargo workspaces plan | grep -v gcp-sdk); do
  cargo minimal-versions check -p ${PKG} --all-features
done
echo "==== DONE ===="

/workspace/.bin/sccache --show-stats
