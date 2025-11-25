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
cargo install --locked cargo-docs-rs
cargo install --locked cargo-workspaces
cargo version
rustup show active-toolchain -v

packages=($(cargo workspaces plan 2>/dev/null | head -20))
if [[ "${TRIGGER_NAME:-}" == "gcb-pm-*" ]]; then
    packages=($(cargo workspaces plan 2>/dev/null | grep -v gcp-sdk))
fi

for package in "${packages[@]}"; do
    env RUSTDOCFLAGS='-D warnings' cargo +nightly docs-rs -p "${package}"
done

echo "==== DONE ===="

/workspace/.bin/sccache --show-stats
