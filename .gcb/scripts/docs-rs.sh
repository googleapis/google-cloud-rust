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

# This is the only case where using `nightly` for CI is acceptable. Or maybe
# "we must do, with regrets". The rationale is that docs.rs uses nightly to
# generate the documentation, our releases would break if this test does not
# pass.
rustup toolchain install nightly
cargo install --locked cargo-docs-rs
cargo install --locked cargo-workspaces
cargo version
rustup show active-toolchain -v

# Seed the basic common packages serially.
export RUSTDOCFLAGS='-D warnings'
.gcb/scripts/cargo-fetch.sh

cargo +nightly docs-rs --frozen -p google-cloud-wkt
cargo +nightly docs-rs --frozen -p google-cloud-rpc
cargo +nightly docs-rs --frozen -p google-cloud-gax
cargo +nightly docs-rs --frozen -p google-cloud-auth
cargo +nightly docs-rs --frozen -p google-cloud-gax-internal
cargo +nightly docs-rs --frozen -p google-cloud-lro

mapfile -t packages < <(cargo workspaces plan 2>/dev/null)
if [[ "${GCB_TRIGGER_NAME:-}" != "gcb-pm-*" ]]; then
    packages=("${packages[@]:0:20}")
fi
printf "%s\n" "${packages[@]}" | \
    xargs -P $(nproc) -I{} cargo +nightly --frozen docs-rs -p {}
