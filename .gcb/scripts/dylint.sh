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

# Runs the custom lints in `tools/lints` (see `[workspace.metadata.dylint]` in
# the top-level Cargo.toml and `dylint.toml`) over the library targets of the
# whole workspace, including generated code.

set -ev

cargo install --locked cargo-dylint@6.4.0 dylint-link@6.4.0
cargo version
rustup show active-toolchain -v

# The lint libraries build against a pinned nightly with `rustc_private`. Each
# library pins its toolchain in its own `rust-toolchain.toml`; install it.
pushd tools/lints/explicit_trait_params
rustup toolchain install
rustup show active-toolchain -v
popd

# The Dylint driver is used as `RUSTC_WORKSPACE_WRAPPER`. Do not wrap it in
# sccache: cached outputs could hide lint diagnostics.
unset RUSTC_WRAPPER

set +v

echo "==== lint UI tests ===="
(cd tools/lints/explicit_trait_params && cargo test)

# `DYLINT_RUSTFLAGS` is passed only to the Dylint driver (i.e. only to workspace
# members), so findings fail the build without affecting other crates.
export DYLINT_RUSTFLAGS="--deny explicit_trait_params"

# Matches `lint.sh`: `--all-features` so feature-gated code is linted, and the
# two largest crates are checked separately.
echo "==== cargo dylint ===="
cargo dylint --all -- --workspace --lib --all-features --exclude google-cloud-compute-v1 --exclude google-cloud-aiplatform-v1

echo "==== cargo dylint (large crates) ===="
cargo dylint --all -- --lib --all-features -p google-cloud-compute-v1
cargo dylint --all -- --lib --all-features -p google-cloud-aiplatform-v1

echo "==== DONE ===="
