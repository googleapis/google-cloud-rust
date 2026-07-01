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

cargo version
rustup show active-toolchain -v

export RUSTDOCFLAGS='-D warnings'
.gcb/scripts/cargo-fetch.sh

cargo doc --no-deps --frozen --all-features -p google-cloud-wkt
cargo doc --no-deps --frozen --all-features -p google-cloud-rpc
cargo doc --no-deps --frozen --all-features -p google-cloud-gax
cargo doc --no-deps --frozen --all-features -p google-cloud-auth
cargo doc --no-deps --frozen --all-features -p google-cloud-gax-internal
cargo doc --no-deps --frozen --all-features -p google-cloud-location

# On PRs, detect any new libraries and compile their documentation. Without this
# step the post-merge build may break, and we prefer to avoid this problem.
if [[ "${GCB_TRIGGER_NAME:-}" != "gcb-pm-*" ]]; then
    git fetch --unshallow
    mapfile -t new_manifests < <(git diff "origin/main...HEAD" --name-only --diff-filter=A | grep /Cargo.toml)
    for manifest in "${new_manifests[@]}"; do
        cargo doc --no-deps --frozen --all-features --manifest-path "${manifest}"
    done
fi

args=()
if [[ "${GCB_TRIGGER_NAME:-}" == "gcb-pm-*" ]]; then
    args+=("--workspace")
fi
cargo doc --no-deps --frozen --all-features "${args[@]}"
