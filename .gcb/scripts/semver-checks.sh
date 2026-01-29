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

cargo install --locked cargo-semver-checks@0.46.0
cargo version
rustup show active-toolchain -v

cargo semver-checks --all-features -p google-cloud-wkt
cargo semver-checks --all-features -p google-cloud-api
cargo semver-checks --all-features -p google-cloud-rpc
cargo semver-checks --all-features -p google-cloud-gax
cargo semver-checks --all-features -p google-cloud-auth
cargo semver-checks --all-features -p google-cloud-gax-internal
cargo semver-checks --all-features -p google-cloud-type
cargo semver-checks --all-features -p google-cloud-iam-v1
cargo semver-checks --all-features -p google-cloud-location
cargo semver-checks --all-features -p google-cloud-longrunning
cargo semver-checks --all-features -p google-cloud-lro
cargo semver-checks --all-features -p google-cloud-kms-v1
cargo semver-checks --all-features -p google-cloud-language-v2
cargo semver-checks --all-features -p google-cloud-secretmanager-v1
cargo semver-checks --all-features -p google-cloud-speech-v2
cargo semver-checks --all-features -p google-cloud-storage

echo "==== DONE ===="

/workspace/.bin/sccache --show-stats
