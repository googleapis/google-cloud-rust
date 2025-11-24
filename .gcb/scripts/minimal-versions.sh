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

cargo install cargo-minimal-versions cargo-hack@0.6.37 cargo-minimal-versions@0.1.31 --locked
cargo version
rustup show active-toolchain -v

set -e
echo "Check key libraries against minimum versions of external packages."
cargo minimal-versions test --package google-cloud-storage
cargo minimal-versions check --package google-cloud-storage
cargo minimal-versions test --package google-cloud-gax-internal
cargo minimal-versions check --package google-cloud-gax-internal

echo "Prepare workspace to run minimal version tool."
cargo run --release --package minimal-version-helper prepare
cargo minimal-versions test --package google-cloud-storage
cargo minimal-versions check --package google-cloud-storage
cargo minimal-versions check --package google-cloud-gax-internal
cargo minimal-versions check --package google-cloud-auth
cargo minimal-versions check --package google-cloud-gax
cargo minimal-versions check --package google-cloud-wkt

echo "==== DONE ===="

/workspace/.bin/sccache --show-stats
