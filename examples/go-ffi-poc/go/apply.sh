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

# Applies the Go side of the POC to a google-cloud-go checkout of upstream commit 867fe707ef, and
# copies the Rust core next to the Go bindings, where storage/internal/rustcore/build.sh and the
# benchmark scripts expect it.
#
#   git clone https://github.com/googleapis/google-cloud-go && git -C google-cloud-go checkout 867fe707ef
#   ./apply.sh google-cloud-go
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
go_repo="${1:?usage: apply.sh <google-cloud-go checkout>}"

git -C "$go_repo" apply "$here/google-cloud-go.patch"
core="$go_repo/storage/internal/rustcore/gcs_core"
mkdir -p "$core"
cp -r "$here/../gcs_core/Cargo.toml" "$here/../gcs_core/Cargo.lock" "$here/../gcs_core/src" "$core/"
echo "applied; next: cd $go_repo/storage && ./internal/rustcore/build.sh (see go/README.md)"
