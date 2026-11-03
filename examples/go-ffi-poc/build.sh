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

# Builds the Rust core (gcs_core) as a static library. Given a google-cloud-go checkout with the
# Go side applied (see go/README.md), also installs it where the Go cgo bindings link it from.
#
#   ./build.sh                             # build only -> gcs_core/target/release/libgcs_core.a
#   ./build.sh ~/src/google-cloud-go       # build and install into that checkout
#   CRYPTO=aws-lc ./build.sh               # aws-lc instead of ring (needs cmake and a C compiler)
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
features=()
case "${CRYPTO:-ring}" in
  ring) ;;
  aws-lc) features=(--no-default-features --features crypto-aws-lc) ;;
  *) echo "CRYPTO must be ring or aws-lc" >&2; exit 1 ;;
esac

(cd "$here/gcs_core" && cargo build --release --locked "${features[@]}")
lib="${CARGO_TARGET_DIR:-$here/gcs_core/target}/release/libgcs_core.a"
echo "built $lib ($(du -h "$lib" | cut -f1))"

if [[ $# -gt 0 ]]; then
  rustcore="$1/storage/internal/rustcore"
  if [[ ! -f "$rustcore/rustcore.go" ]]; then
    echo "$1 has no storage/internal/rustcore/rustcore.go: apply the Go side first (go/README.md)" >&2
    exit 1
  fi
  goos="$(go env GOOS 2>/dev/null || echo linux)"
  goarch="$(go env GOARCH 2>/dev/null || echo amd64)"
  mkdir -p "$rustcore/lib/${goos}_${goarch}"
  cp "$lib" "$rustcore/lib/${goos}_${goarch}/libgcs_core.a"
  echo "installed $rustcore/lib/${goos}_${goarch}/libgcs_core.a"
fi
