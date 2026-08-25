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

set -euo pipefail

WORKFLOW_FILE=".github/workflows/rust-toolchain-check.yaml"

if [[ ! -f "${WORKFLOW_FILE}" ]]; then
  echo "Error: ${WORKFLOW_FILE} not found." >&2
  exit 1
fi

CURRENT_VERSION=$(grep -oP "(?<=CURRENT_RUST_VERSION: ')[0-9.]+" "${WORKFLOW_FILE}")
echo "Current configured Rust toolchain version: ${CURRENT_VERSION}"

LATEST_MINOR=$(curl -sSL https://raw.githubusercontent.com/rust-lang/rust/master/RELEASES.md | grep -oP '(?<=Version 1\.)[0-9]+' | head -n 1)
LATEST_VERSION="1.${LATEST_MINOR}"
echo "Latest stable Rust release:               ${LATEST_VERSION}"

if [[ "${CURRENT_VERSION}" == "${LATEST_VERSION}" && "${1:-}" != "--force" ]]; then
  echo "Rust toolchain is already up to date (${CURRENT_VERSION}). Use --force to run checks anyway."
  exit 0
fi

echo ""
echo "==== 1. Updating Stable Rust Toolchain ===="
rustup update stable
rustc --version

echo ""
echo "==== 2. Running Strict Workspace Clippy ===="
cargo clippy --all-features --all-targets --profile=test --workspace -- --deny warnings

echo ""
echo "==== 3. Verifying Semver Checks Tooling ===="
cargo semver-checks --all-features -p google-cloud-wkt

echo ""
echo "==== Toolchain check passed! Ready for CI version updates to ${LATEST_VERSION}. ===="
