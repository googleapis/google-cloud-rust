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

CURRENT_VERSION=$(sed -nE "s/.*CURRENT_RUST_VERSION: '([0-9.]+)'.*/\1/p" "${WORKFLOW_FILE}")
echo "Current configured Rust toolchain version: ${CURRENT_VERSION}"

LATEST_MINOR=$(curl -sSL https://raw.githubusercontent.com/rust-lang/rust/master/RELEASES.md | sed -nE 's/^Version 1\.([0-9]+)\..*/\1/p' | sed -n '1p')
LATEST_VERSION="1.${LATEST_MINOR}"
echo "Latest stable Rust release:               ${LATEST_VERSION}"

if [[ "${CURRENT_VERSION}" == "${LATEST_VERSION}" && "${1:-}" != "--force" ]]; then
  echo "Rust toolchain is already up to date (${CURRENT_VERSION}). Use --force to run checks anyway."
  exit 0
fi

echo ""
echo "==== 1. Checking out Feature Branch ===="
TARGET_BRANCH="chore-bump-rust-toolchain-${LATEST_VERSION}"
CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)

if [[ "${CURRENT_BRANCH}" != "${TARGET_BRANCH}" ]]; then
  if git show-ref --verify --quiet "refs/heads/${TARGET_BRANCH}"; then
    echo "Switching to existing branch: ${TARGET_BRANCH}"
    git checkout "${TARGET_BRANCH}"
  else
    echo "Creating and checking out branch: ${TARGET_BRANCH} based on main"
    git checkout -b "${TARGET_BRANCH}" main
  fi
fi

echo ""
echo "==== 2. Updating Stable Rust Toolchain ===="
rustup update stable
rustc --version

echo ""
echo "==== 3. Applying Automatic Clippy Fixes ===="
cargo clippy --fix --allow-dirty --allow-staged --all-features --all-targets --profile=test --workspace || true

# Check if automatic fixes modified generated code
if [[ -n "$(git status --porcelain -- '**/generated/**')" ]]; then
  echo "" >&2
  echo "ERROR: Automatic clippy fixes modified generated code in **/generated/**." >&2
  echo "Generated code cannot be edited directly in this repository." >&2
  echo "" >&2
  echo "To resolve:" >&2
  echo "1. Inspect the diff to see what needs to be updated in the generator:" >&2
  echo "   git diff -- '**/generated/**'" >&2
  echo "2. Submit a PR in googleapis/librarian to update generator templates first." >&2
  echo "3. Discard local edits in generated directories before committing:" >&2
  echo "   git restore -- '**/generated/**'" >&2
  exit 1
fi

echo ""
echo "==== 4. Running Strict Workspace Clippy ===="
cargo clippy --all-features --all-targets --profile=test --workspace -- --deny warnings

echo ""
echo "==== 5. Verifying Semver Checks Tooling ===="
cargo semver-checks --all-features -p google-cloud-wkt

echo ""
echo "==== Toolchain check passed! Ready for CI version updates to ${LATEST_VERSION}. ===="
