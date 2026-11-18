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

CARGO_TOML="Cargo.toml"

if [[ ! -f "${CARGO_TOML}" ]]; then
  echo "Error: ${CARGO_TOML} not found. Run this script from the repository root." >&2
  exit 1
fi

CURRENT_MSRV=$(sed -nE 's/^rust-version *= *"([0-9]+\.[0-9]+\.[0-9]+)"/\1/p' "${CARGO_TOML}")
echo "Current configured MSRV: ${CURRENT_MSRV}"

RELEASES=$(curl -sSL https://raw.githubusercontent.com/rust-lang/rust/master/RELEASES.md)

if [[ $# -ge 1 && "${1}" != "--force" ]]; then
  TARGET_MSRV="${1}"
  # Normalize 1.XX -> 1.XX.0
  if [[ "${TARGET_MSRV}" =~ ^[0-9]+\.[0-9]+$ ]]; then
    TARGET_MSRV="${TARGET_MSRV}.0"
  fi
  if [[ ! "${TARGET_MSRV}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "Error: Invalid version format '${TARGET_MSRV}'. Expected 1.XX.0 or 1.XX." >&2
    exit 1
  fi
  ELIGIBLE_LINE=$(echo "${RELEASES}" | grep -E "^Version ${TARGET_MSRV//./\\.} \([0-9]{4}-[0-9]{2}-[0-9]{2}\)" | head -n 1 || true)
else
  # Find the oldest Rust release within the past year (dropping support for releases older than 1 year).
  CUTOFF=$(date -u -d '1 year ago' +%F 2>/dev/null || date -u -v-1y +%F || true)
  if [[ ! "${CUTOFF}" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo "Error: Failed to calculate a valid 1-year cutoff date." >&2
    exit 1
  fi
  ELIGIBLE_LINE=$(echo "${RELEASES}" | \
    grep -E '^Version 1\.[0-9]+\.[0-9]+ \([0-9]{4}-[0-9]{2}-[0-9]{2}\)' | \
    awk -v cutoff="${CUTOFF}" '
      match($0, /[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]/) {
        d = substr($0, RSTART, RLENGTH)
        if (d >= cutoff) {
          last = $0
        } else {
          print last
          found = 1
          exit
        }
      }
      END {
        if (!found && last) print last
      }
    ')
  TARGET_MSRV=$(echo "${ELIGIBLE_LINE}" | sed -nE 's/^Version ([0-9]+\.[0-9]+\.[0-9]+).*/\1/p')
fi

TARGET_MINOR="${TARGET_MSRV%.*}"
RELEASE_DATE=$(echo "${ELIGIBLE_LINE}" | sed -nE 's/.*\(([0-9]{4}-[0-9]{2}-[0-9]{2})\).*/\1/p')

if [[ -z "${TARGET_MSRV}" || -z "${RELEASE_DATE}" ]]; then
  echo "Error: Could not find release date for Rust ${TARGET_MSRV:-unknown} in RELEASES.md." >&2
  exit 1
fi

echo "Target MSRV:             ${TARGET_MSRV} (released ${RELEASE_DATE})"

if [[ "${CURRENT_MSRV}" == "${TARGET_MSRV}" && " $* " != *" --force "* ]]; then
  echo "MSRV is already up to date (${CURRENT_MSRV}). Pass a version or --force to run checks anyway."
  exit 0
fi

# Portable in-place replacement and verification helper (compatible with GNU and BSD sed).
update_and_verify() {
  local file="$1"
  local sed_expr="$2"
  local expected_pattern="$3"

  sed -E "${sed_expr}" "${file}" > "${file}.tmp"
  mv "${file}.tmp" "${file}"

  if ! grep -Eq "${expected_pattern}" "${file}"; then
    echo "ERROR: Failed to update MSRV in ${file} (expected pattern: ${expected_pattern})" >&2
    exit 1
  fi
  echo "  Updated ${file}"
}

echo ""
echo "==== 1. Updating MSRV to ${TARGET_MSRV} ===="
update_and_verify "Cargo.toml" \
  "s/^(rust-version *= *\").*(\")/\1${TARGET_MSRV}\2/" \
  "^rust-version *= *\"${TARGET_MSRV//./\\.}\"$"

update_and_verify ".clippy.toml" \
  "s/^(msrv *= *\").*(\")/\1${TARGET_MSRV}\2/" \
  "^msrv *= *\"${TARGET_MSRV//./\\.}\"$"

update_and_verify ".gcb/msrv.yaml" \
  "s/^(  _RUST_VERSION: ').*(')/\1${TARGET_MINOR}\2/" \
  "^  _RUST_VERSION: '${TARGET_MINOR//./\\.}'$"

update_and_verify "README.md" \
  "s/^(We require Rust >= )[0-9]+\.[0-9]+(\..*)/\1${TARGET_MINOR}\2/" \
  "^We require Rust >= ${TARGET_MINOR//./\\.}\."

update_and_verify "doc/contributor/howto-guide-set-up-development-environment.md" \
  "s/^(You will need rust >= )[0-9]+\.[0-9]+ \(released around [0-9]{4}-[0-9]{2}-[0-9]{2}\)(.*)/\1${TARGET_MINOR} (released around ${RELEASE_DATE})\2/" \
  "^You will need rust >= ${TARGET_MINOR//./\\.} \(released around ${RELEASE_DATE}\)\."

echo ""
echo "==== 2. Running Clippy Verification ===="
cargo clippy --all-targets -- -D warnings
cargo clippy --profile test -- -D warnings
cargo clippy-strict

echo ""
echo "==== MSRV bump to ${TARGET_MSRV} verified! ===="
