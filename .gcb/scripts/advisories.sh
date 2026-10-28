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

# Finds crates with RUSTSEC advisories and tries to fix them with
# `cargo update`.
#
# Usage:
#   advisories.sh [fix] [cargo-deny check args...]
#       Update the affected crates, then re-run the advisory check. Exits
#       non-zero if any advisory remains (e.g. it needs a semver-incompatible
#       bump or an ignore in `deny.toml`). This modifies `Cargo.lock`.
#   advisories.sh report [cargo-deny check args...]
#       Print a markdown summary of the failing advisories, suitable for a
#       GitHub issue body. This does not modify any files.
#
# Extra arguments are passed to `cargo deny check`, e.g. `--disable-fetch`.

set -euo pipefail

# Prints one JSON object per failing advisory:
#   {"id", "name", "version", "solution"}
# Diagnostics without an affected crate are reported with name "-" and a null
# version, so they are never silently dropped.
failing_json() {
  # `cargo deny` exits non-zero when it finds advisories, and writes its
  # diagnostics to stderr. Skip any line that is not JSON.
  { cargo deny --format json check "$@" advisories 2>&1 || true; } |
    jq -cR 'fromjson?
      | select(.type == "diagnostic" and .fields.severity == "error")
      | .fields as $f
      | ([($f.notes // [])[] | select(startswith("Solution: "))
          | ltrimstr("Solution: ")] | first // $f.message // "See advisory")
        as $solution
      | (if ($f.graphs // []) == [] then {name: "-", version: null}
          else $f.graphs[].Krate end)
      | {id: ($f.advisory.id // $f.code), name, version, solution: $solution}'
}

# Runs `cargo update` on every crate listed in the given `failing_json` output.
update() {
  local specs
  mapfile -t specs < <(jq -r 'select(.version) | "\(.name)@\(.version)"' <<<"$1" | sort -u)
  if [[ ${#specs[@]} -eq 0 ]]; then
    return 0
  fi
  cargo update "${specs[@]}"
}

fix() {
  update "$(failing_json "$@")"
  # Always finish with the unfiltered check, so its exit code is the result.
  cargo deny check "$@" --hide-inclusion-graph advisories
}

report() {
  local failing
  failing="$(failing_json "$@")"

  echo "A new advisory was detected during the scheduled check."
  echo
  if [[ -n "${failing}" ]]; then
    echo "| Advisory | Crate | Solution |"
    echo "|---|---|---|"
    jq -rs 'unique_by([.id, .name, .version]) | .[]
      | (if (.id | startswith("RUSTSEC-"))
          then "[\(.id)](https://rustsec.org/advisories/\(.id))"
          else .id end) as $link
      | (if .version then "\(.name)@\(.version)" else .name end) as $crate
      | "| \($link) | \($crate) | \(.solution | gsub("\\|"; "\\|")) |"' \
      <<<"${failing}"
  else
    echo "Could not determine the affected crates automatically."
  fi
  echo
  echo "- To try a fix: \`.gcb/scripts/advisories.sh\` (runs \`cargo update\` on the affected crates and re-checks)"
  echo "- To see details: \`cargo deny check advisories --hide-inclusion-graph\`"
  echo "- If an upgrade is blocked, find what pulls the crate in: \`cargo tree -i <crate>@<version>\`"
  echo "- If no fix is available, ignore the advisory in \`deny.toml\` with a reason."
  if [[ -n "${GITHUB_RUN_ID:-}" ]]; then
    echo
    echo "Run: ${GITHUB_SERVER_URL}/${GITHUB_REPOSITORY}/actions/runs/${GITHUB_RUN_ID}"
  fi
  echo
  echo "cc @googleapis/cloud-sdk-rust-team"
}

case "${1:-}" in
  report)
    shift
    report "$@"
    ;;
  fix)
    shift
    fix "$@"
    ;;
  *)
    fix "$@"
    ;;
esac
