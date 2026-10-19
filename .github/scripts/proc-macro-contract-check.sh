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

# Checks the proc-macro contract tests (`src/*/tests/*_contract/`). See the
# "Compatibility" section in `src/bigquery-derive/src/lib.rs` for the policy.
#
# Usage: [ALLOW_CONTRACT_CHANGE=true] proc-macro-contract-check.sh [BASE_REF]
#
# BASE_REF defaults to `origin/main`. CI passes `HEAD^1`, the base branch tip
# of the pull request merge commit. CI sets ALLOW_CONTRACT_CHANGE=true when the pull
# request has the 'rust: proc-macro-contract-change' label.

set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

ALLOW_CONTRACT_CHANGE="${ALLOW_CONTRACT_CHANGE:-false}"
BASE_REF="${1:-origin/main}"
LABEL='rust: proc-macro-contract-change'
POLICY='the "Compatibility" section in src/bigquery-derive/src/lib.rs'
PATHSPEC=':(glob)src/*/tests/*_contract/**'
MOD_RE='^[[:space:]]*(pub(\([^)]*\))?[[:space:]]+)?mod[[:space:]]+([A-Za-z0-9_]+)[[:space:]]*;.*'

# Prints the sorted module names declared with `mod <name>;` on stdin.
mods_of() {
  sed -nE "s/${MOD_RE}/\3/p" | sort -u
}

failed=0

# Check 1: existing contract cases are unchanged.
if [[ "${ALLOW_CONTRACT_CHANGE}" == "true" ]]; then
  echo "Skipping check 1 (existing contract cases are unchanged): the" \
    "'${LABEL}' label is applied."
else
  merge_base=$(git merge-base "${BASE_REF}" HEAD)
  violations=()
  while IFS=$'\t' read -r status path new_path; do
    [[ -z "${status}" ]] && continue
    if [[ "${status}" == "M" ]]; then
      # `main.rs` must change to register new cases; its `mod` lines are
      # checked below instead.
      [[ "$(basename "${path}")" == "main.rs" ]] && continue
      # Ignore mode changes and changes that only touch whitespace or blank
      # lines: they cannot change the accepted input.
      if ! git diff -w --ignore-blank-lines --no-ext-diff -U0 \
        "${merge_base}" HEAD -- "${path}" | grep -q '^@@'; then
        continue
      fi
    fi
    violations+=("${status}	${path}${new_path:+ -> ${new_path}}")
  done < <(git diff --name-status --diff-filter=MDR "${merge_base}" HEAD -- "${PATHSPEC}")

  while read -r main; do
    [[ -z "${main}" ]] && continue
    # A deleted `main.rs` is already reported above.
    git cat-file -e "HEAD:${main}" 2>/dev/null || continue
    removed=$(comm -23 \
      <(git show "${merge_base}:${main}" | mods_of) \
      <(git show "HEAD:${main}" | mods_of))
    for m in ${removed}; do
      violations+=("mod ${m}; removed from ${main}")
    done
  done < <(git ls-tree -r --name-only "${merge_base}" -- src |
    grep -E '^src/[^/]+/tests/[^/]+_contract/main\.rs$' || true)

  if [[ ${#violations[@]} -gt 0 ]]; then
    failed=1
    cat <<_EOF_
ERROR: The following proc-macro contract cases were modified, renamed, or
deleted relative to ${BASE_REF}:

$(printf '  %s\n' "${violations[@]}")

Contract cases pin the input accepted by proc-macro crates, which
cargo-semver-checks cannot analyze. Existing cases are append-only.

- If you are adding a feature, restore the existing files and add a new case
  file (and a new \`mod\` line in main.rs) instead.
- To change an existing case on purpose, apply this label to the pull
  request: '${LABEL}'. The label means "I changed an existing contract case
  on purpose". It skips only this check.
- If the change affects accepted input, this is very likely a breaking change.
  The pull request (or the release) must take a breaking version bump (major,
  or minor for 0.x) of BOTH the proc-macro crate (e.g.
  google-cloud-bigquery-derive) AND the crate that re-exports it (e.g.
  google-cloud-bigquery), and update the workspace dependency versions in the
  root Cargo.toml. Librarian defaults to non-breaking bumps, so do this by
  hand.
- Comment-only or formatting changes to existing cases also need the label,
  but no version bump. Only whitespace and blank-line changes are ignored.

See ${POLICY}.

_EOF_
  fi
fi

# Check 2: every case file is registered in main.rs and always compiled.
shopt -s nullglob
unregistered=()
conditional=()
for dir in src/*/tests/*_contract/; do
  main="${dir}main.rs"
  if [[ ! -f "${main}" ]]; then
    unregistered+=("${dir} (missing main.rs)")
    continue
  fi
  while read -r line; do
    [[ -n "${line}" ]] && conditional+=("${main}:${line}")
  done < <(grep -nE '#!?\[[[:space:]]*cfg' "${main}" || true)
  registered=$(mods_of <"${main}")
  for f in "${dir}"*.rs; do
    stem=$(basename "${f}" .rs)
    [[ "${stem}" == "main" ]] && continue
    if ! grep -qxF "${stem}" <<<"${registered}"; then
      unregistered+=("${f}")
    fi
  done
done

if [[ ${#unregistered[@]} -gt 0 ]]; then
  failed=1
  cat <<_EOF_
ERROR: The following proc-macro contract case files are not registered with a
\`mod <name>;\` line in their directory's main.rs, so they are never compiled:

$(printf '  %s\n' "${unregistered[@]}")

Add the missing \`mod\` lines (or main.rs). This check cannot be skipped with a
label. See ${POLICY}.

_EOF_
fi

if [[ ${#conditional[@]} -gt 0 ]]; then
  failed=1
  cat <<_EOF_
ERROR: The following contract main.rs files use \`cfg\` attributes:

$(printf '  %s\n' "${conditional[@]}")

Contract case files must not be conditionally compiled, otherwise a case can
silently stop being checked. Remove the \`cfg\` attributes. This check cannot be
skipped with a label. See ${POLICY}.

_EOF_
fi

# Check 3: every published proc-macro crate has contract tests.
uncovered=()
proc_macro_crates=()
while read -r manifest; do
  [[ -z "${manifest}" ]] && continue
  grep -qE '^[[:space:]]*proc[-_]macro[[:space:]]*=[[:space:]]*true' "${manifest}" || continue
  package=$(sed -n '/^\[package\]/,/^\[/p' "${manifest}")
  name=$(sed -nE 's/^name[[:space:]]*=[[:space:]]*"([^"]+)".*/\1/p' <<<"${package}" | head -1)
  if [[ -z "${name}" ]]; then
    uncovered+=("${manifest} (cannot find the package name)")
    continue
  fi
  if grep -qE '^publish[[:space:]]*=[[:space:]]*(false|\[[[:space:]]*\])' <<<"${package}"; then
    echo "Skipping check 3 for ${name}: it is not published."
    continue
  fi
  proc_macro_crates+=("${name}")
  slug="${name#google-cloud-}"
  slug="${slug//-/_}"
  has_cases=false
  for dir in src/*/tests/"${slug}"_contract/; do
    for f in "${dir}"*.rs; do
      if [[ "$(basename "${f}")" != "main.rs" ]]; then
        has_cases=true
      fi
    done
  done
  if [[ "${has_cases}" != "true" ]]; then
    uncovered+=("${name} (${manifest}): expected case files in src/*/tests/${slug}_contract/")
  fi
done < <(git ls-files -- ':(glob)**/Cargo.toml' | grep -v -E '(^|/)target/' || true)

if [[ ${#uncovered[@]} -gt 0 ]]; then
  failed=1
  cat <<_EOF_
ERROR: The following published proc-macro crates have no contract tests:

$(printf '  %s\n' "${uncovered[@]}")

cargo-semver-checks cannot analyze proc-macro crates. Add a contract test
directory with case files and a main.rs that registers them, or set
\`publish = false\` if the crate is not published. This check cannot be skipped
with a label. See ${POLICY}.

_EOF_
fi

if [[ "${failed}" -ne 0 ]]; then
  exit 1
fi
echo "Proc-macro contract tests pass all checks for: ${proc_macro_crates[*]:-<none>}."
