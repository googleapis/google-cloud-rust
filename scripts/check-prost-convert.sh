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

# gax-internal defines ToProto and FromProto, so a conflicting impl breaks its
# own build in the same change. Integration tests, examples, and benchmarks are
# not published. Unit tests in `#[cfg(test)]` modules are still checked.
#
# `git grep` exits with 0 on a match, 1 on no match, and >1 on errors. Capture
# the status so errors fail the check instead of passing silently.
status=0
git grep -n -E '^([^/]|/[^/])*(\.(to_proto|cnv)\(|ToProto::to_proto|FromProto::cnv)' -- '*.rs' \
    ':!src/gax-internal/**' ':!**/tests/**' ':!**/examples/**' ':!**/benches/**' || status=$?

if [[ "${status}" -eq 0 ]]; then
    echo "ERROR: Use fully qualified ToProto::<T>::to_proto(...) and FromProto::<T>::cnv(...) instead of .to_proto() or .cnv()" >&2
    exit 1
elif [[ "${status}" -ne 1 ]]; then
    echo "ERROR: git grep failed with exit code ${status}" >&2
    exit "${status}"
fi
