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

set -euv

cargo version
rustup component add rustfmt
rustup show active-toolchain -v

echo "==== Install cargo-tarpaulin ===="
cargo install cargo-tarpaulin --version 0.32.1 --locked

cargo tarpaulin --out xml

upload=(
    # Get the codecov.io token from secret manager.
    --token "${CODECOV_TOKEN:-}"
    # We known where `cargo tarpaulin` outputs the coverage results, no need to
    # search for them.
    --file "cobertura.xml"
    --disable-search
    # Exit with an error if the upload fails, we can return the build in that
    # case.
    --fail-on-error
    # Provide some basic information
    --git-service github
)
notify=(
    # Get the codecov.io token from secret manager.
    --token "${CODECOV_TOKEN:-}"
)
# The commit SHA is required, use the GCB variable if set, otherwise try using
# git to get the current SHA.
if [[ -n "${COMMIT_SHA:-}" ]]; then
    upload+=(--commit-sha "${COMMIT_SHA}")
    notify+=(--commit-sha "${COMMIT_SHA}")
else
    upload+=(--commit-sha "$(git rev-parse HEAD)")
    notify+=(--commit-sha "$(git rev-parse HEAD)")
fi
# Set the PR number from the GCB variable.
if [[ -n "${_PR_NUMBER:-}" ]]; then
    upload+=(--pr "${_PR_NUMBER}")
fi
# Set the branch name from the GCB variable.
if [[ -n "${BRANCH_NAME:-}" ]]; then
    upload+=(--branch "${BRANCH_NAME}")
fi

# Uploads the code coverage results
echo "Invoking codecovcli with ${upload[@]}" | sed 's/--token.*--/--token [censored] --/'
env -i HOME="${HOME}" /workspace/.bin/codecovcli --verbose upload-process "${upload[@]}"

# Notifies that all uploads are done.
env -i HOME="${HOME}" /workspace/.bin/codecovcli --verbose send-notifications "${notify[@]}"
