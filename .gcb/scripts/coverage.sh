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
rustup show active-toolchain -v

echo "==== Install cargo-llvm-cov ===="
cargo install --locked cargo-llvm-cov

cargo llvm-cov --all-features \
    --package google-cloud-auth \
    --package google-cloud-gax \
    --package google-cloud-gax-internal \
    --package google-cloud-lro \
    --package google-cloud-pubsub \
    --package google-cloud-storage \
    --package google-cloud-test-utils \
    --package google-cloud-wkt \
    --codecov --output-path codecov.json

upload=(
    # Get the codecov.io token from secret manager.
    --token "${CODECOV_TOKEN:-}"
    # We known where `cargo llvm-cov` outputs the coverage results, no need to
    # search for them.
    --file "codecov.json"
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
env -i HOME="${HOME}" /workspace/.bin/codecovcli --verbose upload-process "${upload[@]}"

# Notifies that all uploads are done.
env -i HOME="${HOME}" /workspace/.bin/codecovcli --verbose send-notifications "${notify[@]}"
