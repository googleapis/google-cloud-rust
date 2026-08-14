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

set -ev

source "$(dirname "$0")"/install-librarian.sh

echo "==== Install taplo ===="
cargo install taplo-cli --locked

cargo version
rustup component add rustfmt
rustup show active-toolchain -v

echo "Regenerate all the code"
go run github.com/googleapis/librarian/cmd/librarian@${version} generate --all

# If there is any difference between the generated code and the
# committed code that is an error. All the inputs should be pinned,
# including the generator version and the googleapis SHA.
git diff --exit-code
