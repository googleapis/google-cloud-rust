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

echo "==== Install protoc ===="
curl -fsSL --retry 5 --retry-delay 15 -o /tmp/protoc.zip https://github.com/protocolbuffers/protobuf/releases/download/v28.3/protoc-28.3-linux-x86_64.zip
sha256sum -c <(echo 0ad949f04a6a174da83cdcbdb36dee0a4925272a5b6d83f79a6bf9852076d53f  /tmp/protoc.zip)
env -C /usr/local unzip -x /tmp/protoc.zip
protoc --version

echo "==== Install go compiler ===="
curl -fsSL --retry 5 --retry-delay 15 https://go.dev/dl/go1.25.4.linux-amd64.tar.gz -o /tmp/go.tar.gz
sha256sum -c <(echo 9fa5ffeda4170de60f67f3aa0f824e426421ba724c21e133c1e35d6159ca1bec /tmp/go.tar.gz)
tar -C /usr/local -xzf /tmp/go.tar.gz
export PATH=${PATH}:/usr/local/go/bin

cargo version
rustup component add rustfmt
rustup show active-toolchain -v

echo "Regenerate all the code"
go run github.com/googleapis/librarian/cmd/librarian@main generate --all


# If there is any difference between the generated code and the
# committed code that is an error. All the inputs should be pinned,
# including the generator version and the googleapis SHA.
git diff --exit-code
