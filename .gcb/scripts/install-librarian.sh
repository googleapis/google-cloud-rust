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

set -euv

echo "==== Install go compiler ===="
curl -fsSL --retry 5 --retry-delay 15 https://go.dev/dl/go1.26.7.linux-amd64.tar.gz -o /tmp/go.tar.gz
sha256sum -c <(echo ffb5f8de10c62550dfddab66b36b57030721e0a44a3218e9e1181d7b59f121ca /tmp/go.tar.gz)
tar -C /usr/local -xzf /tmp/go.tar.gz
export PATH=${PATH}:/usr/local/go/bin

echo "Installing the tools to regenerate the code"
# Normally we recommend `librarian config get version` but that requires
# downloading two copies of librarian.
version=$(sed -n 's/^version: *//p' /workspace/librarian.yaml)
# Make multiple download attempts to avoid download-induced flakes.
go install github.com/googleapis/librarian/cmd/librarian@${version} ||
(sleep 5 && go install github.com/googleapis/librarian/cmd/librarian@${version}) ||
(sleep 10 && go install github.com/googleapis/librarian/cmd/librarian@${version})
# Make multiple attempts as installing the tools may require downloads.
go run github.com/googleapis/librarian/cmd/librarian@${version} install ||
(sleep 5 && go run github.com/googleapis/librarian/cmd/librarian@${version} install) ||
(sleep 10 && go run github.com/googleapis/librarian/cmd/librarian@${version} install)
