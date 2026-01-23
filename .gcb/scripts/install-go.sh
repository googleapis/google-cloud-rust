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

URL="https://go.dev/dl/go{_GO_VERSION}.linux-amd64.tar.gz"
curl -fsSL --retry 5 --retry-delay 15 "${URL}" -o /tmp/go.tar.gz
sha256sum -c <(echo "${_GO_SHA256}" /tmp/go.tar.gz)
tar -C /usr/local -zxf /tmp/go.tar.gz --strip-components=1
rm -f /tmp/go.tar.gz

echo ${PATH}:/usr/local/go/bin
