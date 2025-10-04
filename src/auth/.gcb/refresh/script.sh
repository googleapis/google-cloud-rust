#!/bin/bash
#
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

set -euo pipefail

echo "Cloning the repo..."
git clone https://github.com/googleapis/google-cloud-rust.git --depth 1
cd google-cloud-rust/src/auth/.gcb/builds

echo "Rerunning terraform for rust-auth-testing..."
terraform init
terraform plan -out /tmp/auth.plan
terraform apply /tmp/auth.plan
