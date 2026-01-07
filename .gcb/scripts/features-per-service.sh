#!/usr/bin/env bash
# Copyright 20256 Google LLC
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

rustup component add clippy
cargo version
rustup show active-toolchain -v

set -e

# We use google-cloud-aiplatform-v1 to test the generator w.r.t.
# per-client features. As usual, we assume the generator works for
# other generated libraries if it works for one.
cargo clean
echo "==== google-cloud-aiplatform-v1 ===="
cargo build -p google-cloud-aiplatform-v1 --no-default-features
mapfile -t features < <(sed -n -e '/^default = \[/,/^\]/ p' src/generated/cloud/aiplatform/v1/Cargo.toml | sed -n -e '/",/ s/ *"\(.*\)",/\1/p')
for feature in "${features[@]}"; do
  echo "==== google-cloud-aiplatform-v1 + ${feature} ===="
  cargo build --profile=ci -p google-cloud-aiplatform-v1 --no-default-features --features "${feature}"
done

echo "==== DONE ===="

/workspace/.bin/sccache --show-stats
