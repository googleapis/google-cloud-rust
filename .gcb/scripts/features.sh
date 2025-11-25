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

rustup component add clippy
cargo version
rustup show active-toolchain -v

set -e

cargo clean
echo "==== google-cloud-wkt ===="
for sub in test doc; do
  cargo "${sub}" --package google-cloud-wkt --no-default-features
  cargo "${sub}" --package google-cloud-wkt --no-default-features --features chrono
  cargo "${sub}" --package google-cloud-wkt --no-default-features --features time
  cargo "${sub}" --package google-cloud-wkt --no-default-features --features _internal-semver
  cargo "${sub}" --package google-cloud-wkt --all-features
done
cargo clippy --no-deps --package google-cloud-wkt --all-targets -- --deny warnings
cargo clippy --no-deps --package google-cloud-wkt --all-features --all-targets --profile=test -- --deny warnings

cargo clean
echo "==== google-cloud-gax ===="
for sub in test doc; do
  cargo "${sub}" --package google-cloud-gax --no-default-features
  cargo "${sub}" --package google-cloud-gax --no-default-features --features unstable-stream
  cargo "${sub}" --package google-cloud-gax --no-default-features --features _internal-semver
  cargo "${sub}" --package google-cloud-gax --all-features
done
cargo clippy --no-deps --package google-cloud-gax --all-targets -- --deny warnings
cargo clippy --no-deps --package google-cloud-gax --all-features --all-targets --profile=test -- --deny warnings

cargo clean
echo "==== google-cloud-gax-internal ===="
for sub in test doc; do
  cargo "${sub}" --package google-cloud-gax-internal --no-default-features
  cargo "${sub}" --package google-cloud-gax-internal --no-default-features --features _internal-common
  cargo "${sub}" --package google-cloud-gax-internal --no-default-features --features _internal-http-client
  cargo "${sub}" --package google-cloud-gax-internal --no-default-features --features _internal-grpc-client
  cargo "${sub}" --package google-cloud-gax-internal --all-features
done
cargo clippy --no-deps --package google-cloud-gax-internal --all-targets -- --deny warnings
cargo clippy --no-deps --package google-cloud-gax-internal --all-features --all-targets --profile=test -- --deny warnings

cargo clean
echo "==== google-cloud-lro ===="
for sub in test doc; do
  cargo "${sub}" --profile=ci --package google-cloud-lro --no-default-features
  cargo "${sub}" --profile=ci --package google-cloud-lro --no-default-features --features unstable-stream
  cargo "${sub}" --profile=ci --package google-cloud-lro --no-default-features --features _internal-semver
  cargo "${sub}" --profile=ci --package google-cloud-lro --all-features
done
cargo clippy --no-deps --package google-cloud-lro --all-targets -- --deny warnings
cargo clippy --no-deps --package google-cloud-lro --all-features --all-targets --profile=test -- --deny warnings

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

cargo clean
echo "==== google-cloud-storage ===="
for sub in test doc; do
  cargo "${sub}" --profile=ci --package google-cloud-storage --no-default-features
  cargo "${sub}" --profile=ci --package google-cloud-storage --no-default-features --features unstable-stream
  cargo "${sub}" --profile=ci --package google-cloud-storage --all-features
done
cargo clippy --no-deps --package google-cloud-storage --all-targets -- --deny warnings
cargo clippy --no-deps --package google-cloud-storage --all-features --all-targets --profile=test -- --deny warnings

echo "==== DONE ===="

/workspace/.bin/sccache --show-stats
