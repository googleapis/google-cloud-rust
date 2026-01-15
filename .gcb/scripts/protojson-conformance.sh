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

cargo version
rustup show active-toolchain -v

set -ev
cargo build -p protojson-conformance

mkdir -p /workspace/target/protobuf
curl -fsSL --retry 5 --retry-delay 15 https://github.com/protocolbuffers/protobuf/releases/download/v31.1/protobuf-31.1.tar.gz -o /tmp/protobuf.tar.gz
sha256sum -c <(echo 12bfd76d27b9ac3d65c00966901609e020481b9474ef75c7ff4601ac06fa0b82 /tmp/protobuf.tar.gz)
tar -C /workspace/target/protobuf -zxf /tmp/protobuf.tar.gz --strip-components=1

curl -fsSL --retry 5 --retry-delay 15 -o /workspace/.bin/bazelisk https://github.com/bazelbuild/bazelisk/releases/download/v1.27.0/bazelisk-linux-amd64
sha256sum -c <(echo e1508323f347ad1465a887bc5d2bfb91cffc232d11e8e997b623227c6b32fb76 /workspace/.bin/bazelisk)
chmod 755 /workspace/.bin/bazelisk

args=(
    "--enable_bzlmod"
    "--test_output=errors"
    "--verbose_failures=true"
    "--keep_going"
    "--experimental_convenience_symlinks=ignore"
  )
if [[ -n "${BAZEL_REMOTE_CACHE:-}" ]]; then
    args+=("--remote_cache=${BAZEL_REMOTE_CACHE}")
    args+=("--google_default_credentials")
    # See https://docs.bazel.build/versions/main/remote-caching.html#known-issues
    # and https://github.com/bazelbuild/bazel/issues/3360
    args+=("--experimental_guard_against_concurrent_changes")
fi

export USE_BAZEL_VERSION=8.2.1

# First run `bazelisk version` to deflake any problems downloading the toolchain.
(
   cd /workspace/target/protobuf
   /workspace/.bin/bazelisk version || \
   /workspace/.bin/bazelisk version || \
   /workspace/.bin/bazelisk version
)

# Then run `bazelisk fetch` to deflake any problems downloading the dependencies.
(
    cd /workspace/target/protobuf
    /workspace/.bin/bazelisk fetch "${args[@]}" //conformance:conformance_test_runner || \
    /workspace/.bin/bazelisk fetch "${args[@]}" //conformance:conformance_test_runner || \
    /workspace/.bin/bazelisk fetch "${args[@]}" //conformance:conformance_test_runner
)

# Now we are ready to build and run the tests with fewer flakes.
env -C /workspace/target/protobuf \
    /workspace/.bin/bazelisk run "${args[@]}" -- \
    //conformance:conformance_test_runner \
    --failure_list /workspace/tests/protojson-conformance/expected_failures.txt \
    /workspace/target/debug/protojson-conformance

echo "==== DONE ===="

/workspace/.bin/sccache --show-stats
