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
curl -fsSL --retry 5 --retry-delay 15 https://github.com/protocolbuffers/protobuf/releases/download/v31.0/protobuf-31.0.tar.gz |
  tar -C /workspace/target/protobuf -zxf - --strip-components=1

curl -fsSL --retry 5 --retry-delay 15 -o /workspace/.bin/bazelisk https://github.com/bazelbuild/bazelisk/releases/download/v1.27.0/bazelisk-linux-amd64
chmod 755 /workspace/.bin/bazelisk

args=(
    "--enable_bzlmod"
    "--test_output=errors"
    "--verbose_failures=true"
    "--keep_going"
    "--experimental_convenience_symlinks=ignore"
    "--cache_test_results=$(should_cache_test_results)"
  )
if [[ -n "${BAZEL_REMOTE_CACHE:-}" ]]; then
    args+=("--remote_cache=${BAZEL_REMOTE_CACHE}")
    args+=("--google_default_credentials")
    # See https://docs.bazel.build/versions/main/remote-caching.html#known-issues
    # and https://github.com/bazelbuild/bazel/issues/3360
    args+=("--experimental_guard_against_concurrent_changes")
fi

env -C /workspace/target/protobuf USE_BAZEL_VERSION=8.2.1 \
    /workspace/.bin/bazelisk run "${args[@]}" -- \
    //conformance:conformance_test_runner \
    --failure_list /workspace/src/protojson-conformance/expected_failures.txt \
    /workspace/target/debug/protojson-conformance

echo "==== DONE ===="

/workspace/.bin/sccache --show-stats
