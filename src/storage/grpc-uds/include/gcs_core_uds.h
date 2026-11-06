// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef GOOGLE_CLOUD_RUST_STORAGE_GRPC_UDS_H_
#define GOOGLE_CLOUD_RUST_STORAGE_GRPC_UDS_H_

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Starts the `gcscore.v1.GcsCore` gRPC-over-UDS server in-process on a
// background Tokio runtime thread listening on `socket_path` (filesystem path
// or `@abstract` name on Linux), with default flags.
//
// Blocks until the UDS listener is bound and ready to accept connections.
// Returns `0` on success, or `-1` on error (see gcs_core_inproc_last_error).
int32_t gcs_core_start_inproc(const char* socket_path);

// Like gcs_core_start_inproc, but takes the same flags as `gcs-core-server`
// in `argv[0..argc)` (flags only, no program name), e.g.
// {"--socket", "@gcs-core-123", "--grpc-subchannels", "4"}. `--socket` is
// required, `--idle-secs` defaults to 0, and `--pdeathsig` / `--tcp` are
// rejected. Stop the server with gcs_core_stop_inproc and the `--socket` value.
// Returns `0` on success, or `-1` on error (see gcs_core_inproc_last_error).
int32_t gcs_core_start_inproc_with_args(int argc, const char* const* argv);

// Copies why the last gcs_core_start_inproc* call on this thread failed into
// `buf` (truncated to `cap - 1` bytes, NUL-terminated) and returns the full
// message length. A null `buf` or `cap == 0` only returns the length.
size_t gcs_core_inproc_last_error(char* buf, size_t cap);

// Triggers graceful shutdown of the in-process server listening on
// `socket_path` and blocks until its background Tokio runtime thread exits.
// Returns `0` on success, or `-1` if no server is registered for `socket_path`.
int32_t gcs_core_stop_inproc(const char* socket_path);

#ifdef __cplusplus
}  // extern "C"
#endif

#endif  // GOOGLE_CLOUD_RUST_STORAGE_GRPC_UDS_H_
