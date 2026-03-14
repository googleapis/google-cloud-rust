// Copyright 2025 Google LLC
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

//! Shared constants for the Google Cloud Storage Rust client implementation.
//!
//! This module centralizes operational defaults used across
//! `storage/` internals to improve readability and maintainability.
//!
//! Current usage map:
//! - [`DEFAULT_BIDI_ATTEMPT_TIMEOUT_SECS`]
//!   - `storage/request_options.rs` default per-attempt timeout.
//! - [`BIDI_REQUEST_CHANNEL_CAPACITY`]
//!   - `storage/bidi/connector.rs` internal request stream channel.
//!   - `storage/bidi/transport.rs` worker/read-range channels.
//! - [`BIDI_REQUEST_RECV_MANY_BATCH_SIZE`]
//!   - `storage/bidi/worker.rs` batching for incoming read-range requests.
//! - [`BIDI_CONNECT_PROGRESS_LOG_INTERVAL_SECS`]
//!   - `storage/bidi/connector.rs` progress log cadence while connect attempts wait.

/// Default per-attempt timeout (seconds) for bidi open/reconnect attempts.
pub const DEFAULT_BIDI_ATTEMPT_TIMEOUT_SECS: u64 = 60;

/// Channel capacity for bidi request fan-out between workers and range readers.
pub const BIDI_REQUEST_CHANNEL_CAPACITY: usize = 100;

/// Batch size used by worker `recv_many()` when draining new range requests.
pub const BIDI_REQUEST_RECV_MANY_BATCH_SIZE: usize = 16;

/// Log interval (seconds) for "still waiting" progress messages during
/// bidi connect/reconnect attempts.
pub const BIDI_CONNECT_PROGRESS_LOG_INTERVAL_SECS: u64 = 5;
