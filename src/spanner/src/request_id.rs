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

//! Types and utilities for generating Spanner Request IDs (`x-goog-spanner-request-id`).
//!
//! Spanner Request IDs are structured strings sent in gRPC headers to uniquely identify
//! client instances, channels, requests, and retry attempts:
//! `<VERSION>.<RAND_PROCESS_ID>.<nthClientId>.<nthChannelId>.<nthRequest>.<attempt>`

use std::fmt::Write as _;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};

/// The Spanner Request ID protocol version.
const VERSION: &str = "1";

/// A 64-bit random value formatted as 16 lowercase hexadecimal characters (`"%016x"`),
/// generated once per process lifetime.
#[allow(dead_code)]
pub(crate) static RAND_PROCESS_ID: LazyLock<String> =
    LazyLock::new(|| format!("{:016x}", rand::random::<u64>()));

static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(1);

/// A generator for Spanner Request IDs (`x-goog-spanner-request-id`).
///
/// Each `RequestIdCreator` receives a unique `client_id` upon creation and pre-computes
/// the static prefix `<VERSION>.<RAND_PROCESS_ID>.<client_id>.` to minimize string formatting
/// overhead on RPC invocations.
#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct RequestIdCreator {
    client_id: u64,
    client_prefix: String,
    next_request_id: AtomicU64,
}

#[allow(dead_code)]
impl RequestIdCreator {
    /// Constructs a new `RequestIdCreator` with an atomically incremented client ID.
    ///
    /// We use `Ordering::Relaxed` because client and request IDs only require atomic uniqueness
    /// and monotonic increments across threads without synchronizing other memory accesses.
    pub(crate) fn new() -> Self {
        let client_id = NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed);
        let client_prefix = format!("{VERSION}.{}.{}.", *RAND_PROCESS_ID, client_id);
        Self {
            client_id,
            client_prefix,
            next_request_id: AtomicU64::new(1),
        }
    }

    /// Returns the unique client ID assigned to this generator.
    pub(crate) fn client_id(&self) -> u64 {
        self.client_id
    }

    /// Returns a base Request ID string for a new RPC, excluding the attempt suffix:
    /// `"1.<RAND_PROCESS_ID>.<client_id>.<channel_id>.<request_id>."`
    ///
    /// The returned string ends with a dot (`'.'`), ready for the attempt interceptor
    /// (`SpannerRequestIdInterceptor`) to append the 1-based attempt number on each retry.
    ///
    /// # Arguments
    /// * `channel_id` - The 1-based channel identifier (`1, 2, ...`), or `0` for an unknown channel.
    pub(crate) fn next_base_id(&self, channel_id: usize) -> String {
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let mut result = String::with_capacity(self.client_prefix.len() + 48);
        result.push_str(&self.client_prefix);
        let _ = write!(result, "{channel_id}.{request_id}.");
        result
    }

    /// Returns a full Request ID string for an RPC attempt:
    /// `"1.<RAND_PROCESS_ID>.<client_id>.<channel_id>.<request_id>.<attempt>"`
    ///
    /// # Arguments
    /// * `channel_id` - The 1-based channel identifier (`1, 2, ...`), or `0` for an unknown channel.
    /// * `attempt` - The 1-based attempt number (`1` for initial attempt, `2` for first retry).
    pub(crate) fn next_id(&self, channel_id: usize, attempt: u32) -> String {
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let mut result = String::with_capacity(self.client_prefix.len() + 48);
        result.push_str(&self.client_prefix);
        let _ = write!(result, "{channel_id}.{request_id}.{attempt}");
        result
    }

    /// Resets the internal request counter to `1`. Primarily intended for testing.
    pub(crate) fn reset(&self) {
        self.next_request_id.store(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn traits() {
        static_assertions::assert_impl_all!(RequestIdCreator: Send, Sync, std::fmt::Debug);
    }

    #[test]
    fn rand_process_id_format() {
        let process_id = &*RAND_PROCESS_ID;
        assert_eq!(process_id.len(), 16);
        assert!(
            process_id
                .chars()
                .all(|char| char.is_ascii_digit() || ('a'..='f').contains(&char)),
            "RAND_PROCESS_ID must be 16 lowercase hex characters, got {process_id}"
        );
    }

    #[test]
    fn request_id_creator_sequence() {
        let creator = RequestIdCreator::new();
        let client_id = creator.client_id();

        let base_id1 = creator.next_base_id(1);
        let expected_prefix = format!("{VERSION}.{}.{}.", *RAND_PROCESS_ID, client_id);
        assert_eq!(base_id1, format!("{expected_prefix}1.1."));

        let base_id2 = creator.next_base_id(1);
        assert_eq!(base_id2, format!("{expected_prefix}1.2."));

        let base_id3 = creator.next_base_id(2);
        assert_eq!(base_id3, format!("{expected_prefix}2.3."));
    }

    #[test]
    fn multiple_creators_unique_client_ids() {
        let creator1 = RequestIdCreator::new();
        let creator2 = RequestIdCreator::new();
        assert!(creator2.client_id() > creator1.client_id());
    }

    #[test]
    fn next_id_with_attempt() {
        let creator = RequestIdCreator::new();
        let client_id = creator.client_id();
        let expected_prefix = format!("{VERSION}.{}.{}.", *RAND_PROCESS_ID, client_id);

        let id1 = creator.next_id(1, 1);
        assert_eq!(id1, format!("{expected_prefix}1.1.1"));

        let id2 = creator.next_id(5, 42);
        assert_eq!(id2, format!("{expected_prefix}5.2.42"));
    }

    #[test]
    fn reset() {
        let creator = RequestIdCreator::new();
        let client_id = creator.client_id();
        let expected_prefix = format!("{VERSION}.{}.{}.", *RAND_PROCESS_ID, client_id);

        let _ = creator.next_base_id(1);
        let _ = creator.next_base_id(1);
        creator.reset();

        let base_id_after_reset = creator.next_base_id(1);
        assert_eq!(base_id_after_reset, format!("{expected_prefix}1.1."));
    }
}
