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

//! Defines types to query retry policies.

use std::time::Instant;

/// The input into a retry policy query.
///
/// On an error, the client library queries the retry policy as to whether it
/// should make a new attempt. The client library provides an instance of this
/// type to the retry policy.
///
/// This struct may gain new fields in future versions of the client libraries.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct RetryState {
    /// If true, the request is idempotent and it is safe to retry.
    ///
    /// Some policies retry non-idempotent operations because they are safe for
    /// a given configuration of the service or client.
    pub idempotent: bool,

    /// The start time for this retry loop.
    pub start: Instant,

    /// The number of times the request has been attempted.
    pub attempt_count: u32,
}

impl RetryState {
    /// Create a new instance.
    pub fn new(idempotent: bool) -> Self {
        Self::default().set_idempotent(idempotent)
    }

    /// Update the idempotency.
    pub fn set_idempotent(mut self, v: bool) -> Self {
        self.idempotent = v;
        self
    }

    /// Update the start time, useful in mocks.
    pub fn set_start<T: Into<Instant>>(mut self, v: T) -> Self {
        self.start = v.into();
        self
    }

    /// Update the attempt count, useful in mocks.
    pub fn set_attempt_count<T: Into<u32>>(mut self, v: T) -> Self {
        self.attempt_count = v.into();
        self
    }
}

impl std::default::Default for RetryState {
    fn default() -> Self {
        Self {
            start: Instant::now(),
            idempotent: false,
            attempt_count: 0,
        }
    }
}
