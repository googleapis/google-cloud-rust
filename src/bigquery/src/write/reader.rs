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

//! Helpers to reconnect a [ReadRows] stream.

use crate::builder::read::ReadRows;
use crate::client::Read;
use google_cloud_bigquery_v2::Error;
use google_cloud_gax::exponential_backoff::ExponentialBackoff;
use google_cloud_gax::retry_state::RetryState;
use google_cloud_gax::retry_policy::{Aip194Strict, RetryPolicy, RetryPolicyExt};


pub(crate) struct ReaderReconnectPolicy {
    pub backoff: ExponentialBackoff,
    pub max_times: u32,
}

/// State machine for reconnection logic. If reading fails at some point,
/// attempt to reconnect with the current row offset.
///
/// ```text
///                Start here
///                    │
///              ┌─────▼──────┐
///        ┌─────┼ Connecting ◄────────┐◄───────────────────┐
///        │     └─────┬──────┘        │                    │
///        │           │        Failed to read          Failed to
///   Exhausted        │         first message         read message
///    Retries         │      (retains retry state  (resets retry state)
///       or           │           from connecting)         ▲
/// Unrecoverable┌─────▼──────┐        ▲               ┌────┼────┐
///      Error   │ Connected  ┼────────┼───────────────► Reading │
///        │     └─────┬──────┘                        └────┬────┘
///        │           ▼                                    ▼
///        │      Unrecoverable                      Unrecoverable
///        │         Error                                Error
///        │           │            ┌────────────┐          │
///        │           └───────────►│            │          │
///        │                        │ Terminated │◄─────────┘
///        └───────────────────────►│            │
///                                 └────────────┘
/// ```
pub(crate) enum ReaderState {
    /// Connect or reconnect to the BigQuery read stream, with exponential backoff.
    Connecting(RetryState),
    /// Waiting for the first message in a BigQuery read stream.
    Connected(RetryState, google_cloud_gax::streaming::ResponseStream<
                crate::write::generated::gapic_storage::model::ReadRowsResponse,
            >),
    /// Waiting for the next message in a BigQuery read stream.
    Reading(google_cloud_gax::streaming::ResponseStream<
                crate::write::generated::gapic_storage::model::ReadRowsResponse,
            >),
    /// Stream completed cleanly, fatal error occurred, or consumer dropped the receiver.
    Terminated(Option<Error>),
}

pub struct Reader {
    policy: Box<dyn RetryPolicy>,
    state: ReaderState,
    request: ReadRows,  // holds the offset
}

impl Reader {
    pub(crate) fn new(request: ReadRows) -> Self {
        Self {
            policy: Box::new(Aip194Strict.with_attempt_limit(10)),
            state: ReaderState::Connecting(RetryState::new(false)),
            request,
        }
    }

    async fn connect(&mut self) {
        self.request.clone().send().await;
        if let ReaderState::Connecting(retry_state) = &mut self.state {
            retry_state.attempt_count += 1;
            let timeout = self.policy.;
        } else {
            // Something went wrong.
            self.state = ReaderState::Terminated(Error::fmt(&self, f));
        }
    }

    pub async fn next(&mut self) -> Option<Result<ReadRowsResponse, Error>> {
        // Get to Running state (if possible) and then return the first message.
        while true {
            match self.state {
                ReaderState::Connected(stream) => {
                    let message = stream.next().await;
                    if let Some(message) = message {
                        return message;
                    } else {
                        self.state = ReaderState::Terminated;
                    }
                },
                ReaderState::Connecting(retry_state) => {
                    self.connect().await;
                    self.state = ReaderState::Running(self.request.send().await);
                },
                ReaderState::BackingOff(status) => {
                    // self.retry_state.
                },
                ReaderState::Terminated => {
                    // No more messages to read.
                    return None;
                },
        }
    }
}
