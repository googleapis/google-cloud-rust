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

//! Custom errors for the Cloud Pub/Sub clients.

/// Represents an error that can occur when publishing a message.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum PublishError {
    /// The underlying RPC call failed.
    ///
    /// The inner error is wrapped in an [`Arc`](std::sync::Arc) to allow this error to be cloned
    /// and returned for each message in the batch.
    #[error("the publish operation was interrupted by an error: {0}")]
    Rpc(#[source] std::sync::Arc<crate::Error>),

    /// Publishing is paused because a previous message with the same ordering key failed.
    ///
    /// To prevent messages from being sent out of order, the [`Publisher`](crate::client::Publisher)
    /// paused messages for the ordering key.
    ///
    /// To resume publishing, call [`Publisher::resume_publish`](crate::client::Publisher::resume_publish).
    #[error("publishing is paused for the ordering key")]
    OrderingKeyPaused,

    /// The operation failed because the [`Publisher`](crate::client::Publisher) has
    /// been shut down.
    ///
    /// This may occur when the runtime has dropped the background tasks that handle
    /// message publishing. It is possible that the message was successfully published
    /// before shutdown.
    #[error("the publisher has shut down")]
    Shutdown,

    /// The publish message size exceeds the batch configured byte threshold.
    #[error("message size exceeded configured byte threshold")]
    ExceededByteThresholdError(()),
}
