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

//! Custom errors for the Cloud Pub/Sub clients.
//!
//! The Pub/Sub clients define additional error types. These are often returned
//! as the `source()` of an [Error][crate::Error].

/// Represents an error that can occur when publishing a message.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum PublishError {
    /// Publish operation failed sending the RPC.
    #[error("the publish operation was interrupted by an error: {0}")]
    SendError(#[source] std::sync::Arc<crate::Error>),

    /// Publish is paused for the ordering key.
    ///
    /// A previous message with this ordering key has failed to send. To prevent messages from
    /// being sent out of order, the `Publisher` paused messages for this ordering key.
    ///
    /// To resume publishing messages with this ordering key, call `Publisher::resume_publish(...)`.
    #[error("the ordering key was paused")]
    OrderingKeyPaused(()),
}
