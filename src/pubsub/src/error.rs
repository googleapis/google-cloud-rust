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

use crate::Error;
use std::sync::Arc;

/// Represents an error that can occur when publishing a message.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum PublishError {
    /// The underlying RPC failed.
    ///
    /// The inner error is wrapped in an [`Arc`] because the same error may
    /// affect multiple [`publish()`](crate::client::Publisher::publish) calls.
    #[error("the publish operation was interrupted by an error: {0}")]
    Rpc(#[source] Arc<Error>),

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
    /// Typically this can happen when the application is shutting down. Some background
    /// tasks in the client library may be terminated before they can send all the
    /// pending messages.
    #[error("the publisher has shut down")]
    Shutdown,
}

/// Represents an error that can occur when acking or nacking a message.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum AckError {
    /// The message's lease expired before the client could ack or nack it.
    ///
    /// The message has not been acked, and will be redelivered, maybe to
    /// another client.
    #[error("the message's lease has already expired. It was not acked, and will be redelivered.")]
    LeaseExpired,

    /// The underlying RPC failed.
    #[non_exhaustive]
    #[error("the operation failed. RPC error: {source}")]
    Rpc {
        /// The error returned by the service for the request.
        #[source]
        source: Arc<Error>,
    },

    /// Lease management shutdown before the client could acknowledge the
    /// message.
    ///
    /// The client did not acknowledge the message. The service will redeliver
    /// message.
    #[error(
        "shutdown before attempting the operation. \
         The message was not acknowledged, and will be redelivered."
    )]
    ShutdownBeforeAck,

    /// Error during shutdown.
    ///
    /// The result of the operation is unknown. If you attempted to ack
    /// the message, the service may or may not redeliver it.
    #[error("error during shutdown. The result of the operation is unknown. {0}")]
    Shutdown(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
}

#[cfg(test)]
mod tests {
    use super::*;
    use google_cloud_gax::error::rpc::{Code, Status};

    #[test]
    fn ack_error_rpc_debug() {
        let e = AckError::Rpc {
            source: Arc::new(Error::service(
                Status::default()
                    .set_code(Code::FailedPrecondition)
                    .set_message("inner fail"),
            )),
        };
        let fmt = format!("{e}");
        assert!(fmt.contains("operation failed."), "{fmt}");
        assert!(fmt.contains("inner fail"), "{fmt}");
    }

    impl PartialEq for AckError {
        fn eq(&self, other: &Self) -> bool {
            match (self, other) {
                (AckError::LeaseExpired, AckError::LeaseExpired) => true,
                (AckError::ShutdownBeforeAck, AckError::ShutdownBeforeAck) => true,
                (AckError::Rpc { source: s1 }, AckError::Rpc { source: s2 }) => {
                    format!("{:?}", s1) == format!("{:?}", s2)
                }
                (AckError::Shutdown(e1), AckError::Shutdown(e2)) => {
                    format!("{:?}", e1) == format!("{:?}", e2)
                }
                _ => false,
            }
        }
    }
}
