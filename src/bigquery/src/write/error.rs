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

use crate::Error;
use crate::model::{RowError, StorageError};
use google_cloud_gax::error::rpc::Status;

/// Represents an error that can occur when appending rows.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum AppendError {
    /// The underlying RPC failed.
    #[non_exhaustive]
    #[error("the operation failed. RPC error: {source}")]
    Rpc {
        /// The error returned by the service for the request.
        #[from]
        #[source]
        source: Error,
    },

    /// Certain rows have errors.
    #[non_exhaustive]
    #[error(
        "there was an error for the following rows. No rows in the batch were appended. You can remove the bad rows and retry the request. Status: {status:?}, Rows: {row_errors:?}"
    )]
    RowErrors {
        /// The status returned by the service for the request.
        status: Status,
        /// The row-level errors reported by the service.
        row_errors: Vec<RowError>,
    },

    /// The `AppendRows` stream closed unexpectedly.
    #[error(
        "the `AppendRows` stream closed unexpectedly and the client library could not recover."
    )]
    UnexpectedEndOfStream,
}

pub(crate) type AppendResult<T> = std::result::Result<T, AppendError>;

/// Represents an error that can occur when committing a pending write stream.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum CommitError {
    /// The underlying RPC failed.
    #[non_exhaustive]
    #[error("the operation failed. RPC error: {source}")]
    Rpc {
        /// The error returned by the service for the request.
        #[from]
        #[source]
        source: Error,
    },

    /// The stream could not be committed.
    #[non_exhaustive]
    #[error(
        "the service failed to commit the stream. No rows in the stream were committed. Stream errors: {stream_errors:?}"
    )]
    FailedTransaction {
        /// The stream-level errors reported by the service.
        stream_errors: Vec<StorageError>,
    },
}

/// Represents an error that can occur when building a writer.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum WriterBuilderError {
    /// The stream type provided by the service did not match the expected type.
    #[error("stream type mismatch: requested {expected:?}, but matched resource yields {actual:?}")]
    TypeMismatch {
        /// The expected stream type.
        expected: crate::model::write_stream::Type,
        /// The actual stream type returned by the service.
        actual: crate::model::write_stream::Type,
    },

    /// The underlying RPC failed.
    #[non_exhaustive]
    #[error("the operation failed. RPC error: {source}")]
    Rpc {
        /// The error returned by the service for the request.
        #[from]
        #[source]
        source: Error,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::storage_error::StorageErrorCode;
    use google_cloud_gax::error::rpc::{Code, Status};

    #[test]
    fn append_error_rpc_debug() {
        let e = AppendError::Rpc {
            source: Error::service(
                Status::default()
                    .set_code(Code::FailedPrecondition)
                    .set_message("inner fail"),
            ),
        };
        let fmt = format!("{e}");
        assert!(fmt.contains("operation failed."), "{fmt}");
        assert!(fmt.contains("inner fail"), "{fmt}");
    }

    #[test]
    fn commit_error_display() {
        let e = CommitError::Rpc {
            source: Error::service(
                Status::default()
                    .set_code(Code::Unavailable)
                    .set_message("inner fail"),
            ),
        };
        let fmt = format!("{e}");
        assert!(fmt.contains("operation failed."), "{fmt}");
        assert!(fmt.contains("inner fail"), "{fmt}");

        let e = CommitError::FailedTransaction {
            stream_errors: vec![
                StorageError::new()
                    .set_code(StorageErrorCode::InvalidStreamState)
                    .set_entity("projects/p/datasets/d/tables/t/streams/s")
                    .set_error_message("stream not finalized"),
            ],
        };
        let fmt = format!("{e}");
        assert!(fmt.contains("failed to commit the stream"), "{fmt}");
        assert!(fmt.contains("stream not finalized"), "{fmt}");
    }
}
