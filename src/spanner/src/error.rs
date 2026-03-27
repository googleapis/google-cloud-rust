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

use google_cloud_gax::error::rpc::Status;
use std::error::Error;

/// An unexpected error that occurs when the client receives data from Spanner
/// that it cannot properly parse or handle. This typically indicates a bug in
/// the client library or the Spanner service itself, though other causes are possible.
///
/// # Troubleshooting
///
/// This indicates a bug in the client, the service, or a message corrupted
/// while in transit. Please [open an issue] with as much detail as possible.
///
/// [open an issue]: https://github.com/googleapis/google-cloud-rust/issues/new/choose
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SpannerInternalError {
    #[error("unexpected data received from Spanner: {0}")]
    UnexpectedData(String),
}

impl SpannerInternalError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self::UnexpectedData(message.into())
    }
}

pub(crate) fn internal_error(message: impl Into<String>) -> crate::Error {
    crate::Error::deser(SpannerInternalError::new(message))
}

/// An error that occurs when an `execute_batch_update` partially succeeds.
///
/// It contains the update counts for each statement evaluated prior to the failure,
/// as well as the underlying error that caused the batch to fail.
///
/// Statements are executed serially in the order provided in the batch.
/// The `update_counts` correspond to the executed statements in the original
/// request based on their relative order. The statement that failed is the
/// one that follows directly after the last statement with an update count.
/// Execution stops at the first failed statement, and the remaining statements
/// are not executed.
#[derive(thiserror::Error, Debug)]
#[error("{status}")]
#[non_exhaustive]
pub struct BatchUpdateError {
    /// The number of rows modified by each successful statement before the failure.
    pub update_counts: Vec<i64>,
    /// The error that caused the batch to fail.
    #[source]
    pub status: crate::Error,
}

impl BatchUpdateError {
    /// Extracts a `BatchUpdateError` from a `google_cloud_spanner::Error`, if present.
    pub fn extract(err: &crate::Error) -> Option<&Self> {
        err.source()
            .and_then(|source| source.downcast_ref::<BatchUpdateError>())
    }

    pub(crate) fn build_error(update_counts: Vec<i64>, grpc_status: Status) -> crate::Error {
        let status = crate::Error::service(grpc_status.clone());
        let err = Self {
            update_counts,
            status,
        };
        crate::Error::service_full(grpc_status, None, None, Some(Box::new(err)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use google_cloud_gax::error::rpc::Code;
    use static_assertions::assert_impl_all;

    #[test]
    fn auto_traits() {
        assert_impl_all!(BatchUpdateError: Send, Sync, std::fmt::Debug);
    }

    #[test]
    fn extract_success() {
        let update_counts = vec![1, 2, 3];
        let grpc_status = Status::default()
            .set_code(Code::Aborted)
            .set_message("Batch failed");

        let err = BatchUpdateError::build_error(update_counts.clone(), grpc_status);

        let extracted = BatchUpdateError::extract(&err).expect("should extract BatchUpdateError");
        assert_eq!(extracted.update_counts, update_counts);
        assert_eq!(
            extracted
                .status
                .status()
                .expect("status should be populated")
                .code,
            Code::Aborted
        );
    }

    #[test]
    fn extract_failure() {
        let grpc_status = Status::default()
            .set_code(Code::Unknown)
            .set_message("Regular error");
        let err = crate::Error::service(grpc_status);

        let extracted = BatchUpdateError::extract(&err);
        assert!(
            extracted.is_none(),
            "should not extract BatchUpdateError from standard service error"
        );
    }
}
