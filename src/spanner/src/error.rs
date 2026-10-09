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

use google_cloud_gax::error::rpc::{Code, Status, StatusDetails};
use std::error::Error;

pub use crate::from_value::{ConvertError, SharedError};
pub use crate::omni::TlsError;
pub use crate::row::RowError;
pub use wkt::{DurationError, TimestampError};

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
    /// Indicates that Spanner returned data in an unexpected or unparsable format.
    ///
    /// This represents an unexpected state and is an indication of an internal bug.
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

pub(crate) fn aborted_due_to_failed_initial_statement(details: Vec<StatusDetails>) -> crate::Error {
    crate::Error::service(
        Status::default()
            .set_code(Code::Aborted)
            .set_message("Aborted due to failed initial statement")
            .set_details(details),
    )
}

type BoxError = Box<dyn Error + Send + Sync>;

/// An error returned by an application from within a transaction runner closure.
///
/// # Example
///
/// ```
/// use google_cloud_spanner::transaction::ApplicationError;
///
/// #[derive(thiserror::Error, Debug, PartialEq)]
/// #[error("insufficient funds")]
/// struct InsufficientFunds;
///
/// let app_error = ApplicationError::new(InsufficientFunds);
/// assert!(app_error.downcast_ref::<InsufficientFunds>().is_some());
/// ```
///
/// It encapsulates an application/domain error and an optional underlying [`crate::Error`]
/// if the error was triggered by a Spanner statement failure.
///
/// When an `ApplicationError` that wraps an underlying Spanner [`Code::Aborted`] error is
/// returned from a transaction closure, [`TransactionRunner`][crate::transaction_runner::TransactionRunner]
/// recognizes the aborted statement and automatically retries the transaction.
/// If no Spanner statement failed, or if the underlying error was not aborted, the transaction
/// is immediately rolled back and the application error is returned.
#[derive(thiserror::Error, Debug)]
#[error("{domain_error}")]
pub struct ApplicationError {
    #[source]
    domain_error: BoxError,
    spanner_error: Option<crate::Error>,
}

impl ApplicationError {
    /// Creates an `ApplicationError` for a domain/business failure
    /// where no Spanner statement failed.
    pub fn new<E>(domain_error: E) -> Self
    where
        E: Into<BoxError>,
    {
        Self {
            domain_error: domain_error.into(),
            spanner_error: None,
        }
    }

    /// Creates an `ApplicationError` caused by an underlying Spanner statement failure.
    pub fn with_spanner_error<E>(domain_error: E, spanner_error: crate::Error) -> Self
    where
        E: Into<BoxError>,
    {
        Self {
            domain_error: domain_error.into(),
            spanner_error: Some(spanner_error),
        }
    }

    /// Returns a reference to the domain error.
    pub fn domain_error(&self) -> &(dyn Error + Send + Sync + 'static) {
        &*self.domain_error
    }

    /// Returns a reference to the underlying Spanner error, if one was attached.
    pub fn spanner_error(&self) -> Option<&crate::Error> {
        self.spanner_error.as_ref()
    }

    /// Returns a reference to the domain error downcast to `T`, if it matches.
    pub fn downcast_ref<T: Error + 'static>(&self) -> Option<&T> {
        self.domain_error.downcast_ref::<T>()
    }

    /// Extracts an `ApplicationError` from a [`crate::Error`], if present.
    pub fn extract(err: &crate::Error) -> Option<&Self> {
        const MAX_SOURCE_DEPTH: usize = 64;
        let mut current_source = err.source();
        let mut depth = 0;
        while let Some(source) = current_source {
            if let Some(app_error) = source.downcast_ref::<ApplicationError>() {
                return Some(app_error);
            }
            depth += 1;
            if depth >= MAX_SOURCE_DEPTH {
                break;
            }
            current_source = source.source();
        }
        None
    }
}

/// Returns the underlying Spanner [`crate::Error`] by unwrapping any nested [`ApplicationError`] wrappers,
/// bounded by a maximum depth to prevent infinite loops in the presence of pathological or cyclic error chains.
/// If `error` is not an [`ApplicationError`], or if it contains no underlying Spanner error, `error` is returned as-is.
pub(crate) fn underlying_spanner_error(mut error: &crate::Error) -> &crate::Error {
    const MAX_UNWRAP_DEPTH: usize = 16;
    for _ in 0..MAX_UNWRAP_DEPTH {
        let Some(spanner_error) =
            ApplicationError::extract(error).and_then(ApplicationError::spanner_error)
        else {
            break;
        };
        error = spanner_error;
    }
    error
}

impl From<ApplicationError> for crate::Error {
    fn from(err: ApplicationError) -> Self {
        if let Some(ref spanner_err) = err.spanner_error
            && let Some(status) = spanner_err.status().cloned()
        {
            let status_code = spanner_err.http_status_code();
            let headers = spanner_err.http_headers().cloned();
            return crate::Error::service_full(status, status_code, headers, Some(Box::new(err)));
        }
        crate::Error::deser(err)
    }
}

/// Helper to create a [`crate::Error`] from a pure domain error without an underlying Spanner failure.
///
/// # Example
///
/// ```
/// use google_cloud_spanner::transaction::application_error;
///
/// #[derive(thiserror::Error, Debug)]
/// #[error("account is locked")]
/// struct AccountLocked;
///
/// let error = application_error(AccountLocked);
/// ```
pub fn application_error<E>(domain_error: E) -> crate::Error
where
    E: Into<BoxError>,
{
    ApplicationError::new(domain_error).into()
}

/// Extension trait on [`crate::Result`] for convenient mapping to a Spanner application error.
///
/// # Example
///
/// ```
/// use google_cloud_spanner::transaction::SpannerResultExt;
///
/// #[derive(thiserror::Error, Debug)]
/// #[error("transfer failed")]
/// struct TransferFailed;
///
/// let spanner_result: Result<(), google_cloud_spanner::Error> = Ok(());
/// let mapped_result = spanner_result.map_app_err(TransferFailed);
/// assert!(mapped_result.is_ok());
/// ```
pub trait SpannerResultExt<T> {
    /// Maps an error returned by a Spanner operation into a Spanner [`ApplicationError`].
    ///
    /// Preserves the original Spanner error as the underlying cause, allowing the
    /// [`TransactionRunner`][crate::transaction_runner::TransactionRunner] to inspect it
    /// and automatically retry if the failure was caused by an aborted transaction.
    fn map_app_err<E: Into<BoxError>>(self, domain_error: E) -> Result<T, crate::Error>;
}

impl<T> SpannerResultExt<T> for Result<T, crate::Error> {
    fn map_app_err<E: Into<BoxError>>(self, domain_error: E) -> Result<T, crate::Error> {
        self.map_err(|spanner_error| {
            ApplicationError::with_spanner_error(domain_error, spanner_error).into()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use google_cloud_gax::error::rpc::Code;
    use static_assertions::assert_impl_all;
    use std::fmt::Debug;

    #[test]
    fn auto_traits() {
        assert_impl_all!(BatchUpdateError: Send, Sync, Debug);
        assert_impl_all!(SpannerInternalError: Send, Sync, Debug, Clone, PartialEq, Eq);
        assert_impl_all!(ApplicationError: Send, Sync, Debug);
    }

    #[derive(thiserror::Error, Debug, PartialEq, Eq)]
    #[error("insufficient funds in account")]
    struct InsufficientFundsError;

    #[test]
    fn application_error_pure_domain() {
        let error = application_error(InsufficientFundsError);
        let extracted = ApplicationError::extract(&error)
            .expect("should extract ApplicationError from crate::Error");

        assert!(
            extracted.spanner_error().is_none(),
            "pure domain error should not have an underlying Spanner error"
        );
        let domain_downcast = extracted
            .downcast_ref::<InsufficientFundsError>()
            .expect("domain error should downcast to InsufficientFundsError");
        assert_eq!(
            domain_downcast, &InsufficientFundsError,
            "downcasted error should match expected error"
        );
        assert_eq!(
            extracted.domain_error().to_string(),
            "insufficient funds in account",
            "domain error message should match"
        );
        let downcast_from_domain_error_ref = extracted
            .domain_error()
            .downcast_ref::<InsufficientFundsError>()
            .expect("domain error reference should downcast to InsufficientFundsError");
        assert_eq!(
            downcast_from_domain_error_ref, &InsufficientFundsError,
            "downcasted error from domain_error() reference should match expected error"
        );
    }

    #[test]
    fn application_error_with_underlying_spanner_error() {
        let status = Status::default()
            .set_code(Code::Aborted)
            .set_message("Transaction aborted due to conflict");
        let spanner_error = crate::Error::service(status);

        let error = crate::Error::from(ApplicationError::with_spanner_error(
            InsufficientFundsError,
            spanner_error,
        ));
        let extracted = ApplicationError::extract(&error)
            .expect("should extract ApplicationError from crate::Error");

        let spanner_err = extracted
            .spanner_error()
            .expect("underlying Spanner error should be present");
        let code = spanner_err
            .status()
            .expect("status should be populated on Spanner error")
            .code;
        assert_eq!(
            code,
            Code::Aborted,
            "Spanner error status code should be Aborted"
        );

        let domain_downcast = extracted
            .downcast_ref::<InsufficientFundsError>()
            .expect("domain error should downcast to InsufficientFundsError");
        assert_eq!(
            domain_downcast, &InsufficientFundsError,
            "downcasted error should match expected error"
        );
    }

    #[test]
    fn map_app_err_extension_trait() {
        let status = Status::default()
            .set_code(Code::NotFound)
            .set_message("Row not found");
        let spanner_result: Result<i64, crate::Error> = Err(crate::Error::service(status));

        let mapped_result = spanner_result.map_app_err(InsufficientFundsError);
        let mapped_error = mapped_result.expect_err("result should be an Err");

        let extracted = ApplicationError::extract(&mapped_error)
            .expect("should extract ApplicationError from mapped error");
        let spanner_err = extracted
            .spanner_error()
            .expect("underlying Spanner error should be present");
        assert_eq!(
            spanner_err
                .status()
                .expect("status should be populated on Spanner error")
                .code,
            Code::NotFound,
            "Spanner error status code should be NotFound"
        );

        let ok_result: Result<i64, crate::Error> = Ok(42);
        let mapped_ok = ok_result.map_app_err(InsufficientFundsError);
        assert_eq!(
            mapped_ok.expect("ok result should succeed"),
            42,
            "map_app_err should not modify Ok results"
        );
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

    #[test]
    fn underlying_spanner_error_pure_domain_error() {
        let pure_error = application_error(InsufficientFundsError);
        let unwrapped = underlying_spanner_error(&pure_error);
        assert!(
            unwrapped.status().is_none(),
            "pure application error should not have a Spanner status"
        );
    }

    #[test]
    fn underlying_spanner_error_nested_and_bounded() {
        let status = Status::default()
            .set_code(Code::Aborted)
            .set_message("Conflict");
        let root_spanner_error = crate::Error::service(status);

        let mut current_error = root_spanner_error;
        for _ in 0..20 {
            current_error = crate::Error::from(ApplicationError::with_spanner_error(
                InsufficientFundsError,
                current_error,
            ));
        }

        let unwrapped = underlying_spanner_error(&current_error);
        let extracted = ApplicationError::extract(unwrapped);
        assert!(
            extracted.is_some()
                || unwrapped
                    .status()
                    .is_some_and(|status| status.code == Code::Aborted),
            "unwrapping deeply nested error should terminate safely without looping indefinitely"
        );
    }
}
