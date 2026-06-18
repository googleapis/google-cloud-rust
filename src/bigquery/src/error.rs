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

//! Custom errors for the Cloud BigQuery query client.

use google_cloud_bigquery_v2::model::ErrorProto;
use google_cloud_gax::error::Error;

/// Errors that can occur during query configuration, execution, or polling.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum QueryError {
    /// The project ID was not provided or could not be determined.
    #[error("no project ID was provided")]
    MissingProjectId,

    /// Only query jobs are supported by this client.
    #[error("only query jobs are supported")]
    UnsupportedJobType,

    /// The query job failed on the BigQuery service side.
    /// Includes the list of error protocols returned by the service.
    #[error("query job failed: {errors:?}")]
    JobFailed {
        /// The list of all errors associated with the job.
        errors: Vec<ErrorProto>,
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

/// Errors that can occur when retrieving value cells from a [`Row`](crate::query::Row).
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum RowError {
    /// The requested column name or index was not found in the row.
    #[error("could not find column: {0}")]
    ColumnNotFound(String),

    /// The requested column index was out of range.
    #[error("column index out of range: {index} (expected < {len})")]
    IndexOutOfRange {
        /// The index that was requested.
        index: usize,
        /// The total number of columns in the row.
        len: usize,
    },

    /// Failed to convert/parse the cell value to the target type.
    #[error("type conversion error for column '{column}': {source}")]
    TypeConversion {
        /// The column identifier (name or index).
        column: String,
        /// The underlying parsing error.
        #[source]
        source: ConvertError,
    },

    /// The JSON format returned by the service did not match expectations.
    #[error("internal service JSON layout invalid: {0}")]
    InvalidRowFormat(String),

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

/// Represents failures when converting a raw BigQuery cell value (`wkt::Value`) to a Rust type.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum ConvertError {
    /// The value type did not match the expected type.
    #[error("type mismatch, expected {expected}, got {got:?}")]
    TypeMismatch {
        /// The expected type name.
        expected: &'static str,
        /// The actual value received.
        got: wkt::Value,
    },

    /// The value was null, but the target type does not support nulls (non-Option).
    #[error("expected non-null value, got null")]
    NotNull,

    /// An error occurred during custom conversion (e.g. parsing date/time strings).
    #[error("cannot convert value: {0}")]
    Convert(
        #[from]
        #[source]
        Box<dyn std::error::Error + Send + Sync + 'static>,
    ),
}

#[cfg(test)]
mod tests {
    use super::*;
    use google_cloud_gax::error::rpc::{Code, Status};

    #[test]
    fn test_job_failed_display() {
        let err = QueryError::JobFailed {
            errors: vec![
                ErrorProto::new()
                    .set_reason("invalidQuery")
                    .set_message("Syntax error: Unexpected end of input"),
            ],
        };
        assert!(err.to_string().contains("query job failed:"));
        assert!(err.to_string().contains("invalidQuery"));
        assert!(
            err.to_string()
                .contains("Syntax error: Unexpected end of input")
        );
    }

    #[test]
    fn test_rpc_display() {
        let status = Status::default()
            .set_code(Code::InvalidArgument)
            .set_message("simulated bad request");
        let err = QueryError::Rpc {
            source: Error::service(status),
        };
        assert_eq!(
            err.to_string(),
            "the operation failed. RPC error: the service reports an error with code INVALID_ARGUMENT described as: simulated bad request"
        );
    }

    #[test]
    fn test_row_error_display() {
        let err = RowError::ColumnNotFound("name".to_string());
        assert_eq!(err.to_string(), "could not find column: name");

        let err = RowError::IndexOutOfRange { index: 5, len: 3 };
        assert_eq!(
            err.to_string(),
            "column index out of range: 5 (expected < 3)"
        );

        let err = RowError::TypeConversion {
            column: "age".to_string(),
            source: ConvertError::NotNull,
        };
        assert_eq!(
            err.to_string(),
            "type conversion error for column 'age': expected non-null value, got null"
        );

        let err = RowError::InvalidRowFormat("missing f field".to_string());
        assert_eq!(
            err.to_string(),
            "internal service JSON layout invalid: missing f field"
        );

        let status = Status::default()
            .set_code(Code::Internal)
            .set_message("internal error");
        let err = RowError::Rpc {
            source: Error::service(status),
        };
        assert!(err.to_string().contains("the operation failed. RPC error:"));
    }

    #[test]
    fn test_convert_error_display() {
        let err = ConvertError::TypeMismatch {
            expected: "i64",
            got: wkt::Value::String("hello".to_string()),
        };
        assert_eq!(
            err.to_string(),
            "type mismatch, expected i64, got String(\"hello\")"
        );

        let err = ConvertError::NotNull;
        assert_eq!(err.to_string(), "expected non-null value, got null");

        let inner_err: Box<dyn std::error::Error + Send + Sync> = "invalid integer".into();
        let err = ConvertError::Convert(inner_err);
        assert_eq!(err.to_string(), "cannot convert value: invalid integer");
    }
}
