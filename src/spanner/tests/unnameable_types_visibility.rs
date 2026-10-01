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

//! Regression test verifying that previously unnameable public types
//! (`TransactionResult`, `TransactionRetryPolicy`, `ColumnIndex`, and `RowError`)
//! can be named, implemented, or bound from an external-crate perspective.

use google_cloud_spanner::Error;
use google_cloud_spanner::error::{ConvertError, RowError, SharedError, TlsError};
use google_cloud_spanner::model::CommitResponse;
use google_cloud_spanner::result::{
    ColumnIndex, RowError as ResultRowError, TransactionResult as ResultTransactionResult,
};
use google_cloud_spanner::retry_policy::{
    BasicTransactionRetryPolicy as RetryPolicyBasicTransactionRetryPolicy, RetryResult,
    TransactionRetryPolicy as RetryPolicyTransactionRetryPolicy,
};
use google_cloud_spanner::transaction::{
    BasicTransactionRetryPolicy, RetryResult as TransactionRetryResult, TransactionResult,
    TransactionRetryPolicy,
};
use google_cloud_spanner::types::TypeCode;
use google_cloud_spanner::value::{
    ConvertError as ValueConvertError, SharedError as ValueSharedError,
};
use std::io::Error as IoError;
use std::sync::Arc;
use std::time::Duration;
use wkt::Timestamp;

#[test]
fn external_can_name_and_construct_transaction_result() {
    let commit_timestamp = Timestamp::clamp(1_234_567_890, 500);
    let commit_response = CommitResponse::default().set_commit_timestamp(commit_timestamp);
    let transaction_result: TransactionResult<String> =
        TransactionResult::new("success".to_string(), commit_response.clone());

    assert_eq!(
        transaction_result.result, "success",
        "result field should match input"
    );
    assert_eq!(
        transaction_result.commit_response, commit_response,
        "commit_response field should match input"
    );
    assert_eq!(
        transaction_result.commit_timestamp(),
        Some(commit_timestamp),
        "commit_timestamp accessor should return expected timestamp"
    );
    assert!(
        transaction_result.commit_stats().is_none(),
        "commit_stats accessor should return None when not requested"
    );

    // Verify re-export via google_cloud_spanner::result
    let result_from_alias: ResultTransactionResult<String> = transaction_result.clone();
    assert_eq!(
        result_from_alias, transaction_result,
        "result_from_alias should equal transaction_result"
    );

    // Verify consuming accessors
    let (inner_result, inner_response) = transaction_result.clone().into_parts();
    assert_eq!(
        inner_result, "success",
        "into_parts result should match input"
    );
    assert_eq!(
        inner_response, commit_response,
        "into_parts response should match input"
    );
    assert_eq!(
        transaction_result.into_inner(),
        "success",
        "into_inner should return result value"
    );
}

#[derive(Debug)]
struct ExternalCustomRetryPolicy;

impl TransactionRetryPolicy for ExternalCustomRetryPolicy {
    fn on_abort(&self, error: Error, attempts: u32, _elapsed: Duration) -> RetryResult {
        if attempts < 2 {
            RetryResult::Continue(error)
        } else {
            RetryResult::Exhausted(error)
        }
    }
}

#[test]
fn external_can_implement_transaction_retry_policy() {
    let custom_policy = ExternalCustomRetryPolicy;
    let boxed_policy: Box<dyn TransactionRetryPolicy> = Box::new(custom_policy);

    // Verify Box<T> and Arc<T> blanket implementation for TransactionRetryPolicy
    let _wrapped_box: Box<dyn TransactionRetryPolicy> = Box::new(boxed_policy);
    let arc_policy: Arc<dyn TransactionRetryPolicy> = Arc::new(ExternalCustomRetryPolicy);
    let _wrapped_arc: Box<dyn TransactionRetryPolicy> = Box::new(arc_policy);

    // Verify re-exports via google_cloud_spanner::retry_policy and google_cloud_spanner::transaction
    let _basic_from_retry_policy = RetryPolicyBasicTransactionRetryPolicy::new();
    let _boxed_from_retry_policy: Box<dyn RetryPolicyTransactionRetryPolicy> =
        Box::new(BasicTransactionRetryPolicy::new());

    // Verify RetryResult can also be named from transaction module
    let _: fn(Error) -> TransactionRetryResult = TransactionRetryResult::Continue;
}

#[test]
fn external_can_use_column_index_as_bound() {
    fn accepts_column_index<I: ColumnIndex>(_index: I) -> bool {
        true
    }
    assert!(
        accepts_column_index("column_name"),
        "&str should satisfy ColumnIndex"
    );
    assert!(
        accepts_column_index("column_name".to_string()),
        "String should satisfy ColumnIndex"
    );
    let string_column = "column_name".to_string();
    assert!(
        accepts_column_index(&string_column),
        "&String should satisfy ColumnIndex"
    );
    assert!(
        accepts_column_index(0_usize),
        "usize should satisfy ColumnIndex"
    );
}

#[test]
fn external_can_name_and_match_row_error() {
    let column_error = RowError::ColumnNotFound("missing_col".to_string());
    assert_eq!(
        column_error.to_string(),
        "Could not find column: 'missing_col'",
        "RowError display format should match 'Could not find column: \\'missing_col\\''"
    );

    match &column_error {
        RowError::ColumnNotFound(name) => {
            assert_eq!(name, "missing_col", "column name should match");
        }
        RowError::IndexOutOfRange { index, len } => {
            panic!("unexpected IndexOutOfRange: index={index}, len={len}");
        }
        _ => panic!("unexpected error variant"),
    }

    let index_error = RowError::IndexOutOfRange { index: 3, len: 2 };
    match &index_error {
        RowError::IndexOutOfRange { index, len } => {
            assert_eq!(*index, 3, "index should match");
            assert_eq!(*len, 2, "len should match");
        }
        RowError::ColumnNotFound(name) => {
            panic!("unexpected ColumnNotFound: {name}");
        }
        _ => panic!("unexpected error variant"),
    }

    // Verify re-export via google_cloud_spanner::result::RowError
    let result_row_error: ResultRowError = column_error.clone();
    match result_row_error {
        ResultRowError::ColumnNotFound(name) => {
            assert_eq!(name, "missing_col", "column name should match");
        }
        _ => panic!("unexpected error variant"),
    }

    let type_conversion_error = RowError::TypeConversion {
        column: "col_a".to_string(),
        type_code: TypeCode::String,
        source: ConvertError::TypeMismatch {
            want: TypeCode::Int64,
            got: TypeCode::String,
        },
    };
    match &type_conversion_error {
        RowError::TypeConversion {
            column,
            type_code,
            source,
        } => {
            assert_eq!(column, "col_a", "column name should match");
            assert_eq!(*type_code, TypeCode::String, "type code should match");
            match source {
                ConvertError::TypeMismatch { want, got } => {
                    assert_eq!(*want, TypeCode::Int64, "expected want to match Int64");
                    assert_eq!(*got, TypeCode::String, "expected got to match String");
                }
                _ => panic!("unexpected inner convert error variant"),
            }
        }
        _ => panic!("unexpected error variant"),
    }
}

#[test]
fn external_can_extract_errors() {
    let row_error = RowError::ColumnNotFound("missing".to_string());
    let error = Error::deser(row_error.clone());
    let extracted_row = RowError::extract(&error).expect("should extract RowError");
    assert!(
        matches!(extracted_row, RowError::ColumnNotFound(name) if name == "missing"),
        "extracted RowError should match original"
    );

    let convert_error = ConvertError::NotNull;
    let error_from_convert = Error::deser(convert_error);
    let extracted_convert =
        ConvertError::extract(&error_from_convert).expect("should extract ConvertError");
    assert!(
        matches!(extracted_convert, ConvertError::NotNull),
        "extracted ConvertError should match NotNull"
    );

    let type_conversion_error = RowError::TypeConversion {
        column: "col_a".to_string(),
        type_code: TypeCode::String,
        source: ConvertError::TypeMismatch {
            want: TypeCode::Int64,
            got: TypeCode::String,
        },
    };
    let error_from_type_conversion = Error::deser(type_conversion_error.clone());
    let extracted_row_from_type_conversion =
        RowError::extract(&error_from_type_conversion).expect("should extract RowError");
    assert!(
        matches!(
            extracted_row_from_type_conversion,
            RowError::TypeConversion {
                column,
                type_code,
                source: ConvertError::TypeMismatch { want, got },
            } if column == "col_a" && *type_code == TypeCode::String && *want == TypeCode::Int64 && *got == TypeCode::String
        ),
        "extracted RowError should match original TypeConversion"
    );
    let extracted_convert_from_type_conversion =
        ConvertError::extract(&error_from_type_conversion).expect("should extract ConvertError");
    assert!(
        matches!(
            extracted_convert_from_type_conversion,
            ConvertError::TypeMismatch {
                want: TypeCode::Int64,
                got: TypeCode::String,
            }
        ),
        "extracted ConvertError should match TypeMismatch"
    );

    // Verify ConvertError re-export via google_cloud_spanner::value
    let _value_convert_error: ValueConvertError = ConvertError::NotNull;

    // Verify SharedError re-exports
    let _shared_error: SharedError = Arc::new(IoError::other("test"));
    let _value_shared_error: ValueSharedError = Arc::new(IoError::other("test"));

    // Verify TlsError re-export via google_cloud_spanner::error
    let _tls_error: TlsError = TlsError::MissingClientKey;
}
