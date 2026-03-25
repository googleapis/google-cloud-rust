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

use crate::client::Statement;
use crate::error::{BatchUpdateError, internal_error};
use crate::model::result_set_stats::RowCount;
use crate::model::{ExecuteBatchDmlResponse, RequestOptions};
use google_cloud_gax::error::rpc::Code;
use google_cloud_gax::error::rpc::Status as RpcStatus;

/// A builder for [BatchDml].
#[derive(Clone, Default, Debug)]
pub struct BatchDmlBuilder {
    statements: Vec<Statement>,
    request_options: Option<RequestOptions>,
}

impl BatchDmlBuilder {
    /// Creates a new empty BatchDmlBuilder.
    pub fn new() -> Self {
        BatchDmlBuilder::default()
    }

    /// Adds a statement to the batch.
    pub fn add_statement(mut self, statement: impl Into<Statement>) -> Self {
        self.statements.push(statement.into());
        self
    }

    /// Specifies the request options for this batch calculation.
    pub fn with_request_options(mut self, request_options: RequestOptions) -> Self {
        self.request_options = Some(request_options);
        self
    }

    /// Builds and returns the finalized BatchDml object.
    pub fn build(self) -> BatchDml {
        BatchDml {
            statements: self.statements,
            request_options: self.request_options,
        }
    }
}

/// A batch of DML statements to be executed in a single round-trip to Spanner.
#[derive(Clone, Debug)]
pub struct BatchDml {
    pub(crate) statements: Vec<Statement>,
    pub(crate) request_options: Option<RequestOptions>,
}

impl BatchDml {
    pub fn builder() -> BatchDmlBuilder {
        BatchDmlBuilder::new()
    }
}

/// Processes an ExecuteBatchDmlResponse and returns the success counts, or an error.
pub(crate) fn process_response(response: ExecuteBatchDmlResponse) -> crate::Result<Vec<i64>> {
    let mut update_counts = Vec::with_capacity(response.result_sets.len());
    for result_set in response.result_sets {
        let exact_count = result_set
            .stats
            .ok_or_else(|| internal_error("No stats returned for a successful statement"))
            .and_then(|stats| match stats.row_count {
                Some(RowCount::RowCountExact(c)) => Ok(c),
                _ => Err(internal_error(
                    "ExecuteBatchDml returned an invalid or missing row count type",
                )),
            });

        update_counts.push(exact_count?);
    }

    // If a non-zero status is present, it halted the batch somewhere in the middle of the batch.
    if let Some(status) = response.status.filter(|s| s.code != Code::Ok as i32) {
        let grpc_status = RpcStatus::default()
            .set_code(status.code)
            .set_message(status.message);

        // If the error code is Aborted, then we propagate a 'normal' service error.
        // The TransactionRunner will then retry the transaction.
        if status.code == Code::Aborted as i32 {
            return Err(crate::Error::service(grpc_status));
        }
        return Err(BatchUpdateError::build_error(update_counts, grpc_status));
    }

    Ok(update_counts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ResultSet, ResultSetStats};
    use static_assertions::assert_impl_all;

    #[test]
    fn auto_traits() {
        assert_impl_all!(BatchDml: Send, Sync, Clone, std::fmt::Debug);
        assert_impl_all!(BatchDmlBuilder: Send, Sync, Clone, std::fmt::Debug);
    }

    #[test]
    fn builder() {
        let stmt1 = Statement::builder("UPDATE t SET c = 1 WHERE id = 1").build();
        let stmt2 = Statement::builder("UPDATE t SET c = 2 WHERE id = 2").build();

        let batch = BatchDml::builder()
            .add_statement(stmt1)
            .add_statement(stmt2)
            .build();

        assert_eq!(batch.statements.len(), 2);
        assert_eq!(batch.statements[0].sql, "UPDATE t SET c = 1 WHERE id = 1");
        assert_eq!(batch.statements[1].sql, "UPDATE t SET c = 2 WHERE id = 2");
        assert!(batch.request_options.is_none());
    }

    #[test]
    fn builder_with_request_options() {
        let stmt = Statement::builder("UPDATE t SET c = 1 WHERE id = 1").build();
        let req_opts = RequestOptions::new().set_request_tag("tag1");

        let batch = BatchDml::builder()
            .add_statement(stmt)
            .with_request_options(req_opts)
            .build();

        assert_eq!(batch.statements.len(), 1);
        assert_eq!(
            batch
                .request_options
                .expect("request options missing")
                .request_tag,
            "tag1"
        );
    }

    #[test]
    fn process_response_success() {
        let stats1 = ResultSetStats {
            row_count: Some(RowCount::RowCountExact(5)),
            ..Default::default()
        };
        let stats2 = ResultSetStats {
            row_count: Some(RowCount::RowCountExact(10)),
            ..Default::default()
        };

        let rs1 = ResultSet {
            stats: Some(stats1),
            ..Default::default()
        };
        let rs2 = ResultSet {
            stats: Some(stats2),
            ..Default::default()
        };

        let response = ExecuteBatchDmlResponse {
            result_sets: vec![rs1, rs2],
            status: None,
            ..Default::default()
        };

        let result = process_response(response);
        let counts = result.expect("process_response should succeed");
        assert_eq!(counts, vec![5, 10]);
    }

    #[test]
    fn process_response_grpc_error() {
        let stats = ResultSetStats {
            row_count: Some(RowCount::RowCountExact(3)),
            ..Default::default()
        };
        let rs = ResultSet {
            stats: Some(stats),
            ..Default::default()
        };

        // Note: crate::model::Status is the common type for status embedded in generated grpc responses.
        let err_status = google_cloud_rpc::model::Status::default()
            .set_code(3) // INVALID_ARGUMENT
            .set_message("Bad query");

        let response = ExecuteBatchDmlResponse {
            result_sets: vec![rs],
            status: Some(err_status),
            ..Default::default()
        };

        let result = process_response(response);
        let err = result.expect_err("should return error");
        let batch_err = BatchUpdateError::extract(&err).expect("should extract BatchUpdateError");

        assert_eq!(batch_err.update_counts, vec![3]);
        assert_eq!(batch_err.status.status().expect("status").code, 3.into());
        assert_eq!(
            batch_err.status.status().expect("status").message,
            "Bad query"
        );
    }

    #[test]
    fn process_response_aborted() {
        let stats = ResultSetStats {
            row_count: Some(RowCount::RowCountExact(3)),
            ..Default::default()
        };
        let rs = ResultSet {
            stats: Some(stats),
            ..Default::default()
        };

        let err_status = google_cloud_rpc::model::Status::default()
            .set_code(google_cloud_gax::error::rpc::Code::Aborted as i32)
            .set_message("transaction aborted");

        let response = ExecuteBatchDmlResponse {
            result_sets: vec![rs],
            status: Some(err_status),
            ..Default::default()
        };

        let result = process_response(response);
        let err = result.expect_err("should return error");
        let batch_err = BatchUpdateError::extract(&err);
        assert!(batch_err.is_none());
        assert_eq!(
            err.status().expect("status").code,
            google_cloud_gax::error::rpc::Code::Aborted
        );
        assert_eq!(err.status().expect("status").message, "transaction aborted");
    }

    #[test]
    fn process_response_missing_stats() {
        let rs = ResultSet {
            stats: None,
            ..Default::default()
        };

        let response = ExecuteBatchDmlResponse {
            result_sets: vec![rs],
            ..Default::default()
        };

        let result = process_response(response);
        let err = result.expect_err("should fail");
        assert!(err.to_string().contains("No stats returned"));
    }

    #[test]
    fn process_response_missing_row_count_type() {
        let stats = ResultSetStats {
            row_count: None,
            ..Default::default()
        };

        let rs = ResultSet {
            stats: Some(stats),
            ..Default::default()
        };

        let response = ExecuteBatchDmlResponse {
            result_sets: vec![rs],
            ..Default::default()
        };

        let result = process_response(response);
        let err = result.expect_err("should fail");
        assert!(
            err.to_string()
                .contains("invalid or missing row count type")
        );
    }
}
