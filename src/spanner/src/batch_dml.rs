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

use crate::error::{BatchUpdateError, internal_error};
use crate::model::request_options::Priority;
use crate::model::result_set_stats::RowCount;
use crate::model::{ExecuteBatchDmlResponse, RequestOptions, ResultSet};
use crate::statement::Statement;
use crate::{Error, Result};
use google_cloud_gax::backoff_policy::BackoffPolicyArg;
use google_cloud_gax::error::rpc::Code;
use google_cloud_gax::error::rpc::Status as RpcStatus;
use google_cloud_gax::options::RequestOptions as GaxRequestOptions;
use google_cloud_gax::retry_policy::RetryPolicyArg;
use std::time::Duration;

/// A builder for [BatchDml].
#[derive(Clone, Default, Debug)]
pub struct BatchDmlBuilder {
    statements: Vec<Statement>,
    request_options: Option<RequestOptions>,
    last_statements: bool,
    gax_options: GaxRequestOptions,
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

    /// Sets the request tag for this batch.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::statement::Statement;
    /// # use google_cloud_spanner::batch::BatchDml;
    /// let statement1 = Statement::builder("UPDATE users SET active = true WHERE id = 1").build();
    /// let batch = BatchDml::builder()
    ///     .add_statement(statement1)
    ///     .set_request_tag("my-tag")
    ///     .build();
    /// ```
    ///
    /// See also: [Troubleshooting with tags](https://docs.cloud.google.com/spanner/docs/introspection/troubleshooting-with-tags)
    pub fn set_request_tag(mut self, tag: impl Into<String>) -> Self {
        self.request_options
            .get_or_insert_with(RequestOptions::default)
            .request_tag = tag.into();
        self
    }

    /// Sets the RPC priority to use for this batch DML request.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::statement::Statement;
    /// # use google_cloud_spanner::batch::BatchDml;
    /// # use google_cloud_spanner::model::request_options::Priority;
    /// let statement1 = Statement::builder("UPDATE Users SET Active = true WHERE Id = 1").build();
    /// let batch = BatchDml::builder()
    ///     .add_statement(statement1)
    ///     .set_priority(Priority::Low)
    ///     .build();
    /// ```
    ///
    /// If not specified, the default priority is `Priority::High`.
    pub fn set_priority(mut self, priority: Priority) -> Self {
        self.request_options
            .get_or_insert_with(RequestOptions::default)
            .priority = priority;
        self
    }

    /// Sets the per-attempt timeout for this batch DML request.
    pub fn with_attempt_timeout(mut self, timeout: Duration) -> Self {
        self.gax_options.set_attempt_timeout(timeout);
        self
    }

    /// Sets the retry policy for this batch DML request.
    pub fn with_retry_policy(mut self, policy: impl Into<RetryPolicyArg>) -> Self {
        self.gax_options.set_retry_policy(policy);
        self
    }

    /// Sets the backoff policy for this batch DML request.
    pub fn with_backoff_policy(mut self, policy: impl Into<BackoffPolicyArg>) -> Self {
        self.gax_options.set_backoff_policy(policy);
        self
    }

    /// Sets whether this batch is the last statement in a read/write transaction.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::statement::Statement;
    /// # use google_cloud_spanner::batch::BatchDml;
    /// let statement1 = Statement::builder("UPDATE users SET active = true WHERE id = 1").build();
    /// let batch = BatchDml::builder()
    ///     .add_statement(statement1)
    ///     .set_last_statements(true)
    ///     .build();
    /// ```
    ///
    /// If true, indicates that this request marks the end of the transaction, which
    /// allows Spanner to optimize execution.
    pub fn set_last_statements(mut self, last_statements: bool) -> Self {
        self.last_statements = last_statements;
        self
    }

    /// Builds and returns the finalized BatchDml object.
    pub fn build(self) -> BatchDml {
        BatchDml {
            statements: self.statements,
            request_options: self.request_options,
            last_statements: self.last_statements,
            gax_options: self.gax_options,
        }
    }
}

/// A batch of DML statements to be executed in a single round-trip to Spanner.
#[derive(Clone, Debug)]
pub struct BatchDml {
    pub(crate) statements: Vec<Statement>,
    pub(crate) request_options: Option<RequestOptions>,
    pub(crate) last_statements: bool,
    pub(crate) gax_options: GaxRequestOptions,
}

impl BatchDml {
    /// Creates a new builder for constructing a [`BatchDml`] request.
    pub fn builder() -> BatchDmlBuilder {
        BatchDmlBuilder::new()
    }
}

impl From<BatchDmlBuilder> for BatchDml {
    fn from(builder: BatchDmlBuilder) -> Self {
        builder.build()
    }
}

impl<T: Into<Statement>> From<Vec<T>> for BatchDml {
    fn from(statements: Vec<T>) -> Self {
        BatchDml {
            statements: statements.into_iter().map(Into::into).collect(),
            request_options: None,
            last_statements: false,
            gax_options: GaxRequestOptions::default(),
        }
    }
}

/// Extracts exact update counts from the given slice of [`ResultSet`]s.
///
/// Every [`ResultSet`] must contain [`ResultSetStats`] with an exact row count. Returns an `internal_error`
/// if `stats` is missing or if the row count is invalid or non-exact.
fn extract_update_counts(result_sets: &[ResultSet]) -> Result<Vec<i64>> {
    let mut update_counts = Vec::with_capacity(result_sets.len());
    for result_set in result_sets {
        let stats = result_set
            .stats
            .as_ref()
            .ok_or_else(|| internal_error("ExecuteBatchDml ResultSet missing stats/row_count"))?;
        let exact_count = match stats.row_count {
            Some(RowCount::RowCountExact(count)) => count,
            _ => {
                return Err(internal_error(
                    "ExecuteBatchDml returned an invalid or missing row count type",
                ));
            }
        };
        update_counts.push(exact_count);
    }
    Ok(update_counts)
}

/// Processes an ExecuteBatchDmlResponse and returns the success counts, or an error.
pub(crate) fn process_response(response: ExecuteBatchDmlResponse) -> Result<Vec<i64>> {
    // If a non-zero status is present, execution halted due to a statement failure or transaction abort.
    if let Some(status) = response
        .status
        .filter(|status| status.code != Code::Ok as i32)
    {
        let grpc_status = RpcStatus::default()
            .set_code(status.code)
            .set_message(status.message)
            .set_details(status.details);
        // If the error code is Aborted, propagate a normal service error so TransactionRunner retries.
        // We check this before extracting update counts because an aborted transaction is completely rolled back.
        if status.code == Code::Aborted as i32 {
            return Err(Error::service(grpc_status));
        }
        let update_counts = extract_update_counts(&response.result_sets)?;
        return Err(BatchUpdateError::build_error(update_counts, grpc_status));
    }

    extract_update_counts(&response.result_sets)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ResultSet, ResultSetMetadata, ResultSetStats, Transaction};
    use google_cloud_rpc::model::Status;
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
            .set_last_statements(true)
            .build();

        assert_eq!(batch.statements.len(), 2);
        assert_eq!(batch.statements[0].sql, "UPDATE t SET c = 1 WHERE id = 1");
        assert_eq!(batch.statements[1].sql, "UPDATE t SET c = 2 WHERE id = 2");
        assert!(batch.last_statements);
        assert!(
            batch.request_options.is_none(),
            "Unexpected request_options set: {:#?}",
            batch.request_options
        );
    }

    #[test]
    fn builder_with_gax_options() {
        use google_cloud_gax::backoff_policy::BackoffPolicy;
        use google_cloud_gax::retry_policy::Aip194Strict;
        use google_cloud_gax::retry_state::RetryState;
        use std::time::Duration;

        #[derive(Debug)]
        struct DummyBackoff;
        impl BackoffPolicy for DummyBackoff {
            fn on_failure(&self, _state: &RetryState) -> Duration {
                Duration::ZERO
            }
        }

        let stmt = Statement::builder("UPDATE t SET c = 1 WHERE id = 1").build();

        let batch = BatchDml::builder()
            .add_statement(stmt)
            .with_attempt_timeout(Duration::from_secs(5))
            .with_retry_policy(Aip194Strict)
            .with_backoff_policy(DummyBackoff)
            .build();

        assert_eq!(
            *batch.gax_options.attempt_timeout(),
            Some(Duration::from_secs(5))
        );
        assert!(batch.gax_options.retry_policy().is_some());
        assert!(batch.gax_options.backoff_policy().is_some());
    }

    #[test]
    fn builder_with_request_tag() {
        let stmt = Statement::builder("UPDATE t SET c = 1 WHERE id = 1").build();

        let batch = BatchDml::builder()
            .add_statement(stmt)
            .set_request_tag("tag1")
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
    fn builder_with_priority() {
        let stmt = Statement::builder("UPDATE t SET c = 1 WHERE id = 1").build();

        let batch = BatchDml::builder()
            .add_statement(stmt)
            .set_priority(Priority::High)
            .build();

        assert_eq!(batch.statements.len(), 1);
        assert_eq!(
            batch
                .request_options
                .expect("request options missing")
                .priority,
            Priority::High
        );
    }

    #[test]
    fn process_response_success() -> anyhow::Result<()> {
        let stats1 = ResultSetStats {
            row_count: Some(RowCount::RowCountExact(5)),
            ..Default::default()
        };
        let stats2 = ResultSetStats {
            row_count: Some(RowCount::RowCountExact(10)),
            ..Default::default()
        };

        let result_set1 = ResultSet {
            stats: Some(stats1),
            ..Default::default()
        };
        let result_set2 = ResultSet {
            stats: Some(stats2),
            ..Default::default()
        };

        let response = ExecuteBatchDmlResponse {
            result_sets: vec![result_set1, result_set2],
            status: None,
            ..Default::default()
        };

        let update_counts = process_response(response)?;
        assert_eq!(
            update_counts,
            vec![5, 10],
            "Expected update counts to match exact counts from result sets"
        );
        Ok(())
    }

    #[test]
    fn process_response_grpc_error() {
        let stats = ResultSetStats {
            row_count: Some(RowCount::RowCountExact(3)),
            ..Default::default()
        };
        let result_set = ResultSet {
            stats: Some(stats),
            ..Default::default()
        };

        // Note: crate::model::Status is the common type for status embedded in generated grpc responses.
        let err_status = Status::default()
            .set_code(Code::InvalidArgument as i32)
            .set_message("Bad query");

        let response = ExecuteBatchDmlResponse {
            result_sets: vec![result_set],
            status: Some(err_status),
            ..Default::default()
        };

        let result = process_response(response);
        let error = result.expect_err("should return error when response status is non-zero");
        let batch_error =
            BatchUpdateError::extract(&error).expect("should extract BatchUpdateError cleanly");

        assert_eq!(
            batch_error.update_counts,
            vec![3],
            "Update counts should contain the successful statement's count"
        );
        assert_eq!(
            batch_error
                .status
                .status()
                .expect("status should be available")
                .code,
            Code::InvalidArgument,
            "Error code should be InvalidArgument"
        );
        assert_eq!(
            batch_error
                .status
                .status()
                .expect("status should be available")
                .message,
            "Bad query",
            "Error message should match the server status message"
        );
    }

    #[test]
    fn process_response_grpc_error_empty_result_sets() {
        let err_status = Status::default()
            .set_code(Code::InvalidArgument as i32)
            .set_message("Initial statement syntax error");

        let response = ExecuteBatchDmlResponse {
            result_sets: vec![],
            status: Some(err_status),
            ..Default::default()
        };

        let result = process_response(response);
        let error = result.expect_err("should return error when initial statement failed");
        let batch_error =
            BatchUpdateError::extract(&error).expect("should extract BatchUpdateError cleanly");

        assert_eq!(
            batch_error.update_counts,
            Vec::<i64>::new(),
            "Update counts should be empty when the first statement failed"
        );
        assert_eq!(
            batch_error
                .status
                .status()
                .expect("status should be available")
                .code,
            Code::InvalidArgument,
            "Error code should be InvalidArgument"
        );
        assert_eq!(
            batch_error
                .status
                .status()
                .expect("status should be available")
                .message,
            "Initial statement syntax error",
            "Error message should match the server status message"
        );
    }

    #[test]
    fn process_response_metadata_with_stats_grpc_error() {
        let stats = ResultSetStats {
            row_count: Some(RowCount::RowCountExact(7)),
            ..Default::default()
        };
        let result_set = ResultSet {
            metadata: Some(ResultSetMetadata {
                transaction: Some(Transaction {
                    id: vec![7, 7, 7].into(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            stats: Some(stats),
            ..Default::default()
        };

        let err_status = Status::default()
            .set_code(Code::InvalidArgument as i32)
            .set_message("Second statement invalid");

        let response = ExecuteBatchDmlResponse {
            result_sets: vec![result_set],
            status: Some(err_status),
            ..Default::default()
        };

        let result = process_response(response);
        let error = result.expect_err("should return error when subsequent statement fails");
        let batch_error =
            BatchUpdateError::extract(&error).expect("should extract BatchUpdateError cleanly");

        assert_eq!(
            batch_error.update_counts,
            vec![7],
            "Update counts should contain the successful first statement count"
        );
        assert_eq!(
            batch_error
                .status
                .status()
                .expect("status should be available")
                .code,
            Code::InvalidArgument,
            "Status code should be InvalidArgument"
        );
        assert_eq!(
            batch_error
                .status
                .status()
                .expect("status should be available")
                .message,
            "Second statement invalid",
            "Status message should match server status message"
        );
    }

    #[test]
    fn process_response_aborted() {
        let stats = ResultSetStats {
            row_count: Some(RowCount::RowCountExact(3)),
            ..Default::default()
        };
        let result_set = ResultSet {
            stats: Some(stats),
            ..Default::default()
        };

        let err_status = Status::default()
            .set_code(Code::Aborted as i32)
            .set_message("transaction aborted");

        let response = ExecuteBatchDmlResponse {
            result_sets: vec![result_set],
            status: Some(err_status),
            ..Default::default()
        };

        let result = process_response(response);
        let error = result.expect_err("should return error on aborted status");
        let batch_error = BatchUpdateError::extract(&error);
        assert!(
            batch_error.is_none(),
            "Unexpected BatchUpdateError on Aborted status: {batch_error:?}"
        );
        assert_eq!(
            error.status().expect("status should be present").code,
            Code::Aborted,
            "Service error code should be Aborted"
        );
        assert_eq!(
            error.status().expect("status should be present").message,
            "transaction aborted",
            "Service error message should match aborted message"
        );
    }

    fn assert_process_response_fails(
        response: ExecuteBatchDmlResponse,
        expected_error_substring: &str,
        failure_description: &'static str,
    ) {
        let result = process_response(response);
        let error = result.expect_err(failure_description);
        assert!(
            error.to_string().contains(expected_error_substring),
            "Expected error containing '{expected_error_substring}', got: {error}"
        );
    }

    #[test]
    fn process_response_missing_stats() {
        let result_set = ResultSet {
            stats: None,
            ..Default::default()
        };

        let response = ExecuteBatchDmlResponse {
            result_sets: vec![result_set],
            ..Default::default()
        };

        assert_process_response_fails(
            response,
            "ExecuteBatchDml ResultSet missing stats/row_count",
            "should fail when ResultSet is missing stats",
        );
    }

    #[test]
    fn process_response_subsequent_result_set_missing_stats() {
        let stats = ResultSetStats {
            row_count: Some(RowCount::RowCountExact(5)),
            ..Default::default()
        };
        let result_set1 = ResultSet {
            stats: Some(stats),
            ..Default::default()
        };
        let result_set2 = ResultSet {
            stats: None,
            ..Default::default()
        };

        let response = ExecuteBatchDmlResponse {
            result_sets: vec![result_set1, result_set2],
            ..Default::default()
        };

        assert_process_response_fails(
            response,
            "ExecuteBatchDml ResultSet missing stats/row_count",
            "should fail when subsequent ResultSet is missing stats",
        );
    }

    #[test]
    fn process_response_grpc_error_with_missing_stats() {
        let result_set = ResultSet {
            metadata: Some(ResultSetMetadata {
                transaction: Some(Transaction {
                    id: vec![7, 7, 7].into(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            stats: None,
            ..Default::default()
        };

        let error_status = Status::default()
            .set_code(Code::InvalidArgument as i32)
            .set_message("Table not found or syntax invalid");

        let response = ExecuteBatchDmlResponse {
            result_sets: vec![result_set],
            status: Some(error_status),
            ..Default::default()
        };

        assert_process_response_fails(
            response,
            "ExecuteBatchDml ResultSet missing stats/row_count",
            "should fail with internal error when stats are missing, even if grpc error status is present",
        );
    }

    #[test]
    fn process_response_grpc_error_with_invalid_row_count_type() {
        let stats = ResultSetStats {
            row_count: Some(RowCount::RowCountLowerBound(10)),
            ..Default::default()
        };
        let result_set = ResultSet {
            stats: Some(stats),
            ..Default::default()
        };

        let error_status = Status::default()
            .set_code(Code::InvalidArgument as i32)
            .set_message("syntax error on subsequent statement");

        let response = ExecuteBatchDmlResponse {
            result_sets: vec![result_set],
            status: Some(error_status),
            ..Default::default()
        };

        assert_process_response_fails(
            response,
            "invalid or missing row count type",
            "should fail with internal error when row count type is invalid, even with grpc error status",
        );
    }

    #[test]
    fn process_response_status_ok_code() -> anyhow::Result<()> {
        let stats = ResultSetStats {
            row_count: Some(RowCount::RowCountExact(4)),
            ..Default::default()
        };
        let result_set = ResultSet {
            stats: Some(stats),
            ..Default::default()
        };
        let ok_status = Status::default().set_code(Code::Ok as i32);
        let response = ExecuteBatchDmlResponse {
            result_sets: vec![result_set],
            status: Some(ok_status),
            ..Default::default()
        };

        let update_counts = process_response(response)?;
        assert_eq!(
            update_counts,
            vec![4],
            "Expected update counts when status code is explicitly Ok"
        );
        Ok(())
    }

    #[test]
    fn process_response_status_ok_code_missing_stats() {
        let result_set = ResultSet {
            stats: None,
            ..Default::default()
        };
        let ok_status = Status::default().set_code(Code::Ok as i32);
        let response = ExecuteBatchDmlResponse {
            result_sets: vec![result_set],
            status: Some(ok_status),
            ..Default::default()
        };

        assert_process_response_fails(
            response,
            "ExecuteBatchDml ResultSet missing stats/row_count",
            "should fail when status is Ok but ResultSet is missing stats",
        );
    }

    #[test]
    fn process_response_aborted_empty_result_sets() {
        let err_status = Status::default()
            .set_code(Code::Aborted as i32)
            .set_message("transaction aborted on first statement");

        let response = ExecuteBatchDmlResponse {
            result_sets: vec![],
            status: Some(err_status),
            ..Default::default()
        };

        let result = process_response(response);
        let error = result
            .expect_err("should return service error on aborted status with empty result sets");
        assert_eq!(
            error.status().expect("status should be present").code,
            Code::Aborted,
            "Service error code should be Aborted"
        );
        assert_eq!(
            error.status().expect("status should be present").message,
            "transaction aborted on first statement",
            "Service error message should match aborted message"
        );
    }

    #[test]
    fn process_response_aborted_with_missing_stats() {
        let result_set = ResultSet {
            stats: None,
            ..Default::default()
        };
        let err_status = Status::default()
            .set_code(Code::Aborted as i32)
            .set_message("transaction aborted");

        let response = ExecuteBatchDmlResponse {
            result_sets: vec![result_set],
            status: Some(err_status),
            ..Default::default()
        };

        let result = process_response(response);
        let error = result
            .expect_err("abort status should take precedence over missing stats in result set");
        assert_eq!(
            error.status().expect("status should be present").code,
            Code::Aborted,
            "Service error code should be Aborted"
        );
        assert_eq!(
            error.status().expect("status should be present").message,
            "transaction aborted",
            "Service error message should match aborted message"
        );
    }

    #[test]
    fn process_response_empty_result_sets_success() -> anyhow::Result<()> {
        let response = ExecuteBatchDmlResponse {
            result_sets: vec![],
            status: None,
            ..Default::default()
        };

        let update_counts = process_response(response)?;
        assert_eq!(
            update_counts,
            Vec::<i64>::new(),
            "Expected empty update counts when result_sets is empty and status is None"
        );
        Ok(())
    }

    #[test]
    fn process_response_missing_row_count_type() {
        let stats = ResultSetStats {
            row_count: None,
            ..Default::default()
        };

        let result_set = ResultSet {
            stats: Some(stats),
            ..Default::default()
        };

        let response = ExecuteBatchDmlResponse {
            result_sets: vec![result_set],
            ..Default::default()
        };

        assert_process_response_fails(
            response,
            "invalid or missing row count type",
            "should fail when row count is missing in stats",
        );
    }

    #[test]
    fn process_response_row_count_lower_bound() {
        let stats = ResultSetStats {
            row_count: Some(RowCount::RowCountLowerBound(42)),
            ..Default::default()
        };

        let result_set = ResultSet {
            stats: Some(stats),
            ..Default::default()
        };

        let response = ExecuteBatchDmlResponse {
            result_sets: vec![result_set],
            ..Default::default()
        };

        assert_process_response_fails(
            response,
            "invalid or missing row count type",
            "RowCountLowerBound is not valid for Batch DML",
        );
    }

    #[test]
    fn from_vector_of_strings() {
        let statements = vec!["UPDATE table SET col = 1", "UPDATE table SET col = 2"];
        let batch: BatchDml = statements.into();
        assert_eq!(
            batch.statements.len(),
            2,
            "Expected exactly 2 statements converted from strings"
        );
        assert_eq!(batch.statements[0].sql, "UPDATE table SET col = 1");
        assert_eq!(batch.statements[1].sql, "UPDATE table SET col = 2");
    }

    #[test]
    fn from_vector_of_statements() {
        let statement1 = Statement::builder("UPDATE table SET col = 1").build();
        let statement2 = Statement::builder("UPDATE table SET col = 2").build();
        let statements = vec![statement1, statement2];
        let batch: BatchDml = statements.into();
        assert_eq!(
            batch.statements.len(),
            2,
            "Expected exactly 2 statements converted from Statement objects"
        );
        assert_eq!(batch.statements[0].sql, "UPDATE table SET col = 1");
        assert_eq!(batch.statements[1].sql, "UPDATE table SET col = 2");
    }

    #[test]
    fn from_builder() {
        let builder = BatchDml::builder().add_statement("UPDATE table SET col = 1");
        let batch: BatchDml = builder.into();
        assert_eq!(
            batch.statements.len(),
            1,
            "Expected 1 statement built from builder"
        );
        assert_eq!(batch.statements[0].sql, "UPDATE table SET col = 1");
    }
}
