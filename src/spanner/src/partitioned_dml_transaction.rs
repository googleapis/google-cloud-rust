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

use crate::database_client::DatabaseClient;
use crate::google::spanner::v1::result_set_stats::RowCount::RowCountLowerBound;
use crate::model::transaction_options::PartitionedDml;
use crate::model::{
    BeginTransactionRequest, ExecuteSqlRequest, TransactionOptions, TransactionSelector,
    transaction_selector,
};
use crate::server_streaming::stream::PartialResultSetStream;
use crate::statement::Statement;
use crate::transaction_retry_helper::{TransactionRetrySettings, retry_aborted};

/// A builder for [PartitionedDmlTransaction].
///
/// # Example
/// ```
/// # use google_cloud_spanner::client::{Spanner, Statement};
/// # async fn build_transaction(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
///     let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
///     let transaction = db_client.partitioned_dml_transaction().build().await?;
///     let statement = Statement::builder("UPDATE users SET active = true WHERE TRUE").build();
///     let modified_rows = transaction.execute_update(statement).await?;
/// #   Ok(())
/// # }
/// ```
pub struct PartitionedDmlTransactionBuilder {
    client: DatabaseClient,
    retry_settings: TransactionRetrySettings,
}

impl PartitionedDmlTransactionBuilder {
    pub(crate) fn new(client: DatabaseClient) -> Self {
        Self {
            client,
            retry_settings: TransactionRetrySettings::default(),
        }
    }

    /// Sets the retry settings for the transaction.
    ///
    /// # Example
    /// ```
    /// # use std::time::Duration;
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::client::TransactionRetrySettings;
    /// # async fn build_transaction(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    ///     let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    ///     
    ///     let retry_settings = TransactionRetrySettings {
    ///         max_attempts: 5,
    ///         total_timeout: Duration::from_secs(60),
    ///     };
    ///
    ///     let transaction = db_client
    ///         .partitioned_dml_transaction()
    ///         .with_retry_settings(retry_settings)
    ///         .build()
    ///         .await?;
    /// #   Ok(())
    /// # }
    /// ```
    ///
    /// The client will retry the entire transaction if it is aborted by Spanner.
    /// These settings determine the maximum number of attempts and the total
    /// timeout for retries. If both are 0, the client will retry indefinitely.
    pub fn with_retry_settings(mut self, settings: TransactionRetrySettings) -> Self {
        self.retry_settings = settings;
        self
    }

    /// Builds the [PartitionedDmlTransaction].
    pub async fn build(self) -> crate::Result<PartitionedDmlTransaction> {
        Ok(PartitionedDmlTransaction {
            client: self.client,
            retry_settings: self.retry_settings,
        })
    }
}

/// A Partitioned DML transaction.
///
/// Partitioned DML transactions are used to execute a single DML statement that may modify a large
/// number of rows. The execution of the statement will automatically be partitioned into smaller
/// transactions by Spanner, which may execute in parallel.
///
/// A Partitioned DML transaction cannot be committed or rolled back.
///
/// See also: <https://docs.cloud.google.com/spanner/docs/dml-partitioned>
pub struct PartitionedDmlTransaction {
    client: DatabaseClient,
    retry_settings: TransactionRetrySettings,
}

impl PartitionedDmlTransaction {
    /// Executes a Partitioned DML statement.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::{Spanner, Statement};
    /// # async fn run(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let transaction = db_client.partitioned_dml_transaction().build().await?;
    /// let statement = Statement::builder("UPDATE users SET active = true WHERE TRUE").build();
    /// let modified_rows = transaction.execute_update(statement).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Return
    ///
    /// The number of rows that was at least modified by the statement. Note that the actual number
    /// of rows that was modified may be higher than this number if the statement was retried or
    /// split into multiple transactions by Spanner, and some of these (sub)transactions were
    /// executed multiple times.
    ///
    /// See also: <https://docs.cloud.google.com/spanner/docs/dml-partitioned>
    pub async fn execute_update<T: Into<Statement>>(self, statement: T) -> crate::Result<i64> {
        let statement = statement.into();

        let transaction_options =
            TransactionOptions::default().set_partitioned_dml(PartitionedDml::default());
        let begin_request = BeginTransactionRequest {
            session: self.client.session.name.clone(),
            options: Some(transaction_options),
            ..Default::default()
        };

        // Execute the statement and retry if the transaction is aborted by Spanner.
        retry_aborted(&self.retry_settings, || async {
            let transaction = self
                .client
                .spanner
                .begin_transaction(begin_request.clone(), crate::RequestOptions::default())
                .await?;

            let execute_request = ExecuteSqlRequest::default()
                .set_session(self.client.session.name.clone())
                .set_transaction(TransactionSelector {
                    selector: Some(transaction_selector::Selector::Id(transaction.id.clone())),
                    ..Default::default()
                })
                .set_or_clear_params(statement.get_params())
                .set_param_types(statement.get_param_types())
                .set_sql(statement.sql.clone());

            let stream_builder = self
                .client
                .spanner
                .execute_streaming_sql(execute_request.clone(), crate::RequestOptions::default());
            let stream = stream_builder.send().await?;

            extract_lower_bound_update_count_from_stream(stream).await
        })
        .await
    }
}

/// Reads through the stream of `PartialResultSet` messages returned by the execution
/// of a Partitioned DML statement and extracts the `row_count_lower_bound` from the
/// query statistics. If the execution is successful but no lower bound is found,
/// an internal error is returned.
async fn extract_lower_bound_update_count_from_stream(
    mut stream: PartialResultSetStream,
) -> crate::Result<i64> {
    let mut lower_bound: Option<i64> = None;
    while let Some(message_res) = stream.next_message().await {
        let prs = message_res?;
        if let Some(stats) = prs.stats {
            if let Some(RowCountLowerBound(val)) = stats.row_count {
                lower_bound = Some(val);
            }
        }
    }
    match lower_bound {
        Some(lb) => Ok(lb),
        None => Err(crate::Error::deser(
            crate::error::SpannerInternalError::new(
                "ExecuteStreamingSql completed successfully but no row_count_lower_bound was returned",
            ),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::read_only_transaction::tests::{create_session_mock, setup_db_client};
    use gaxi::grpc::tonic;
    use spanner_grpc_mock::google::spanner::v1;
    type MockExecuteStreamingSqlStream =
        <spanner_grpc_mock::MockSpanner as v1::spanner_server::Spanner>::ExecuteStreamingSqlStream;

    #[test]
    fn auto_traits() {
        static_assertions::assert_impl_all!(PartitionedDmlTransactionBuilder: Send, Sync);
        static_assertions::assert_impl_all!(PartitionedDmlTransaction: Send, Sync);
    }

    #[tokio::test]
    async fn execute_update_success() {
        let mut mock = create_session_mock();

        mock.expect_begin_transaction().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.session,
                "projects/p/instances/i/databases/d/sessions/123"
            );
            Ok(tonic::Response::new(v1::Transaction {
                id: vec![0, 1, 2],
                ..Default::default()
            }))
        });

        mock.expect_execute_streaming_sql().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.sql, "UPDATE Users SET active = true");

            let res = vec![v1::PartialResultSet {
                stats: Some(v1::ResultSetStats {
                    row_count: Some(v1::result_set_stats::RowCount::RowCountLowerBound(500)),
                    ..Default::default()
                }),
                ..Default::default()
            }];
            let stream = tokio_stream::iter(res.into_iter().map(Ok));
            Ok(tonic::Response::new(
                Box::pin(stream) as MockExecuteStreamingSqlStream
            ))
        });

        let (db_client, _server) = setup_db_client(mock).await;
        let transaction = db_client
            .partitioned_dml_transaction()
            .build()
            .await
            .unwrap();
        let statement = Statement::builder("UPDATE Users SET active = true").build();
        let res: i64 = transaction.execute_update(statement).await.unwrap();
        assert_eq!(res, 500);
    }

    #[tokio::test]
    #[ignore = "Transaction retries will be implemented in a subsequent PR"]
    async fn execute_update_with_aborted_retry() {
        let mut mock = create_session_mock();

        mock.expect_begin_transaction().times(2).returning(|_req| {
            Ok(tonic::Response::new(v1::Transaction {
                id: vec![0, 1, 2],
                ..Default::default()
            }))
        });

        let mut attempt = 0;
        mock.expect_execute_streaming_sql()
            .times(2)
            .returning(move |_req| {
                attempt += 1;
                if attempt == 1 {
                    // Return an error stream on first try
                    let stream = tokio_stream::iter(vec![Err(tonic::Status::new(
                        tonic::Code::Aborted,
                        "aborted",
                    ))]);
                    Ok(tonic::Response::new(
                        Box::pin(stream) as MockExecuteStreamingSqlStream
                    ))
                } else {
                    let res = vec![v1::PartialResultSet {
                        stats: Some(v1::ResultSetStats {
                            row_count: Some(v1::result_set_stats::RowCount::RowCountLowerBound(
                                100,
                            )),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }];
                    let stream = tokio_stream::iter(res.into_iter().map(Ok));
                    Ok(tonic::Response::new(
                        Box::pin(stream) as MockExecuteStreamingSqlStream
                    ))
                }
            });

        let (db_client, _server) = setup_db_client(mock).await;
        let transaction = db_client
            .partitioned_dml_transaction()
            .build()
            .await
            .unwrap();
        let res: i64 = transaction
            .execute_update(Statement::builder("UPDATE Users SET active = true").build())
            .await
            .unwrap();
        assert_eq!(res, 100);
    }

    #[tokio::test]
    async fn builder_with_retry_settings() {
        let mock = create_session_mock();
        let (db_client, _server) = setup_db_client(mock).await;

        let settings = TransactionRetrySettings {
            max_attempts: 10,
            total_timeout: std::time::Duration::from_secs(42),
        };

        let transaction = db_client
            .partitioned_dml_transaction()
            .with_retry_settings(settings)
            .build()
            .await
            .unwrap();

        assert_eq!(transaction.retry_settings.max_attempts, 10);
        assert_eq!(
            transaction.retry_settings.total_timeout,
            std::time::Duration::from_secs(42)
        );
    }

    #[tokio::test]
    async fn execute_update_missing_lower_bound() {
        let mut mock = create_session_mock();

        mock.expect_begin_transaction().once().returning(|_req| {
            Ok(tonic::Response::new(v1::Transaction {
                id: vec![0, 1, 2],
                ..Default::default()
            }))
        });

        mock.expect_execute_streaming_sql()
            .once()
            .returning(|_req| {
                let res = vec![v1::PartialResultSet {
                    stats: Some(v1::ResultSetStats {
                        // Provide a RowCountExact instead of RowCountLowerBound
                        row_count: Some(v1::result_set_stats::RowCount::RowCountExact(100)),
                        ..Default::default()
                    }),
                    ..Default::default()
                }];
                let stream = tokio_stream::iter(res.into_iter().map(Ok));
                Ok(tonic::Response::new(
                    Box::pin(stream) as MockExecuteStreamingSqlStream
                ))
            });

        let (db_client, _server) = setup_db_client(mock).await;
        let transaction = db_client
            .partitioned_dml_transaction()
            .build()
            .await
            .unwrap();

        let statement = Statement::builder("UPDATE Users SET active = true").build();
        let res = transaction.execute_update(statement).await;

        assert!(res.is_err());
        let err = res.unwrap_err();
        assert!(err.is_deserialization());
        assert!(
            err.to_string()
                .contains("no row_count_lower_bound was returned")
        );
    }
}
