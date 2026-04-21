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
    BeginTransactionRequest, TransactionOptions, TransactionSelector, transaction_selector,
};
use crate::server_streaming::stream::PartialResultSetStream;
use crate::statement::Statement;
use crate::transaction_retry_policy::{
    BasicTransactionRetryPolicy, TransactionRetryPolicy, retry_aborted,
};

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
    retry_policy: Box<dyn TransactionRetryPolicy>,
}

impl PartitionedDmlTransactionBuilder {
    pub(crate) fn new(client: DatabaseClient) -> Self {
        Self {
            client,
            retry_policy: Box::new(BasicTransactionRetryPolicy::default()),
        }
    }

    /// Sets the retry policy for the transaction.
    ///
    /// # Example
    /// ```
    /// # use std::time::Duration;
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::client::BasicTransactionRetryPolicy;
    /// # async fn build_transaction(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    ///     let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    ///     
    ///     let retry_policy = BasicTransactionRetryPolicy {
    ///         max_attempts: 5,
    ///         total_timeout: Duration::from_secs(60),
    ///     };
    ///
    ///     let transaction = db_client
    ///         .partitioned_dml_transaction()
    ///         .with_retry_policy(retry_policy)
    ///         .build()
    ///         .await?;
    /// #   Ok(())
    /// # }
    /// ```
    ///
    /// The client will retry the entire transaction if it is aborted by Spanner.
    /// This policy can be used to customize whether a transaction should be retried
    /// or not. The default is to retry indefinitely until the transaction succeeds.
    pub fn with_retry_policy<P: TransactionRetryPolicy + 'static>(mut self, policy: P) -> Self {
        self.retry_policy = Box::new(policy);
        self
    }

    /// Builds the [PartitionedDmlTransaction].
    pub async fn build(self) -> crate::Result<PartitionedDmlTransaction> {
        Ok(PartitionedDmlTransaction {
            client: self.client,
            retry_policy: self.retry_policy,
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
    retry_policy: Box<dyn TransactionRetryPolicy>,
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

        let session_name = self.client.session_name();
        let transaction_options =
            TransactionOptions::default().set_partitioned_dml(PartitionedDml::default());
        let begin_request = BeginTransactionRequest {
            session: session_name.clone(),
            options: Some(transaction_options),
            ..Default::default()
        };
        let base_request = statement.into_request();
        let channel_hint = self.client.spanner.next_channel_hint();

        // Execute the statement and retry if the transaction is aborted by Spanner.
        retry_aborted(&*self.retry_policy, || async {
            let transaction = self
                .client
                .spanner
                .begin_transaction(
                    begin_request.clone(),
                    crate::RequestOptions::default(),
                    channel_hint,
                )
                .await?;

            let execute_request = base_request
                .clone()
                .set_session(session_name.clone())
                .set_transaction(TransactionSelector {
                    selector: Some(transaction_selector::Selector::Id(transaction.id.clone())),
                    ..Default::default()
                });

            let stream_builder = self.client.spanner.execute_streaming_sql(
                execute_request.clone(),
                crate::RequestOptions::default(),
                channel_hint,
            );
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
    while let Some(prs) = stream.next_message().await.transpose()? {
        if let Some(RowCountLowerBound(val)) = prs.stats.and_then(|s| s.row_count) {
            lower_bound = Some(val);
        }
    }
    lower_bound.ok_or_else(|| {
        crate::Error::deser(crate::error::SpannerInternalError::new(
            "ExecuteStreamingSql completed successfully but no row_count_lower_bound was returned",
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::read_only_transaction::tests::{create_session_mock, setup_db_client};
    use crate::result_set::tests::adapt;
    use crate::transaction_retry_policy::tests::create_aborted_status;
    use gaxi::grpc::tonic;
    use spanner_grpc_mock::google::spanner::v1;

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

            let stream = adapt([Ok(v1::PartialResultSet {
                stats: Some(v1::ResultSetStats {
                    row_count: Some(v1::result_set_stats::RowCount::RowCountLowerBound(500)),
                    ..Default::default()
                }),
                ..Default::default()
            })]);
            Ok(tonic::Response::from(stream))
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
    async fn execute_update_with_aborted_retry() {
        let mut mock = create_session_mock();

        mock.expect_begin_transaction().times(2).returning(|_req| {
            Ok(tonic::Response::new(v1::Transaction {
                id: vec![0, 1, 2],
                ..Default::default()
            }))
        });

        let mut seq = mockall::Sequence::new();
        mock.expect_execute_streaming_sql()
            .times(1)
            .in_sequence(&mut seq)
            .returning(move |_req| {
                // Return an error stream on first try
                let stream = adapt([Err(create_aborted_status(std::time::Duration::from_nanos(
                    1,
                )))]);
                Ok(tonic::Response::from(stream))
            });
        mock.expect_execute_streaming_sql()
            .times(1)
            .in_sequence(&mut seq)
            .returning(move |_req| {
                let stream = adapt([Ok(v1::PartialResultSet {
                    stats: Some(v1::ResultSetStats {
                        row_count: Some(v1::result_set_stats::RowCount::RowCountLowerBound(100)),
                        ..Default::default()
                    }),
                    ..Default::default()
                })]);
                Ok(tonic::Response::from(stream))
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

        let policy = BasicTransactionRetryPolicy {
            max_attempts: 10,
            total_timeout: std::time::Duration::from_secs(42),
        };

        let _transaction = db_client
            .partitioned_dml_transaction()
            .with_retry_policy(policy)
            .build()
            .await
            .unwrap();
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
                let stream = adapt([Ok(v1::PartialResultSet {
                    stats: Some(v1::ResultSetStats {
                        // Provide a RowCountExact instead of RowCountLowerBound
                        row_count: Some(v1::result_set_stats::RowCount::RowCountExact(100)),
                        ..Default::default()
                    }),
                    ..Default::default()
                })]);
                Ok(tonic::Response::from(stream))
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
