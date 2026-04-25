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
use crate::model::PartitionOptions;
use crate::precommit::PrecommitTokenTracker;
use crate::read_only_transaction::{
    MultiUseReadOnlyTransaction, MultiUseReadOnlyTransactionBuilder, ReadContextTransactionSelector,
};
use crate::result_set::{ResultSet, StreamOperation};
use crate::statement::Statement;
use crate::timestamp_bound::TimestampBound;
use google_cloud_gax::backoff_policy::BackoffPolicyArg;
use google_cloud_gax::options::RequestOptions as GaxRequestOptions;
use google_cloud_gax::retry_policy::RetryPolicyArg;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// A builder for [BatchReadOnlyTransaction].
///
/// # Example
/// ```
/// # use google_cloud_spanner::client::Spanner;
/// # use google_cloud_spanner::client::TimestampBound;
/// # async fn build_tx(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
/// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
/// let read_only_transaction = db_client.batch_read_only_transaction()
///     .with_timestamp_bound(TimestampBound::strong())
///     .build()
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct BatchReadOnlyTransactionBuilder {
    inner: MultiUseReadOnlyTransactionBuilder,
}

impl BatchReadOnlyTransactionBuilder {
    pub(crate) fn new(client: DatabaseClient) -> Self {
        Self {
            inner: MultiUseReadOnlyTransactionBuilder::new(client)
                .with_explicit_begin_transaction(true),
        }
    }

    /// Sets the timestamp bound for the read-only transaction.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::client::TimestampBound;
    /// # async fn set_bound(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let builder = db_client.batch_read_only_transaction().with_timestamp_bound(TimestampBound::strong());
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_timestamp_bound(self, bound: TimestampBound) -> Self {
        Self {
            inner: self.inner.with_timestamp_bound(bound),
        }
    }

    /// Builds the [BatchReadOnlyTransaction] and starts the transaction
    /// by calling the `BeginTransaction` RPC.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # async fn build(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let transaction = db_client.batch_read_only_transaction().build().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn build(self) -> crate::Result<BatchReadOnlyTransaction> {
        let inner = self.inner.build().await?;
        Ok(BatchReadOnlyTransaction { inner })
    }
}

/// A read-only transaction that can be used to partition reads and queries
/// and execute these in parallel across multiple workers.
///
/// # Example
/// ```
/// # use google_cloud_spanner::client::Spanner;
/// # use google_cloud_spanner::client::Statement;
/// # use google_cloud_spanner::model::PartitionOptions;
///
/// # async fn run(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
/// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
/// let transaction = db_client.batch_read_only_transaction().build().await?;
/// let stmt = Statement::builder("SELECT * FROM users WHERE id = @id")
///     .add_param("id", &42)
///     .build();
/// let options = PartitionOptions::default()
///     .set_max_partitions(10);
/// let partitions = transaction.partition_query(stmt, options).await?;
///
/// // partitions can be sent to other workers for parallel execution
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct BatchReadOnlyTransaction {
    inner: MultiUseReadOnlyTransaction,
}

impl BatchReadOnlyTransaction {
    /// Returns the read timestamp chosen for the transaction.
    pub fn read_timestamp(&self) -> Option<wkt::Timestamp> {
        self.inner.read_timestamp()
    }

    /// Creates a set of partitions that can be used to execute a query in parallel.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::client::Statement;
    /// # use google_cloud_spanner::model::PartitionOptions;
    /// # async fn run(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let transaction = db.batch_read_only_transaction().build().await?;
    ///
    /// let stmt = Statement::builder("SELECT * FROM users WHERE id = @id")
    ///     .add_param("id", &42)
    ///     .build();
    /// let options = PartitionOptions::default()
    ///     .set_max_partitions(10);
    /// let partitions = transaction.partition_query(stmt, options).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn partition_query<T: Into<Statement>>(
        &self,
        statement: T,
        options: PartitionOptions,
    ) -> crate::Result<Vec<Partition>> {
        let statement = statement.into();
        let request = statement
            .clone()
            .into_partition_query_request()
            .set_session(self.inner.context.session_name.clone())
            .set_transaction(self.inner.context.transaction_selector.selector())
            .set_partition_options(options);

        let response = self
            .inner
            .context
            .client
            .spanner
            .partition_query(request, crate::RequestOptions::default())
            .await?;

        Ok(response
            .partitions
            .into_iter()
            .map(|p| {
                let mut req = statement.clone().into_request();
                req.session = self.inner.context.session_name.clone();
                req.transaction = Some(self.inner.context.transaction_selector.selector());
                req.partition_token = p.partition_token;

                Partition {
                    inner: PartitionedOperation::Query(req),
                    gax_options: GaxRequestOptions::default(),
                }
            })
            .collect())
    }

    /// Creates a set of partitions that can be used to execute a read in parallel.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::{KeySet, Spanner};
    /// # use google_cloud_spanner::client::ReadRequest;
    /// # use google_cloud_spanner::model::PartitionOptions;
    /// # async fn run(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let transaction = db.batch_read_only_transaction().build().await?;
    ///
    /// let read = ReadRequest::builder("users", vec!["id".to_string(), "name".to_string()])
    ///     .with_keys(KeySet::all())
    ///     .build();
    /// let options = PartitionOptions::default()
    ///     .set_max_partitions(10);
    /// let partitions = transaction.partition_read(read, options).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn partition_read<T: Into<crate::read::ReadRequest>>(
        &self,
        read: T,
        options: PartitionOptions,
    ) -> crate::Result<Vec<Partition>> {
        let read = read.into();
        let request = read
            .clone()
            .into_partition_read_request()
            .set_session(self.inner.context.session_name.clone())
            .set_transaction(self.inner.context.transaction_selector.selector())
            .set_partition_options(options);

        let response = self
            .inner
            .context
            .client
            .spanner
            .partition_read(request, crate::RequestOptions::default())
            .await?;

        Ok(response
            .partitions
            .into_iter()
            .map(|p| {
                let mut req = read.clone().into_request();
                req.session = self.inner.context.session_name.clone();
                req.transaction = Some(self.inner.context.transaction_selector.selector());
                req.partition_token = p.partition_token;

                Partition {
                    inner: PartitionedOperation::Read(req),
                    gax_options: GaxRequestOptions::default(),
                }
            })
            .collect())
    }
}

/// Defines the segments of data to be read in a partitioned read or query.
/// These partitions can be serialized and processed across several
/// different machines or processes.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Partition {
    pub(crate) inner: PartitionedOperation,
    #[serde(skip)]
    pub(crate) gax_options: GaxRequestOptions,
}

impl Partition {
    /// Sets whether Data Boost is enabled for this partition.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::{Spanner, Statement};
    /// # use google_cloud_spanner::model::PartitionOptions;
    /// # async fn run_query(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// # let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// # let transaction = db_client.batch_read_only_transaction().build().await?;
    /// # let partitions = transaction.partition_query(Statement::builder("SELECT * FROM Users").build(), PartitionOptions::default()).await?;
    /// // On a worker receiving a partition, execute it with Data Boost:
    /// let mut result_set = partitions[0].clone()
    ///     .with_data_boost(true)
    ///     .execute(&db_client)
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_data_boost(mut self, enabled: bool) -> Self {
        match &mut self.inner {
            PartitionedOperation::Query(req) => req.data_boost_enabled = enabled,
            PartitionedOperation::Read(req) => req.data_boost_enabled = enabled,
        }
        self
    }

    /// Sets the per-attempt timeout for this partition execution.
    ///
    /// **Note:** This field is **not serialized**. Each host that executes a partition must set its own attempt timeout.
    pub fn with_attempt_timeout(mut self, timeout: Duration) -> Self {
        self.gax_options.set_attempt_timeout(timeout);
        self
    }

    /// Sets the retry policy for this partition execution.
    ///
    /// **Note:** This field is **not serialized**. Each host that executes a partition must set its own retry policy.
    pub fn with_retry_policy(mut self, policy: impl Into<RetryPolicyArg>) -> Self {
        self.gax_options.set_retry_policy(policy);
        self
    }

    /// Sets the backoff policy for this partition execution.
    ///
    /// **Note:** This field is **not serialized**. Each host that executes a partition must set its own backoff policy.
    pub fn with_backoff_policy(mut self, policy: impl Into<BackoffPolicyArg>) -> Self {
        self.gax_options.set_backoff_policy(policy);
        self
    }

    /// Executes this partition and returns a [ResultSet] that
    /// contains the rows that belong to this partition.
    ///
    /// # Example: executing a query partition
    /// ```
    /// # use google_cloud_spanner::client::{Spanner, Statement};
    /// # use google_cloud_spanner::model::PartitionOptions;
    /// # async fn run_query(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let transaction = db_client.batch_read_only_transaction().build().await?;
    /// let partitions = transaction.partition_query(
    ///     Statement::builder("SELECT * FROM Users").build(),
    ///     PartitionOptions::default()
    /// ).await?;
    ///
    /// // ... send partitions to other workers ...
    ///
    /// // On a worker receiving a partition, execute it:
    /// let mut result_set = partitions[0].execute(&db_client).await?;
    /// while let Some(row) = result_set.next().await.transpose()? {
    ///     // process row
    /// }
    /// # Ok(())
    /// # }
    /// ```
    /// # Example: executing a read partition
    /// ```
    /// # use google_cloud_spanner::client::{Spanner, ReadRequest, KeySet};
    /// # use google_cloud_spanner::model::PartitionOptions;
    /// # async fn run_read(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let transaction = db_client.batch_read_only_transaction().build().await?;
    /// let req = ReadRequest::builder("Users", vec!["Id", "Name"]).with_keys(KeySet::all()).build();
    /// let partitions = transaction.partition_read(req, PartitionOptions::default()).await?;
    ///
    /// // ... send partitions to other workers ...
    ///
    /// // On a worker receiving a partition, execute it:
    /// let mut result_set = partitions[0].execute(&db_client).await?;
    /// while let Some(row) = result_set.next().await.transpose()? {
    ///     // process row
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// A partition can be executed by any `DatabaseClient` that is connected to
    /// the database that the partitions belong to.
    pub async fn execute(&self, client: &DatabaseClient) -> crate::Result<ResultSet> {
        match &self.inner {
            PartitionedOperation::Query(req) => {
                Self::execute_query(client, req, self.gax_options.clone()).await
            }
            PartitionedOperation::Read(req) => {
                Self::execute_read(client, req, self.gax_options.clone()).await
            }
        }
    }

    async fn execute_query(
        client: &DatabaseClient,
        req: &crate::model::ExecuteSqlRequest,
        gax_options: GaxRequestOptions,
    ) -> crate::Result<ResultSet> {
        let stream = client
            .spanner
            .execute_streaming_sql(req.clone(), gax_options.clone())
            .send()
            .await?;

        Ok(ResultSet::new(
            stream,
            Some(ReadContextTransactionSelector::Fixed(
                req.transaction.clone().unwrap_or_default(),
                None,
            )),
            PrecommitTokenTracker::new_noop(),
            client.clone(),
            req.session.clone(),
            StreamOperation::Query(req.clone()),
            gax_options,
        ))
    }

    async fn execute_read(
        client: &DatabaseClient,
        req: &crate::model::ReadRequest,
        gax_options: GaxRequestOptions,
    ) -> crate::Result<ResultSet> {
        let stream = client
            .spanner
            .streaming_read(req.clone(), gax_options.clone())
            .send()
            .await?;

        Ok(ResultSet::new(
            stream,
            Some(ReadContextTransactionSelector::Fixed(
                req.transaction.clone().unwrap_or_default(),
                None,
            )),
            PrecommitTokenTracker::new_noop(),
            client.clone(),
            req.session.clone(),
            StreamOperation::Read(req.clone()),
            gax_options,
        ))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum PartitionedOperation {
    Query(crate::model::ExecuteSqlRequest),
    Read(crate::model::ReadRequest),
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::client::Statement;
    use crate::client::{KeySet, ReadRequest as SpannerReadRequest, TimestampBound};
    use crate::model::transaction_selector::Selector;
    use crate::model::{ExecuteSqlRequest, ReadRequest as GrpcReadRequest, TransactionSelector};
    use crate::read_only_transaction::tests::{create_session_mock, setup_db_client};
    use gaxi::grpc::tonic::Response;
    use google_cloud_test_macros::tokio_test_no_panics;
    use prost_types::Timestamp;
    use spanner_grpc_mock::google::spanner::v1::{
        Partition as MockPartition, PartitionResponse, Transaction,
    };
    use static_assertions::assert_impl_all;
    use std::fmt::Debug;

    #[test]
    fn auto_traits() {
        assert_impl_all!(BatchReadOnlyTransactionBuilder: Send, Sync);
        assert_impl_all!(BatchReadOnlyTransaction: Send, Sync, Debug);
        assert_impl_all!(Partition: Send, Sync, Debug);
    }

    #[test]
    fn serialize_partition_skips_gax_options() -> anyhow::Result<()> {
        use std::time::Duration;

        let req = crate::model::ExecuteSqlRequest::new()
            .set_sql("SELECT 1")
            .set_partition_token(b"token".to_vec());

        let mut gax_options = GaxRequestOptions::default();
        gax_options.set_attempt_timeout(Duration::from_secs(5));
        let partition = Partition {
            inner: PartitionedOperation::Query(req),
            gax_options,
        };

        let serialized = serde_json::to_string(&partition)?;
        let deserialized: Partition = serde_json::from_str(&serialized)?;

        // Verify that gax_options was NOT preserved (it uses default, which is None timeout)
        assert_eq!(*deserialized.gax_options.attempt_timeout(), None);

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn partition_execute_respects_options() -> anyhow::Result<()> {
        use gaxi::grpc::tonic::Response;
        use std::time::Duration;

        let mut mock = create_session_mock();

        mock.expect_execute_streaming_sql().once().returning(|req| {
            let timeout = req.metadata().get("grpc-timeout");
            assert!(timeout.is_some(), "Missing grpc-timeout header");
            assert_eq!(timeout.unwrap(), "5000000u"); // 5 seconds in micros

            let (_, rx) = tokio::sync::mpsc::channel(1);
            Ok(Response::from(rx))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let req = crate::model::ExecuteSqlRequest::new()
            .set_session("projects/p/instances/i/databases/d/sessions/123")
            .set_transaction(crate::model::TransactionSelector {
                selector: Some(Selector::Id(b"tx_id_1".to_vec().into())),
                ..Default::default()
            })
            .set_sql("SELECT 1")
            .set_partition_token(b"token".to_vec());

        let partition = Partition {
            inner: PartitionedOperation::Query(req),
            gax_options: GaxRequestOptions::default(),
        };

        let partition = partition.with_attempt_timeout(Duration::from_secs(5));

        let _result_set = partition.execute(&db_client).await?;

        Ok(())
    }

    #[test]
    fn serialize_partition_query() -> anyhow::Result<()> {
        let req = crate::model::ExecuteSqlRequest::new()
            .set_session("projects/p/instances/i/databases/d/sessions/123")
            .set_transaction(crate::model::TransactionSelector {
                selector: Some(crate::model::transaction_selector::Selector::Id(
                    b"tx_id_1".to_vec().into(),
                )),
                ..Default::default()
            })
            .set_sql("SELECT * FROM Users")
            .set_partition_token(b"partition_token_123".to_vec());

        let partition = Partition {
            inner: PartitionedOperation::Query(req),
            gax_options: GaxRequestOptions::default(),
        };

        let serialized = serde_json::to_string(&partition)?;
        let deserialized: Partition = serde_json::from_str(&serialized)?;

        match &deserialized.inner {
            PartitionedOperation::Query(r) => {
                assert_eq!(r.partition_token.as_ref(), b"partition_token_123");
                assert_eq!(r.sql, "SELECT * FROM Users");
                assert_eq!(r.session, "projects/p/instances/i/databases/d/sessions/123");
            }
            _ => panic!("Expected Query partition"),
        }
        Ok(())
    }

    #[test]
    fn serialize_partition_read() -> anyhow::Result<()> {
        let req = crate::model::ReadRequest::new()
            .set_session("projects/p/instances/i/databases/d/sessions/456")
            .set_transaction(crate::model::TransactionSelector {
                selector: Some(crate::model::transaction_selector::Selector::Id(
                    b"tx_id_2".to_vec().into(),
                )),
                ..Default::default()
            })
            .set_table("Users")
            .set_columns(vec!["Id"])
            .set_partition_token(b"partition_token_456".to_vec());

        let partition = Partition {
            inner: PartitionedOperation::Read(req),
            gax_options: GaxRequestOptions::default(),
        };

        let serialized = serde_json::to_string(&partition)?;
        let deserialized: Partition = serde_json::from_str(&serialized)?;

        match &deserialized.inner {
            PartitionedOperation::Read(r) => {
                assert_eq!(r.partition_token.as_ref(), b"partition_token_456");
                assert_eq!(r.table, "Users");
                assert_eq!(r.session, "projects/p/instances/i/databases/d/sessions/456");
            }
            _ => panic!("Expected Read partition"),
        }
        Ok(())
    }

    #[tokio::test]
    async fn execute_query() -> anyhow::Result<()> {
        let mut mock = create_session_mock();

        mock.expect_execute_streaming_sql().once().returning(|req| {
            let req = req.into_inner();
            // Verify the partition details were properly stamped onto the request
            assert_eq!(
                req.session,
                "projects/p/instances/i/databases/d/sessions/123"
            );
            assert_eq!(req.partition_token, b"partition_token_123".as_slice());
            assert!(req.transaction.is_some());
            assert_eq!(req.sql, "SELECT * FROM Users");

            let (_, rx) = tokio::sync::mpsc::channel(1);
            Ok(Response::from(rx))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let req = crate::model::ExecuteSqlRequest::new()
            .set_session("projects/p/instances/i/databases/d/sessions/123")
            .set_transaction(crate::model::TransactionSelector {
                selector: Some(crate::model::transaction_selector::Selector::Id(
                    b"tx_id_1".to_vec().into(),
                )),
                ..Default::default()
            })
            .set_sql("SELECT * FROM Users")
            .set_partition_token(b"partition_token_123".to_vec());

        let partition = Partition {
            inner: PartitionedOperation::Query(req),
            gax_options: GaxRequestOptions::default(),
        };

        let _result_set = partition.execute(&db_client).await?;

        Ok(())
    }

    #[tokio::test]
    async fn execute_read() -> anyhow::Result<()> {
        let mut mock = create_session_mock();

        mock.expect_streaming_read().once().returning(|req| {
            let req = req.into_inner();
            // Verify the partition details were properly stamped onto the request
            assert_eq!(
                req.session,
                "projects/p/instances/i/databases/d/sessions/456"
            );
            assert_eq!(req.partition_token, b"partition_token_456".as_slice());
            assert!(req.transaction.is_some());
            assert_eq!(req.table, "Users");

            let (_, rx) = tokio::sync::mpsc::channel(1);
            Ok(Response::from(rx))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let req = crate::model::ReadRequest::new()
            .set_session("projects/p/instances/i/databases/d/sessions/456")
            .set_transaction(crate::model::TransactionSelector {
                selector: Some(crate::model::transaction_selector::Selector::Id(
                    b"tx_id_2".to_vec().into(),
                )),
                ..Default::default()
            })
            .set_table("Users")
            .set_columns(vec!["Id"])
            .set_partition_token(b"partition_token_456".to_vec());

        let partition = Partition {
            inner: PartitionedOperation::Read(req),
            gax_options: GaxRequestOptions::default(),
        };

        let _result_set = partition.execute(&db_client).await?;

        Ok(())
    }

    #[tokio::test]
    async fn partition_query() -> anyhow::Result<()> {
        let mut mock = create_session_mock();

        mock.expect_begin_transaction().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.session,
                "projects/p/instances/i/databases/d/sessions/123"
            );
            Ok(Response::new(Transaction {
                id: vec![1, 2, 3],
                read_timestamp: Some(Timestamp {
                    seconds: 123456789,
                    nanos: 0,
                }),
                ..Default::default()
            }))
        });

        mock.expect_partition_query().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.session,
                "projects/p/instances/i/databases/d/sessions/123"
            );
            assert_eq!(req.sql, "SELECT 1");
            Ok(Response::new(PartitionResponse {
                partitions: vec![
                    MockPartition {
                        partition_token: vec![10],
                    },
                    MockPartition {
                        partition_token: vec![20],
                    },
                ],
                transaction: None,
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let tx = db_client
            .batch_read_only_transaction()
            .with_timestamp_bound(TimestampBound::strong())
            .build()
            .await?;

        let ts = tx.read_timestamp().expect("Missing read timestamp");
        assert_eq!(ts.seconds(), 123456789);
        assert_eq!(ts.nanos(), 0);

        let partitions = tx
            .partition_query(
                Statement::builder("SELECT 1").build(),
                PartitionOptions::default(),
            )
            .await?;

        assert_eq!(partitions.len(), 2);

        match &partitions[0].inner {
            PartitionedOperation::Query(req) => {
                assert_eq!(req.partition_token.as_ref(), &[10]);
                assert_eq!(req.sql, "SELECT 1");
            }
            _ => panic!("Expected Query partition"),
        }
        Ok(())
    }

    #[tokio::test]
    async fn partition_read() -> anyhow::Result<()> {
        let mut mock = create_session_mock();

        mock.expect_begin_transaction().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.session,
                "projects/p/instances/i/databases/d/sessions/123"
            );
            Ok(Response::new(Transaction {
                id: vec![1, 2, 3],
                read_timestamp: Some(Timestamp {
                    seconds: 123456789,
                    nanos: 0,
                }),
                ..Default::default()
            }))
        });

        mock.expect_partition_read().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.session,
                "projects/p/instances/i/databases/d/sessions/123"
            );
            assert_eq!(req.table, "Users");
            Ok(Response::new(PartitionResponse {
                partitions: vec![MockPartition {
                    partition_token: vec![30],
                }],
                transaction: None,
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let transaction = db_client.batch_read_only_transaction().build().await?;

        let read = SpannerReadRequest::builder("Users", vec!["Id", "Name"])
            .with_keys(KeySet::all())
            .build();
        let partitions = transaction
            .partition_read(read, PartitionOptions::default())
            .await?;

        assert_eq!(partitions.len(), 1);

        match &partitions[0].inner {
            PartitionedOperation::Read(req) => {
                assert_eq!(req.partition_token.as_ref(), &[30]);
                assert_eq!(req.table, "Users");
            }
            _ => panic!("Expected Read partition"),
        }
        Ok(())
    }

    #[tokio::test]
    async fn execute_query_with_data_boost() -> anyhow::Result<()> {
        let mut mock = create_session_mock();

        mock.expect_execute_streaming_sql().once().returning(|req| {
            let req = req.into_inner();
            assert!(req.data_boost_enabled, "data_boost_enabled should be true");
            let (_, rx) = tokio::sync::mpsc::channel(1);
            Ok(Response::from(rx))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let req = ExecuteSqlRequest::new()
            .set_session("projects/p/instances/i/databases/d/sessions/123")
            .set_transaction(TransactionSelector {
                selector: Some(Selector::Id(b"tx_id_1".to_vec().into())),
                ..Default::default()
            })
            .set_sql("SELECT * FROM Users")
            .set_partition_token(b"partition_token_123".to_vec());

        let partition = Partition {
            inner: PartitionedOperation::Query(req),
            gax_options: GaxRequestOptions::default(),
        };

        let _result_set = partition.with_data_boost(true).execute(&db_client).await?;

        Ok(())
    }

    #[tokio::test]
    async fn execute_read_with_data_boost() -> anyhow::Result<()> {
        let mut mock = create_session_mock();

        mock.expect_streaming_read().once().returning(|req| {
            let req = req.into_inner();
            assert!(req.data_boost_enabled, "data_boost_enabled should be true");
            let (_, rx) = tokio::sync::mpsc::channel(1);
            Ok(Response::from(rx))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let req = GrpcReadRequest::new()
            .set_session("projects/p/instances/i/databases/d/sessions/123")
            .set_transaction(TransactionSelector {
                selector: Some(Selector::Id(b"tx_id_2".to_vec().into())),
                ..Default::default()
            })
            .set_table("Users")
            .set_columns(vec!["Id".to_string(), "Name".to_string()])
            .set_partition_token(b"partition_token_456".to_vec());

        let partition = Partition {
            inner: PartitionedOperation::Read(req),
            gax_options: GaxRequestOptions::default(),
        };

        let _result_set = partition.with_data_boost(true).execute(&db_client).await?;

        Ok(())
    }
}
