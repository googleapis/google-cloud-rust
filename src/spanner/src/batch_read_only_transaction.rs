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
    BeginTransactionOption, MultiUseReadOnlyTransaction, MultiUseReadOnlyTransactionBuilder,
    ReadContextTransactionSelector,
};
use crate::result_set::{ResultSet, StreamOperation};
use crate::statement::Statement;
use crate::timestamp_bound::TimestampBound;

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
                .with_begin_transaction_option(BeginTransactionOption::ExplicitBegin),
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
/// # use google_cloud_spanner::PartitionOptions;
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
    /// # use google_cloud_spanner::PartitionOptions;
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
        let selector = self.inner.context.transaction_selector.selector().await?;
        let statement = statement.into();
        let request = statement
            .clone()
            .into_partition_query_request()
            .set_session(self.inner.context.client.session.name.clone())
            .set_transaction(selector.clone())
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
            .map(|p| Partition {
                inner: PartitionedOperation::Query {
                    partition_token: p.partition_token,
                    transaction_selector: selector.clone(),
                    session_name: self.inner.context.client.session.name.clone(),
                    statement: statement.clone(),
                },
            })
            .collect())
    }

    /// Creates a set of partitions that can be used to execute a read in parallel.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::{KeySet, Spanner};
    /// # use google_cloud_spanner::client::ReadRequest;
    /// # use google_cloud_spanner::PartitionOptions;
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
        let selector = self.inner.context.transaction_selector.selector().await?;
        let read = read.into();
        let request = read
            .clone()
            .into_partition_read_request()
            .set_session(self.inner.context.client.session.name.clone())
            .set_transaction(selector.clone())
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
            .map(|p| Partition {
                inner: PartitionedOperation::Read {
                    partition_token: p.partition_token,
                    transaction_selector: selector.clone(),
                    session_name: self.inner.context.client.session.name.clone(),
                    read_request: read.clone(),
                },
            })
            .collect())
    }
}

/// Defines the segments of data to be read in a partitioned read or query.
/// These partitions can be serialized and processed across several
/// different machines or processes.
#[derive(Clone, Debug)]
pub struct Partition {
    pub(crate) inner: PartitionedOperation,
}

impl Partition {
    /// Executes this partition and returns a [ResultSet] that
    /// contains the rows that belong to this partition.
    ///
    /// # Example: executing a query partition
    /// ```
    /// # use google_cloud_spanner::client::{Spanner, Statement};
    /// # use google_cloud_spanner::PartitionOptions;
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
    /// # use google_cloud_spanner::PartitionOptions;
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
            PartitionedOperation::Query {
                partition_token,
                transaction_selector,
                session_name,
                statement,
            } => {
                Self::execute_query(
                    client,
                    partition_token,
                    transaction_selector,
                    session_name,
                    statement,
                )
                .await
            }
            PartitionedOperation::Read {
                partition_token,
                transaction_selector,
                session_name,
                read_request,
            } => {
                Self::execute_read(
                    client,
                    partition_token,
                    transaction_selector,
                    session_name,
                    read_request,
                )
                .await
            }
        }
    }

    async fn execute_query(
        client: &DatabaseClient,
        partition_token: &prost::bytes::Bytes,
        transaction_selector: &crate::model::TransactionSelector,
        session_name: &str,
        statement: &Statement,
    ) -> crate::Result<ResultSet> {
        let request = statement
            .clone()
            .into_request()
            .set_session(session_name.to_string())
            .set_transaction(transaction_selector.clone())
            .set_partition_token(partition_token.clone());

        let stream = client
            .spanner
            // TODO(#4972): make request options configurable
            .execute_streaming_sql(request.clone(), crate::RequestOptions::default())
            .send()
            .await?;

        Ok(ResultSet::new(
            stream,
            Some(ReadContextTransactionSelector::Fixed(
                transaction_selector.clone(),
                None,
            )),
            PrecommitTokenTracker::new_noop(),
            client.clone(),
            StreamOperation::Query(request),
        ))
    }

    async fn execute_read(
        client: &DatabaseClient,
        partition_token: &prost::bytes::Bytes,
        transaction_selector: &crate::model::TransactionSelector,
        session_name: &str,
        read_request: &crate::read::ReadRequest,
    ) -> crate::Result<ResultSet> {
        let request = read_request
            .clone()
            .into_request()
            .set_session(session_name.to_string())
            .set_transaction(transaction_selector.clone())
            .set_partition_token(partition_token.clone());

        let stream = client
            .spanner
            // TODO(#4972): make request options configurable
            .streaming_read(request.clone(), crate::RequestOptions::default())
            .send()
            .await?;

        Ok(ResultSet::new(
            stream,
            Some(ReadContextTransactionSelector::Fixed(
                transaction_selector.clone(),
                None,
            )),
            PrecommitTokenTracker::new_noop(),
            client.clone(),
            StreamOperation::Read(request),
        ))
    }
}

#[derive(Clone, Debug)]
pub(crate) enum PartitionedOperation {
    Query {
        partition_token: prost::bytes::Bytes,
        transaction_selector: crate::model::TransactionSelector,
        session_name: String,
        statement: Statement,
    },
    Read {
        partition_token: prost::bytes::Bytes,
        transaction_selector: crate::model::TransactionSelector,
        session_name: String,
        read_request: crate::read::ReadRequest,
    },
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::client::Statement;
    use crate::client::{KeySet, ReadRequest as SpannerReadRequest, TimestampBound};
    use crate::model::TransactionSelector;
    use crate::model::transaction_selector::Selector;
    use crate::read_only_transaction::tests::{create_session_mock, setup_db_client};
    use gaxi::grpc::tonic::Response;
    use prost_types::Timestamp;
    use spanner_grpc_mock::google::spanner::v1::{
        Partition as MockPartition, PartitionResponse, Transaction,
    };

    #[test]
    fn auto_traits() {
        static_assertions::assert_impl_all!(BatchReadOnlyTransactionBuilder: Send, Sync);
        static_assertions::assert_impl_all!(BatchReadOnlyTransaction: Send, Sync, std::fmt::Debug);
        static_assertions::assert_impl_all!(Partition: Send, Sync, std::fmt::Debug);
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

            Ok(Response::new(Box::pin(tokio_stream::iter(vec![]))))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let partition = Partition {
            inner: PartitionedOperation::Query {
                partition_token: b"partition_token_123".to_vec().into(),
                transaction_selector: TransactionSelector {
                    selector: Some(Selector::Id(b"tx_id_1".to_vec().into())),
                    ..Default::default()
                },
                session_name: "projects/p/instances/i/databases/d/sessions/123".into(),
                statement: Statement::builder("SELECT * FROM Users").build(),
            },
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

            Ok(Response::new(Box::pin(tokio_stream::iter(vec![]))))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let partition = Partition {
            inner: PartitionedOperation::Read {
                partition_token: b"partition_token_456".to_vec().into(),
                transaction_selector: TransactionSelector {
                    selector: Some(Selector::Id(b"tx_id_2".to_vec().into())),
                    ..Default::default()
                },
                session_name: "projects/p/instances/i/databases/d/sessions/456".into(),
                read_request: SpannerReadRequest::builder("Users", vec!["Id"])
                    .with_keys(KeySet::all())
                    .build(),
            },
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
            PartitionedOperation::Query {
                partition_token,
                statement,
                ..
            } => {
                assert_eq!(partition_token.as_ref(), &[10]);
                assert_eq!(statement.sql, "SELECT 1");
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
            PartitionedOperation::Read {
                partition_token,
                read_request,
                ..
            } => {
                assert_eq!(partition_token.as_ref(), &[30]);
                assert_eq!(read_request.table, "Users");
            }
            _ => panic!("Expected Read partition"),
        }
        Ok(())
    }
}
