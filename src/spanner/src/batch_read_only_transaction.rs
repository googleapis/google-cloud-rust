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

use crate::Error;
use crate::channel_pool::ChannelTarget;
use crate::database_client::DatabaseClient;
use crate::model::{ExecuteSqlRequest, PartitionOptions, ReadRequest};
use crate::precommit::PrecommitTokenTracker;
use crate::read_only_transaction::{
    BeginTransactionOption, MultiUseReadOnlyTransaction, MultiUseReadOnlyTransactionBuilder,
    ReadContextTransactionSelector,
};
use crate::result_set::{ResultSet, ResultSetParams, StreamOperation};
use crate::server_streaming::stream::PartialResultSetStream;
use crate::statement::Statement;
use crate::timestamp_bound::TimestampBound;
use google_cloud_gax::backoff_policy::BackoffPolicyArg;
use google_cloud_gax::options::RequestOptions as GaxRequestOptions;
use google_cloud_gax::retry_policy::RetryPolicyArg;
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::time::{Duration, Instant};

/// A builder for [BatchReadOnlyTransaction].
///
/// # Example
/// ```
/// # use google_cloud_spanner::client::Spanner;
/// # use google_cloud_spanner::transaction::TimestampBound;
/// # async fn build_tx(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
/// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
/// let read_only_transaction = db_client.batch_read_only_transaction()
///     .set_timestamp_bound(TimestampBound::strong())
///     .build()
///     .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct BatchReadOnlyTransactionBuilder {
    inner: MultiUseReadOnlyTransactionBuilder,
}

impl BatchReadOnlyTransactionBuilder {
    pub(crate) fn new(client: DatabaseClient) -> Self {
        Self {
            inner: MultiUseReadOnlyTransactionBuilder::new(client)
                .set_begin_transaction_option(BeginTransactionOption::ExplicitBegin),
        }
    }

    /// Sets the timestamp bound for the read-only transaction.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::transaction::TimestampBound;
    /// # async fn set_bound(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let builder = db_client.batch_read_only_transaction().set_timestamp_bound(TimestampBound::strong());
    /// # Ok(())
    /// # }
    /// ```
    pub fn set_timestamp_bound(self, bound: TimestampBound) -> Self {
        Self {
            inner: self.inner.set_timestamp_bound(bound),
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
/// # use google_cloud_spanner::statement::Statement;
/// # use google_cloud_spanner::model::PartitionOptions;
/// #
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
    /// # use google_cloud_spanner::statement::Statement;
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
        let selector = self.inner.context.transaction_selector.selector().await?;
        let statement = statement.into();
        let request = statement
            .clone()
            .into_partition_query_request()
            .set_session(self.inner.context.session_name.clone())
            .set_transaction(selector.clone())
            .set_partition_options(options);

        let response = self
            .inner
            .context
            .client
            .partition_query(
                request,
                crate::RequestOptions::default(),
                self.inner.context.affinity(),
            )
            .await?;

        Ok(response
            .partitions
            .into_iter()
            .map(|p| {
                let mut req = statement.clone().into_request();
                req.session = self.inner.context.session_name.clone();
                req.transaction = Some(selector.clone());
                req.partition_token = p.partition_token;

                Partition {
                    version: CURRENT_WIRE_VERSION,
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
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::key::KeySet;
    /// # use google_cloud_spanner::read::ReadRequest;
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
        let selector = self.inner.context.transaction_selector.selector().await?;
        let read = read.into();
        let request = read
            .clone()
            .into_partition_read_request()
            .set_session(self.inner.context.session_name.clone())
            .set_transaction(selector.clone())
            .set_partition_options(options);

        let response = self
            .inner
            .context
            .client
            .partition_read(
                request,
                crate::RequestOptions::default(),
                self.inner.context.affinity(),
            )
            .await?;

        Ok(response
            .partitions
            .into_iter()
            .map(|p| {
                let mut req = read.clone().into_request();
                req.session = self.inner.context.session_name.clone();
                req.transaction = Some(selector.clone());
                req.partition_token = p.partition_token;

                Partition {
                    version: CURRENT_WIRE_VERSION,
                    inner: PartitionedOperation::Read(req),
                    gax_options: GaxRequestOptions::default(),
                }
            })
            .collect())
    }
}

/// The legacy unversioned wire format (version omitted or 0) emitted prior to GA.
const LEGACY_WIRE_VERSION: u32 = 0;
/// The current wire format version for [`Partition`] serialization envelopes.
const CURRENT_WIRE_VERSION: u32 = 1;

/// Defines the segments of data to be read in a partitioned read or query.
///
/// # Example
/// ```
/// # use google_cloud_spanner::client::Spanner;
/// # use google_cloud_spanner::statement::Statement;
/// # use google_cloud_spanner::model::PartitionOptions;
/// # use google_cloud_spanner::batch::Partition;
/// # async fn run_query(spanner: Spanner) -> Result<(), Box<dyn std::error::Error>> {
/// # let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
/// # let transaction = db_client.batch_read_only_transaction().build().await?;
/// # let partitions = transaction.partition_query(
/// #     Statement::builder("SELECT * FROM Users").build(),
/// #     PartitionOptions::default(),
/// # ).await?;
/// // Coordinator serializes partition to send to a worker:
/// let serialized = serde_json::to_string(&partitions[0])?;
///
/// // Worker deserializes the partition and executes it:
/// let partition: Partition = serde_json::from_str(&serialized)?;
/// let mut result_set = partition.execute(&db_client).await?;
/// # Ok(())
/// # }
/// ```
///
/// Partitions can be serialized and processed across several different worker machines
/// or processes using any [`serde`]-compatible format (such as JSON or binary).
///
/// Serialization is wrapped in a versioned wire envelope to guarantee backward and
/// forward compatibility across client versions.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(try_from = "PartitionEnvelope")]
pub struct Partition {
    pub(crate) version: u32,
    pub(crate) inner: PartitionedOperation,
    #[serde(skip)]
    pub(crate) gax_options: GaxRequestOptions,
}

impl Partition {
    /// Sets whether Data Boost is enabled for this partition.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::statement::Statement;
    /// # use google_cloud_spanner::model::PartitionOptions;
    /// # async fn run_query(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// # let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// # let transaction = db_client.batch_read_only_transaction().build().await?;
    /// # let partitions = transaction.partition_query(Statement::builder("SELECT * FROM Users").build(), PartitionOptions::default()).await?;
    /// // On a worker receiving a partition, execute it with Data Boost:
    /// let mut result_set = partitions[0].clone()
    ///     .set_data_boost(true)
    ///     .execute(&db_client)
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn set_data_boost(mut self, enabled: bool) -> Self {
        match &mut self.inner {
            PartitionedOperation::Query(request) => request.data_boost_enabled = enabled,
            PartitionedOperation::Read(request) => request.data_boost_enabled = enabled,
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
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::statement::Statement;
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
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::key::KeySet;
    /// # use google_cloud_spanner::read::ReadRequest;
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
        if self.version != CURRENT_WIRE_VERSION {
            return Err(Error::deser(format!(
                "unsupported Partition wire format version {}, expected {}",
                self.version, CURRENT_WIRE_VERSION
            )));
        }
        match &self.inner {
            PartitionedOperation::Query(request) => {
                Self::execute_query(client, request, self.gax_options.clone()).await
            }
            PartitionedOperation::Read(request) => {
                Self::execute_read(client, request, self.gax_options.clone()).await
            }
        }
    }

    async fn execute_partition_stream<F, Fut>(
        client: &DatabaseClient,
        method_name: &'static str,
        rpc_call: F,
    ) -> crate::Result<(PartialResultSetStream, Instant)>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = crate::Result<PartialResultSetStream>>,
    {
        let attempt_start_time = Instant::now();
        match rpc_call().await {
            Ok(stream) => Ok((stream, attempt_start_time)),
            Err(e) => {
                let elapsed = attempt_start_time.elapsed();
                client
                    .o11y
                    .record_attempt(method_name, elapsed, Some(&e), None);
                client.o11y.record_operation(method_name, elapsed, Some(&e));
                Err(e)
            }
        }
    }

    async fn execute_query(
        client: &DatabaseClient,
        request: &ExecuteSqlRequest,
        gax_options: GaxRequestOptions,
    ) -> crate::Result<ResultSet> {
        let transaction = request
            .transaction
            .clone()
            .ok_or_else(|| Error::deser("missing transaction in partition query request"))?;

        let (stream, attempt_start_time) =
            Self::execute_partition_stream(client, "ExecuteStreamingSql", || {
                client
                    .execute_streaming_sql(request.clone(), gax_options.clone(), ChannelTarget::Any)
                    .send()
            })
            .await?;

        ResultSet::create(ResultSetParams {
            stream,
            transaction_selector: Some(ReadContextTransactionSelector::Fixed(transaction, None)),
            precommit_token_tracker: PrecommitTokenTracker::new_noop(),
            client: client.clone(),
            session_name: request.session.clone(),
            transaction_tag: None,
            operation: StreamOperation::Query(request.clone()),
            gax_options,
            method_name: "ExecuteStreamingSql",
            attempt_start_time: Some(attempt_start_time),
            operation_start_time: Some(attempt_start_time),
            affinity: None,
        })
        .await
    }

    async fn execute_read(
        client: &DatabaseClient,
        request: &ReadRequest,
        gax_options: GaxRequestOptions,
    ) -> crate::Result<ResultSet> {
        let transaction = request
            .transaction
            .clone()
            .ok_or_else(|| Error::deser("missing transaction in partition read request"))?;

        let (stream, attempt_start_time) =
            Self::execute_partition_stream(client, "StreamingRead", || {
                client
                    .streaming_read(request.clone(), gax_options.clone(), ChannelTarget::Any)
                    .send()
            })
            .await?;

        ResultSet::create(ResultSetParams {
            stream,
            transaction_selector: Some(ReadContextTransactionSelector::Fixed(transaction, None)),
            precommit_token_tracker: PrecommitTokenTracker::new_noop(),
            client: client.clone(),
            session_name: request.session.clone(),
            transaction_tag: None,
            operation: StreamOperation::Read(request.clone()),
            gax_options,
            method_name: "StreamingRead",
            attempt_start_time: Some(attempt_start_time),
            operation_start_time: Some(attempt_start_time),
            affinity: None,
        })
        .await
    }

    fn validate_operation(operation: &PartitionedOperation) -> Result<(), &'static str> {
        match operation {
            PartitionedOperation::Query(request) => Self::validate_query_request(request),
            PartitionedOperation::Read(request) => Self::validate_read_request(request),
        }
    }

    fn validate_query_request(request: &ExecuteSqlRequest) -> Result<(), &'static str> {
        if request.session.is_empty() {
            return Err("missing session in partition query request");
        }
        if request.transaction.is_none() {
            return Err("missing transaction in partition query request");
        }
        if request.partition_token.is_empty() {
            return Err("missing partition token in partition query request");
        }
        if request.sql.is_empty() {
            return Err("missing sql in partition query request");
        }
        Ok(())
    }

    fn validate_read_request(request: &ReadRequest) -> Result<(), &'static str> {
        if request.session.is_empty() {
            return Err("missing session in partition read request");
        }
        if request.transaction.is_none() {
            return Err("missing transaction in partition read request");
        }
        if request.partition_token.is_empty() {
            return Err("missing partition token in partition read request");
        }
        if request.table.is_empty() {
            return Err("missing table in partition read request");
        }
        Ok(())
    }
}

#[derive(Deserialize)]
struct PartitionEnvelope {
    #[serde(default)]
    version: u32,
    inner: PartitionedOperation,
}

impl TryFrom<PartitionEnvelope> for Partition {
    type Error = String;

    fn try_from(envelope: PartitionEnvelope) -> Result<Self, Self::Error> {
        match envelope.version {
            LEGACY_WIRE_VERSION | CURRENT_WIRE_VERSION => (),
            unsupported => {
                return Err(format!(
                    "unsupported Partition wire format version {unsupported}, expected {CURRENT_WIRE_VERSION}"
                ));
            }
        }
        Self::validate_operation(&envelope.inner).map_err(ToString::to_string)?;
        Ok(Partition {
            version: CURRENT_WIRE_VERSION,
            inner: envelope.inner,
            gax_options: GaxRequestOptions::default(),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) enum PartitionedOperation {
    Query(ExecuteSqlRequest),
    Read(ReadRequest),
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::key::KeySet;
    use crate::model::transaction_selector::Selector;
    use crate::model::{ExecuteSqlRequest, ReadRequest as GrpcReadRequest, TransactionSelector};
    use crate::read::ReadRequest as SpannerReadRequest;
    use crate::read_only_transaction::tests::{create_session_mock, setup_db_client};
    use crate::statement::Statement;
    use crate::transaction::TimestampBound;
    use gaxi::grpc::tonic::Response;
    use google_cloud_test_macros::tokio_test_no_panics;
    use prost_types::Timestamp;
    use serde::de::DeserializeOwned;
    use serde_json::Value as JsonValue;
    use spanner_grpc_mock::google::spanner::v1::{
        PartialResultSet, Partition as MockPartition, PartitionResponse, ResultSetMetadata,
        StructType, Transaction,
    };
    use static_assertions::assert_impl_all;
    use std::fmt::Debug;

    #[test]
    fn auto_traits() {
        assert_impl_all!(BatchReadOnlyTransactionBuilder: Debug, Send, Sync);
        assert_impl_all!(BatchReadOnlyTransaction: Debug, Send, Sync);
        assert_impl_all!(Partition: Debug, Send, Sync, Clone, Serialize, DeserializeOwned);
    }

    #[test]
    fn serialize_partition_skips_gax_options() -> anyhow::Result<()> {
        let req = ExecuteSqlRequest::new()
            .set_session("projects/p/instances/i/databases/d/sessions/123")
            .set_transaction(TransactionSelector {
                selector: Some(Selector::Id(b"tx_id_1".to_vec().into())),
                ..Default::default()
            })
            .set_sql("SELECT 1")
            .set_partition_token(b"token".to_vec());

        let mut gax_options = GaxRequestOptions::default();
        gax_options.set_attempt_timeout(Duration::from_secs(5));
        let partition = Partition {
            version: CURRENT_WIRE_VERSION,
            inner: PartitionedOperation::Query(req),
            gax_options,
        };

        let serialized = serde_json::to_string(&partition)?;
        let deserialized: Partition = serde_json::from_str(&serialized)?;

        // Verify that gax_options was NOT preserved (it uses default, which is None timeout)
        assert_eq!(*deserialized.gax_options.attempt_timeout(), None);

        Ok(())
    }

    fn setup_select1() -> PartialResultSet {
        PartialResultSet {
            metadata: Some(ResultSetMetadata {
                row_type: Some(StructType {
                    fields: vec![Default::default()],
                }),
                ..Default::default()
            }),
            values: vec![prost_types::Value {
                kind: Some(prost_types::value::Kind::StringValue("1".to_string())),
            }],
            last: true,
            ..Default::default()
        }
    }

    #[tokio_test_no_panics]
    async fn partition_execute_respects_options() -> anyhow::Result<()> {
        let mut mock = create_session_mock();

        mock.expect_execute_streaming_sql().once().returning(|req| {
            let timeout = req.metadata().get("grpc-timeout");
            assert!(timeout.is_some(), "Missing grpc-timeout header");
            assert_eq!(timeout.expect("Missing grpc-timeout header"), "5000000u"); // 5 seconds in micros

            Ok(Response::from(crate::result_set::tests::adapt([Ok(
                setup_select1(),
            )])))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let req = ExecuteSqlRequest::new()
            .set_session("projects/p/instances/i/databases/d/sessions/123")
            .set_transaction(TransactionSelector {
                selector: Some(Selector::Id(b"tx_id_1".to_vec().into())),
                ..Default::default()
            })
            .set_sql("SELECT 1")
            .set_partition_token(b"token".to_vec());

        let partition = Partition {
            version: CURRENT_WIRE_VERSION,
            inner: PartitionedOperation::Query(req),
            gax_options: GaxRequestOptions::default(),
        };

        let partition = partition.with_attempt_timeout(Duration::from_secs(5));

        let _result_set = partition.execute(&db_client).await?;

        Ok(())
    }

    #[test]
    fn serialize_partition_query() -> anyhow::Result<()> {
        let req = ExecuteSqlRequest::new()
            .set_session("projects/p/instances/i/databases/d/sessions/123")
            .set_transaction(TransactionSelector {
                selector: Some(Selector::Id(b"tx_id_1".to_vec().into())),
                ..Default::default()
            })
            .set_sql("SELECT * FROM Users")
            .set_partition_token(b"partition_token_123".to_vec());

        let partition = Partition {
            version: CURRENT_WIRE_VERSION,
            inner: PartitionedOperation::Query(req),
            gax_options: GaxRequestOptions::default(),
        };

        let serialized = serde_json::to_string(&partition)?;
        let value: JsonValue = serde_json::from_str(&serialized)?;
        assert_eq!(value["version"], 1, "envelope version must be 1");
        assert!(
            value["inner"]["Query"].is_object(),
            "operation Query payload must be present"
        );

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
        let req = GrpcReadRequest::new()
            .set_session("projects/p/instances/i/databases/d/sessions/456")
            .set_transaction(TransactionSelector {
                selector: Some(Selector::Id(b"tx_id_2".to_vec().into())),
                ..Default::default()
            })
            .set_table("Users")
            .set_columns(vec!["Id"])
            .set_partition_token(b"partition_token_456".to_vec());

        let partition = Partition {
            version: CURRENT_WIRE_VERSION,
            inner: PartitionedOperation::Read(req),
            gax_options: GaxRequestOptions::default(),
        };

        let serialized = serde_json::to_string(&partition)?;
        let value: JsonValue = serde_json::from_str(&serialized)?;
        assert_eq!(value["version"], 1, "envelope version must be 1");
        assert!(
            value["inner"]["Read"].is_object(),
            "operation Read payload must be present"
        );

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

    #[tokio_test_no_panics]
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

            Ok(Response::from(crate::result_set::tests::adapt([Ok(
                setup_select1(),
            )])))
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
            version: CURRENT_WIRE_VERSION,
            inner: PartitionedOperation::Query(req),
            gax_options: GaxRequestOptions::default(),
        };

        let _result_set = partition.execute(&db_client).await?;

        Ok(())
    }

    #[tokio_test_no_panics]
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

            Ok(Response::from(crate::result_set::tests::adapt([Ok(
                setup_select1(),
            )])))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let req = GrpcReadRequest::new()
            .set_session("projects/p/instances/i/databases/d/sessions/456")
            .set_transaction(TransactionSelector {
                selector: Some(Selector::Id(b"tx_id_2".to_vec().into())),
                ..Default::default()
            })
            .set_table("Users")
            .set_columns(vec!["Id"])
            .set_partition_token(b"partition_token_456".to_vec());

        let partition = Partition {
            version: CURRENT_WIRE_VERSION,
            inner: PartitionedOperation::Read(req),
            gax_options: GaxRequestOptions::default(),
        };

        let _result_set = partition.execute(&db_client).await?;

        Ok(())
    }

    #[tokio_test_no_panics]
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
            .set_timestamp_bound(TimestampBound::strong())
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

    #[tokio_test_no_panics]
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

    #[tokio_test_no_panics]
    async fn execute_query_with_data_boost() -> anyhow::Result<()> {
        let mut mock = create_session_mock();

        mock.expect_execute_streaming_sql().once().returning(|req| {
            let req = req.into_inner();
            assert!(req.data_boost_enabled, "data_boost_enabled should be true");
            Ok(Response::from(crate::result_set::tests::adapt([Ok(
                setup_select1(),
            )])))
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
            version: CURRENT_WIRE_VERSION,
            inner: PartitionedOperation::Query(req),
            gax_options: GaxRequestOptions::default(),
        };

        let _result_set = partition.set_data_boost(true).execute(&db_client).await?;

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn execute_read_with_data_boost() -> anyhow::Result<()> {
        let mut mock = create_session_mock();

        mock.expect_streaming_read().once().returning(|req| {
            let req = req.into_inner();
            assert!(req.data_boost_enabled, "data_boost_enabled should be true");
            Ok(Response::from(crate::result_set::tests::adapt([Ok(
                setup_select1(),
            )])))
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
            version: CURRENT_WIRE_VERSION,
            inner: PartitionedOperation::Read(req),
            gax_options: GaxRequestOptions::default(),
        };

        let _result_set = partition.set_data_boost(true).execute(&db_client).await?;

        Ok(())
    }

    #[test]
    fn deserialize_partition_unsupported_version() {
        let json = r#"{
            "version": 99,
            "inner": {
                "Query": {
                    "session": "projects/p/instances/i/databases/d/sessions/123",
                    "transaction": {"id": "dHhfaWRfMQ=="},
                    "sql": "SELECT 1",
                    "partitionToken": "dG9rZW4="
                }
            }
        }"#;

        let result: Result<Partition, _> = serde_json::from_str(json);
        assert!(
            result.is_err(),
            "deserialization with unsupported version must fail"
        );
        let error_message = result
            .expect_err("deserialization with unsupported version must fail")
            .to_string();
        assert!(
            error_message.contains("unsupported Partition wire format version 99, expected 1"),
            "error message must mention unsupported version: {error_message}"
        );
    }

    #[test]
    fn deserialize_legacy_unversioned_partition() -> anyhow::Result<()> {
        let json = r#"{
            "inner": {
                "Query": {
                    "session": "projects/p/instances/i/databases/d/sessions/123",
                    "transaction": {"id": "dHhfaWRfMQ=="},
                    "sql": "SELECT 1",
                    "partitionToken": "dG9rZW4="
                }
            }
        }"#;

        let partition: Partition = serde_json::from_str(json)?;
        assert_eq!(
            partition.version, CURRENT_WIRE_VERSION,
            "legacy unversioned partition must normalize to CURRENT_WIRE_VERSION"
        );
        Ok(())
    }

    #[test]
    fn deserialize_explicit_legacy_version_0_partition() -> anyhow::Result<()> {
        let json = r#"{
            "version": 0,
            "inner": {
                "Query": {
                    "session": "projects/p/instances/i/databases/d/sessions/123",
                    "transaction": {"id": "dHhfaWRfMQ=="},
                    "sql": "SELECT 1",
                    "partitionToken": "dG9rZW4="
                }
            }
        }"#;

        let partition: Partition = serde_json::from_str(json)?;
        assert_eq!(
            partition.version, CURRENT_WIRE_VERSION,
            "explicit legacy version 0 partition must normalize to CURRENT_WIRE_VERSION"
        );
        Ok(())
    }

    #[test]
    fn deserialize_partition_missing_transaction() {
        let json = r#"{
            "version": 1,
            "inner": {
                "Query": {
                    "session": "projects/p/instances/i/databases/d/sessions/123",
                    "sql": "SELECT 1",
                    "partitionToken": "dG9rZW4="
                }
            }
        }"#;

        let result: Result<Partition, _> = serde_json::from_str(json);
        assert!(
            result.is_err(),
            "query partition without transaction must fail"
        );
        let error_message = result
            .expect_err("query partition without transaction must fail")
            .to_string();
        assert!(
            error_message.contains("missing transaction in partition query request"),
            "error message must describe missing transaction: {error_message}"
        );
    }

    #[test]
    fn deserialize_partition_missing_session() {
        let json = r#"{
            "version": 1,
            "inner": {
                "Query": {
                    "transaction": {"id": "dHhfaWRfMQ=="},
                    "sql": "SELECT 1",
                    "partitionToken": "dG9rZW4="
                }
            }
        }"#;

        let result: Result<Partition, _> = serde_json::from_str(json);
        assert!(result.is_err(), "query partition without session must fail");
        let error_message = result
            .expect_err("query partition without session must fail")
            .to_string();
        assert!(
            error_message.contains("missing session in partition query request"),
            "error message must describe missing session: {error_message}"
        );
    }

    #[test]
    fn deserialize_partition_missing_partition_token() {
        let json = r#"{
            "version": 1,
            "inner": {
                "Query": {
                    "session": "projects/p/instances/i/databases/d/sessions/123",
                    "transaction": {"id": "dHhfaWRfMQ=="},
                    "sql": "SELECT 1"
                }
            }
        }"#;

        let result: Result<Partition, _> = serde_json::from_str(json);
        assert!(
            result.is_err(),
            "query partition without partition token must fail"
        );
        let error_message = result
            .expect_err("query partition without partition token must fail")
            .to_string();
        assert!(
            error_message.contains("missing partition token in partition query request"),
            "error message must describe missing partition token: {error_message}"
        );
    }

    #[test]
    fn deserialize_partition_missing_sql() {
        let json = r#"{
            "version": 1,
            "inner": {
                "Query": {
                    "session": "projects/p/instances/i/databases/d/sessions/123",
                    "transaction": {"id": "dHhfaWRfMQ=="},
                    "partitionToken": "dG9rZW4="
                }
            }
        }"#;

        let result: Result<Partition, _> = serde_json::from_str(json);
        assert!(result.is_err(), "query partition without sql must fail");
        let error_message = result
            .expect_err("query partition without sql must fail")
            .to_string();
        assert!(
            error_message.contains("missing sql in partition query request"),
            "error message must describe missing sql: {error_message}"
        );
    }

    #[test]
    fn deserialize_partition_read_missing_table() {
        let json = r#"{
            "version": 1,
            "inner": {
                "Read": {
                    "session": "projects/p/instances/i/databases/d/sessions/123",
                    "transaction": {"id": "dHhfaWRfMQ=="},
                    "partitionToken": "dG9rZW4="
                }
            }
        }"#;

        let result: Result<Partition, _> = serde_json::from_str(json);
        assert!(result.is_err(), "read partition without table must fail");
        let error_message = result
            .expect_err("read partition without table must fail")
            .to_string();
        assert!(
            error_message.contains("missing table in partition read request"),
            "error message must describe missing table: {error_message}"
        );
    }

    #[tokio_test_no_panics]
    async fn execute_unsupported_version_returns_error() -> anyhow::Result<()> {
        let mock = create_session_mock();
        let (db_client, _server) = setup_db_client(mock).await;

        let request = ExecuteSqlRequest::new()
            .set_session("projects/p/instances/i/databases/d/sessions/123")
            .set_transaction(TransactionSelector {
                selector: Some(Selector::Id(b"tx_id_1".to_vec().into())),
                ..Default::default()
            })
            .set_sql("SELECT 1")
            .set_partition_token(b"partition_token_123".to_vec());

        let partition = Partition {
            version: 99,
            inner: PartitionedOperation::Query(request),
            gax_options: GaxRequestOptions::default(),
        };

        let result = partition.execute(&db_client).await;
        assert!(
            result.is_err(),
            "execute with unsupported version must return error"
        );
        let error = result.expect_err("execute with unsupported version must return error");
        assert!(
            error
                .to_string()
                .contains("unsupported Partition wire format version 99, expected 1"),
            "error must indicate unsupported version: {error}"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn execute_query_missing_transaction_returns_error() -> anyhow::Result<()> {
        let mock = create_session_mock();
        let (db_client, _server) = setup_db_client(mock).await;

        let request = ExecuteSqlRequest::new()
            .set_session("projects/p/instances/i/databases/d/sessions/123")
            .set_sql("SELECT 1")
            .set_partition_token(b"partition_token_123".to_vec());

        let partition = Partition {
            version: CURRENT_WIRE_VERSION,
            inner: PartitionedOperation::Query(request),
            gax_options: GaxRequestOptions::default(),
        };

        let result = partition.execute(&db_client).await;
        assert!(
            result.is_err(),
            "execute without transaction must return error"
        );
        let error = result.expect_err("execute without transaction must return error");
        assert!(
            error
                .to_string()
                .contains("missing transaction in partition query request"),
            "error must indicate missing transaction: {error}"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn execute_read_missing_transaction_returns_error() -> anyhow::Result<()> {
        let mock = create_session_mock();
        let (db_client, _server) = setup_db_client(mock).await;

        let request = GrpcReadRequest::new()
            .set_session("projects/p/instances/i/databases/d/sessions/123")
            .set_table("Users")
            .set_partition_token(b"partition_token_456".to_vec());

        let partition = Partition {
            version: CURRENT_WIRE_VERSION,
            inner: PartitionedOperation::Read(request),
            gax_options: GaxRequestOptions::default(),
        };

        let result = partition.execute(&db_client).await;
        assert!(
            result.is_err(),
            "execute without transaction must return error"
        );
        let error = result.expect_err("execute without transaction must return error");
        assert!(
            error
                .to_string()
                .contains("missing transaction in partition read request"),
            "error must indicate missing transaction: {error}"
        );

        Ok(())
    }
}
