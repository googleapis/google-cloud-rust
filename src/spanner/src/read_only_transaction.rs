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
use crate::model::TransactionOptions;
use crate::model::transaction_options::ReadOnly;
use crate::model::transaction_options::read_only::TimestampBound as ReadOnlyTimestampBound;
use crate::result_set::ResultSet;
use crate::statement::Statement;
use crate::timestamp_bound::TimestampBound;

/// A builder for [SingleUseReadOnlyTransaction].
///
/// # Example
/// ```
/// # use google_cloud_spanner::client::Spanner;
/// # use google_cloud_spanner::client::TimestampBound;
/// # async fn build_tx(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
/// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
/// let read_only_tx = db_client.single_use()
///     .with_timestamp_bound(TimestampBound::strong())
///     .build();
/// # Ok(())
/// # }
/// ```
pub struct SingleUseReadOnlyTransactionBuilder {
    client: DatabaseClient,
    timestamp_bound: Option<TimestampBound>,
}

impl SingleUseReadOnlyTransactionBuilder {
    pub(crate) fn new(client: DatabaseClient) -> Self {
        Self {
            client,
            timestamp_bound: None,
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
    /// let builder = db_client.single_use().with_timestamp_bound(TimestampBound::strong());
    /// # Ok(())
    /// # }
    /// ```
    /// When reading data in Spanner in a read-only transaction, you can set a timestamp bound,
    /// which tells Spanner how to choose a timestamp at which to read the data.
    ///
    /// See <https://docs.cloud.google.com/spanner/docs/timestamp-bounds> for more information.
    pub fn with_timestamp_bound(mut self, bound: TimestampBound) -> Self {
        self.timestamp_bound = Some(bound);
        self
    }

    /// Builds the [SingleUseReadOnlyTransaction].
    ///
    /// # Example
    ///
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # async fn build(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let tx = db_client.single_use().build();
    /// # Ok(())
    /// # }
    /// ```
    pub fn build(self) -> SingleUseReadOnlyTransaction {
        let mut read_only = ReadOnly::default();
        if let Some(bound) = self.timestamp_bound {
            read_only.timestamp_bound = Some(bound.0);
        } else {
            read_only.timestamp_bound = Some(ReadOnlyTimestampBound::Strong(true));
        }

        let transaction_options = TransactionOptions {
            mode: Some(crate::model::transaction_options::Mode::ReadOnly(Box::new(
                read_only,
            ))),
            ..Default::default()
        };

        let transaction_selector = crate::model::TransactionSelector {
            selector: Some(crate::model::transaction_selector::Selector::SingleUse(
                Box::new(transaction_options),
            )),
            ..Default::default()
        };

        SingleUseReadOnlyTransaction {
            context: ReadContext::new(self.client, transaction_selector),
        }
    }
}

/// A single-use read-only transaction. A single-use read-only transaction is the most
/// efficient way to execute a single query or read operation.
///
/// # Example
/// ```
/// # use google_cloud_spanner::client::Spanner;
/// # use google_cloud_spanner::client::Statement;
/// # async fn run(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
/// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
/// let tx = db_client.single_use().build();
/// let stmt = Statement::builder("SELECT * FROM users WHERE id = @id")
///     .add_param("id", &42)
///     .build();
/// let rs = tx.execute_query(stmt).await?;
/// # Ok(())
/// # }
/// ```
pub struct SingleUseReadOnlyTransaction {
    context: ReadContext,
}

impl SingleUseReadOnlyTransaction {
    /// Executes a query.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::client::Statement;
    /// # async fn run(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let tx = db_client.single_use().build();
    /// let stmt = Statement::builder("SELECT * FROM users WHERE id = @id")
    ///     .add_param("id", &42)
    ///     .build();
    /// let mut rs = tx.execute_query(stmt).await?;
    /// while let Some(row) = rs.next().await {
    ///     let _row = row?;
    ///     // process row
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute_query(&self, statement: impl Into<Statement>) -> crate::Result<ResultSet> {
        self.context.execute_query(statement).await
    }
}

/// A builder for [MultiUseReadOnlyTransaction].
///
/// # Example
/// ```
/// # use google_cloud_spanner::client::Spanner;
/// # use google_cloud_spanner::client::TimestampBound;
/// # async fn build_tx(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
/// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
/// let read_only_tx = db_client.read_only_transaction()
///     .with_timestamp_bound(TimestampBound::strong())
///     .build()
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct MultiUseReadOnlyTransactionBuilder {
    client: DatabaseClient,
    timestamp_bound: Option<TimestampBound>,
}

impl MultiUseReadOnlyTransactionBuilder {
    pub(crate) fn new(client: DatabaseClient) -> Self {
        Self {
            client,
            timestamp_bound: None,
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
    /// let builder = db_client.read_only_transaction().with_timestamp_bound(TimestampBound::strong());
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_timestamp_bound(mut self, bound: TimestampBound) -> Self {
        self.timestamp_bound = Some(bound);
        self
    }

    /// Builds the [MultiUseReadOnlyTransaction] and starts the transaction
    /// by calling the `BeginTransaction` RPC.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # async fn build(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let tx = db_client.read_only_transaction().build().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn build(self) -> crate::Result<MultiUseReadOnlyTransaction> {
        let mut read_only = ReadOnly::default();
        if let Some(bound) = self.timestamp_bound {
            read_only.timestamp_bound = Some(bound.0);
        } else {
            read_only.timestamp_bound = Some(ReadOnlyTimestampBound::Strong(true));
        }
        read_only.return_read_timestamp = true;

        let transaction_options = TransactionOptions {
            mode: Some(crate::model::transaction_options::Mode::ReadOnly(Box::new(
                read_only,
            ))),
            ..Default::default()
        };

        let request = crate::model::BeginTransactionRequest {
            session: self.client.session.name.clone(),
            options: Some(transaction_options),
            ..Default::default()
        };

        // TODO(#4972): make request options configurable
        let response = self
            .client
            .spanner
            .begin_transaction(request, crate::RequestOptions::default())
            .await?;

        let transaction_selector = crate::model::TransactionSelector {
            selector: Some(crate::model::transaction_selector::Selector::Id(
                response.id,
            )),
            ..Default::default()
        };

        Ok(MultiUseReadOnlyTransaction {
            context: ReadContext::new(self.client, transaction_selector),
            read_timestamp: response
                .read_timestamp
                .and_then(|ts| std::convert::TryInto::<time::OffsetDateTime>::try_into(ts).ok()),
        })
    }
}

/// A multi-use read-only transaction. This transaction can be used for multiple read queries
/// ensuring consistency across all queries.
///
/// # Example
/// ```
/// # use google_cloud_spanner::client::Spanner;
/// # use google_cloud_spanner::client::Statement;
/// # async fn run(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
/// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
/// let tx = db_client.read_only_transaction().build().await?;
/// let stmt1 = Statement::builder("SELECT * FROM users WHERE id = @id")
///     .add_param("id", &42)
///     .build();
/// let mut rs1 = tx.execute_query(stmt1).await?;
///
/// let stmt2 = Statement::builder("SELECT * FROM other_table")
///     .build();
/// let mut rs2 = tx.execute_query(stmt2).await?;
/// # Ok(())
/// # }
/// ```
pub struct MultiUseReadOnlyTransaction {
    context: ReadContext,
    pub(crate) read_timestamp: Option<time::OffsetDateTime>,
}

impl MultiUseReadOnlyTransaction {
    /// Returns the read timestamp chosen for the transaction.
    pub fn read_timestamp(&self) -> Option<time::OffsetDateTime> {
        self.read_timestamp
    }

    /// Executes a query using this transaction.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::client::Statement;
    /// # async fn run(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let tx = db_client.read_only_transaction().build().await?;
    /// let stmt = Statement::builder("SELECT * FROM users WHERE id = @id")
    ///     .add_param("id", &42)
    ///     .build();
    /// let mut rs = tx.execute_query(stmt).await?;
    /// while let Some(row) = rs.next().await {
    ///     let _row = row?;
    ///     // process row
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute_query(&self, statement: impl Into<Statement>) -> crate::Result<ResultSet> {
        self.context.execute_query(statement).await
    }
}

#[derive(Clone)]
pub(crate) struct ReadContext {
    client: DatabaseClient,
    transaction_selector: crate::model::TransactionSelector,
}

impl ReadContext {
    pub(crate) fn new(
        client: DatabaseClient,
        transaction_selector: crate::model::TransactionSelector,
    ) -> Self {
        Self {
            client,
            transaction_selector,
        }
    }

    pub(crate) async fn execute_query(
        &self,
        statement: impl Into<Statement>,
    ) -> crate::Result<ResultSet> {
        let statement = statement.into();

        let request = crate::model::ExecuteSqlRequest {
            session: self.client.session.name.clone(),
            transaction: Some(self.transaction_selector.clone()),
            params: statement.get_params(),
            param_types: statement.get_param_types(),
            sql: statement.sql,
            ..Default::default()
        };

        let stream = self
            .client
            .spanner
            // TODO(#4972): make request options configurable
            .execute_streaming_sql(request, crate::RequestOptions::default())
            .send()
            .await?;

        Ok(ResultSet::new(stream))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_traits() {
        static_assertions::assert_impl_all!(SingleUseReadOnlyTransactionBuilder: Send, Sync);
        static_assertions::assert_impl_all!(SingleUseReadOnlyTransaction: Send, Sync);
        static_assertions::assert_impl_all!(MultiUseReadOnlyTransactionBuilder: Send, Sync);
        static_assertions::assert_impl_all!(MultiUseReadOnlyTransaction: Send, Sync);
        static_assertions::assert_impl_all!(ReadContext: Send, Sync);
    }

    fn create_session_mock() -> spanner_grpc_mock::MockSpanner {
        let mut mock = spanner_grpc_mock::MockSpanner::new();
        mock.expect_create_session().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(
                spanner_grpc_mock::google::spanner::v1::Session {
                    name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                    ..Default::default()
                },
            ))
        });
        mock
    }

    fn setup_select1() -> spanner_grpc_mock::google::spanner::v1::PartialResultSet {
        spanner_grpc_mock::google::spanner::v1::PartialResultSet {
            metadata: Some(spanner_grpc_mock::google::spanner::v1::ResultSetMetadata {
                row_type: Some(spanner_grpc_mock::google::spanner::v1::StructType {
                    fields: vec![spanner_grpc_mock::google::spanner::v1::struct_type::Field {
                        name: "".to_string(),
                        r#type: None,
                    }],
                }),
                transaction: None,
                undeclared_parameters: None,
            }),
            values: vec![prost_types::Value {
                kind: Some(prost_types::value::Kind::StringValue("1".to_string())),
            }],
            chunked_value: false,
            resume_token: vec![],
            stats: None,
            precommit_token: None,
            cache_update: None,
            last: true,
        }
    }

    async fn setup_db_client(
        mock: spanner_grpc_mock::MockSpanner,
    ) -> (DatabaseClient, tokio::task::JoinHandle<()>) {
        use crate::client::Spanner;
        use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
        let (address, server) = spanner_grpc_mock::start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");
        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let db_client = spanner
            .database_client("projects/p/instances/i/databases/d")
            .build()
            .await
            .expect("Failed to create DatabaseClient");

        (db_client, server)
    }

    async fn assert_select1_result_set(mut rs: crate::result_set::ResultSet) {
        let row = rs.next().await.expect("has row").expect("has valid row");
        assert_eq!(row.raw_values().len(), 1);
        if let Some(prost_types::value::Kind::StringValue(ref s)) = row.raw_values()[0].0.kind {
            assert_eq!(s, "1");
        } else {
            panic!("Expected StringValue");
        }

        assert!(rs.next().await.is_none());
    }

    #[tokio::test]
    async fn single_use_builder() {
        let mock = create_session_mock();

        let (db_client, _server) = setup_db_client(mock).await;

        let tx = db_client.single_use().build();
        let ro = match tx.context.transaction_selector.selector {
            Some(crate::model::transaction_selector::Selector::SingleUse(opts)) => {
                match opts.mode {
                    Some(crate::model::transaction_options::Mode::ReadOnly(ro)) => ro,
                    _ => panic!("Expected ReadOnly mode"),
                }
            }
            _ => panic!("Expected SingleUse selector"),
        };
        assert_eq!(
            ro.timestamp_bound,
            Some(crate::model::transaction_options::read_only::TimestampBound::Strong(true))
        );

        let tx2 = db_client
            .single_use()
            .with_timestamp_bound(crate::timestamp_bound::TimestampBound::max_staleness(
                std::time::Duration::from_secs(10),
            ))
            .build();
        let ro2 = match tx2.context.transaction_selector.selector {
            Some(crate::model::transaction_selector::Selector::SingleUse(opts)) => {
                match opts.mode {
                    Some(crate::model::transaction_options::Mode::ReadOnly(ro)) => ro,
                    _ => panic!("Expected ReadOnly mode"),
                }
            }
            _ => panic!("Expected SingleUse selector"),
        };
        assert_eq!(
            ro2.timestamp_bound,
            Some(
                crate::model::transaction_options::read_only::TimestampBound::MaxStaleness(
                    Box::new(wkt::Duration::new(10, 0).unwrap())
                )
            )
        );
    }

    #[tokio::test]
    async fn execute_single_query() {
        use crate::client::Statement;

        let mut mock = create_session_mock();

        mock.expect_execute_streaming_sql().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.session,
                "projects/p/instances/i/databases/d/sessions/123"
            );
            assert_eq!(req.sql, "SELECT 1");

            Ok(gaxi::grpc::tonic::Response::new(Box::pin(
                tokio_stream::iter(vec![Ok(setup_select1())]),
            )))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let tx = db_client.single_use().build();
        let rs = tx
            .execute_query(Statement::builder("SELECT 1").build())
            .await
            .expect("Failed to execute query");

        assert_select1_result_set(rs).await;
    }

    #[tokio::test]
    async fn execute_multi_query() {
        use crate::client::Statement;
        use spanner_grpc_mock::google::spanner::v1 as mock_v1;

        let mut mock = create_session_mock();

        mock.expect_begin_transaction().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.session,
                "projects/p/instances/i/databases/d/sessions/123"
            );
            Ok(gaxi::grpc::tonic::Response::new(mock_v1::Transaction {
                id: vec![1, 2, 3],
                read_timestamp: Some(prost_types::Timestamp {
                    seconds: 123456789,
                    nanos: 0,
                }),
                precommit_token: None,
            }))
        });

        mock.expect_execute_streaming_sql()
            .times(2)
            .returning(|req| {
                let req = req.into_inner();
                assert_eq!(
                    req.session,
                    "projects/p/instances/i/databases/d/sessions/123"
                );
                assert_eq!(
                    req.transaction.unwrap().selector.unwrap(),
                    mock_v1::transaction_selector::Selector::Id(vec![1, 2, 3])
                );

                Ok(gaxi::grpc::tonic::Response::new(Box::pin(
                    tokio_stream::iter(vec![Ok(setup_select1())]),
                )))
            });

        let (db_client, _server) = setup_db_client(mock).await;

        let tx = db_client
            .read_only_transaction()
            .build()
            .await
            .expect("Failed to start tx");
        assert_eq!(tx.read_timestamp().unwrap().unix_timestamp(), 123456789);

        for _ in 0..2 {
            let rs = tx
                .execute_query(Statement::builder("SELECT 1").build())
                .await
                .expect("Failed to execute query");

            assert_select1_result_set(rs).await;
        }
    }
}
