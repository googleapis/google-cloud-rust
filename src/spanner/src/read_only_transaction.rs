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
/// ```rust,no_run
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
    /// ```rust,no_run
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
    /// ```rust,no_run
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

        SingleUseReadOnlyTransaction {
            client: self.client,
            transaction_options: TransactionOptions {
                mode: Some(crate::model::transaction_options::Mode::ReadOnly(Box::new(
                    read_only,
                ))),
                ..Default::default()
            },
        }
    }
}

/// A single-use read-only transaction. A single-use read-only transaction is the most
/// efficient way to execute a single query or read operation.
///
/// # Example
/// ```rust,no_run
/// # use google_cloud_spanner::client::Spanner;
/// # use google_cloud_spanner::client::Statement;
/// # async fn run(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
/// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
/// let tx = db_client.single_use().build();
/// let rs = tx.execute_query(Statement::new("SELECT 1")).await?;
/// # Ok(())
/// # }
/// ```
pub struct SingleUseReadOnlyTransaction {
    client: DatabaseClient,
    transaction_options: TransactionOptions,
}

impl SingleUseReadOnlyTransaction {
    /// Executes a query.
    ///
    /// # Example
    /// ```rust,no_run
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::client::Statement;
    /// # async fn run(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let tx = db_client.single_use().build();
    /// let mut rs = tx.execute_query(Statement::new("SELECT 1")).await?;
    /// while let Some(row) = rs.next().await {
    ///     let _row = row?;
    ///     // process row
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute_query(&self, statement: Statement) -> crate::Result<ResultSet> {
        let request = crate::model::ExecuteSqlRequest {
            session: self.client.session.name.clone(),
            transaction: Some(crate::model::TransactionSelector {
                selector: Some(crate::model::transaction_selector::Selector::SingleUse(
                    Box::new(self.transaction_options.clone()),
                )),
                ..Default::default()
            }),
            sql: statement.sql,
            ..Default::default()
        };

        let stream = self
            .client
            .spanner
            // TODO: make request options configurable
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
    fn test_auto_traits() {
        static_assertions::assert_impl_all!(SingleUseReadOnlyTransactionBuilder: Send, Sync);
        static_assertions::assert_impl_all!(SingleUseReadOnlyTransaction: Send, Sync);
    }

    #[tokio::test]
    async fn test_builder() {
        use crate::client::Spanner;
        use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
        use spanner_grpc_mock::MockSpanner;
        use spanner_grpc_mock::start;

        let mut mock = MockSpanner::new();
        mock.expect_create_session().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(
                spanner_grpc_mock::google::spanner::v1::Session {
                    name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                    ..Default::default()
                },
            ))
        });

        let (address, _server) = start("0.0.0.0:0", mock)
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

        let tx = db_client.single_use().build();
        let ro = match tx.transaction_options.mode {
            Some(crate::model::transaction_options::Mode::ReadOnly(ro)) => ro,
            _ => panic!("Expected ReadOnly mode"),
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
        let ro2 = match tx2.transaction_options.mode {
            Some(crate::model::transaction_options::Mode::ReadOnly(ro)) => ro,
            _ => panic!("Expected ReadOnly mode"),
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
    async fn test_execute_query() {
        use crate::client::Spanner;
        use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
        use spanner_grpc_mock::MockSpanner;
        use spanner_grpc_mock::google::spanner::v1 as mock_v1;
        use spanner_grpc_mock::start;

        let mut mock = MockSpanner::new();
        mock.expect_create_session().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(mock_v1::Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        mock.expect_execute_streaming_sql().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.session,
                "projects/p/instances/i/databases/d/sessions/123"
            );
            assert_eq!(req.sql, "SELECT 1");

            let result_set = mock_v1::PartialResultSet {
                metadata: Some(mock_v1::ResultSetMetadata {
                    row_type: Some(mock_v1::StructType {
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
            };
            Ok(gaxi::grpc::tonic::Response::new(Box::pin(
                tokio_stream::iter(vec![Ok(result_set)]),
            )))
        });

        let (address, _server) = start("0.0.0.0:0", mock)
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

        let tx = db_client.single_use().build();
        let mut rs = tx
            .execute_query(Statement::new("SELECT 1"))
            .await
            .expect("Failed to execute query");

        let row = rs.next().await.expect("has row").expect("has valid row");
        assert_eq!(row.raw_values().len(), 1);
        if let Some(prost_types::value::Kind::StringValue(ref s)) = row.raw_values()[0].0.kind {
            assert_eq!(s, "1");
        } else {
            panic!("Expected StringValue");
        }

        assert!(rs.next().await.is_none());
    }
}
