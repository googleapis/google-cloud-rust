use crate::client::Spanner;
use crate::model::Session;
use crate::read_context::{MultiUseTransaction};
use std::sync::Arc;

pub struct ReadOnlyTransactionBuilder {
    pub(crate) multi_use_transaction_builder: crate::read_context::MultiUseTransactionBuilder,
}


use crate::read_context::impl_read_options_builder;
impl_read_options_builder!(ReadOnlyTransactionBuilder, multi_use_transaction_builder.transaction_builder);

impl ReadOnlyTransactionBuilder {
    pub(crate) fn new(client: Arc<Spanner>, session: Arc<Session>) -> Self {
        Self {
            multi_use_transaction_builder: crate::read_context::MultiUseTransactionBuilder::new(
                client,
                session,
                crate::generated::gapic_dataplane::model::TransactionOptions {
                    mode: Some(crate::generated::gapic_dataplane::model::transaction_options::Mode::ReadOnly(Box::new(
                        crate::generated::gapic_dataplane::model::transaction_options::ReadOnly::default(),
                    ))),
                    ..Default::default()
                },
            ),
        }
    }

    pub fn with_explicit_begin_transaction(mut self, explicit: bool) -> Self {
        self.multi_use_transaction_builder = self.multi_use_transaction_builder.with_explicit_begin_transaction(explicit);
        self
    }

    pub async fn build(self) -> Result<ReadOnlyTransaction, crate::Error> {
        let transaction = self.multi_use_transaction_builder.build().await?;
        Ok(ReadOnlyTransaction { transaction })
    }
}

pub struct ReadOnlyTransaction {
    pub(crate) transaction: MultiUseTransaction,
}

impl ReadOnlyTransaction {
    pub async fn execute_query(
        &self,
        statement: impl Into<crate::statement::Statement>,
    ) -> Result<crate::result_set::ResultSet, crate::Error> {
        self.transaction.execute_query(statement).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use spanner_grpc_mock::{MockSpanner, start};
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;

    fn create_mock_stream() -> <MockSpanner as spanner_grpc_mock::google::spanner::v1::spanner_server::Spanner>::ExecuteStreamingSqlStream{
        let stream = tokio_stream::iter(vec![Ok(
            spanner_grpc_mock::google::spanner::v1::PartialResultSet {
                metadata: Some(spanner_grpc_mock::google::spanner::v1::ResultSetMetadata {
                    row_type: None,
                    transaction: None,
                    undeclared_parameters: None,
                }),
                values: vec![],
                chunked_value: false,
                resume_token: vec![],
                stats: None,
                precommit_token: None,
                cache_update: None,
                last: true,
            },
        )]);
        Box::pin(stream) as <MockSpanner as spanner_grpc_mock::google::spanner::v1::spanner_server::Spanner>::ExecuteStreamingSqlStream
    }

    fn create_mock_with_session() -> MockSpanner {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(spanner_grpc_mock::google::spanner::v1::Session {
                name: "projects/test-project/instances/test-instance/databases/test-db/sessions/123".to_string(),
                multiplexed: true,
                ..Default::default()
            }))
        });
        mock
    }

    macro_rules! setup_mock_db_client {
        ($mock:expr) => {{
            let (address, _server) = start("0.0.0.0:0", $mock)
                .await
                .expect("Failed to start mock server");
            let client = Spanner::builder()
                .with_endpoint(address)
                .with_credentials(Anonymous::new().build())
                .build()
                .await
                .expect("Failed to build client");

            let db_client = client
                .database_client("projects/test-project/instances/test-instance/databases/test-db")
                .await
                .expect("Failed to create DatabaseClient");
            (db_client, _server)
        }};
    }

    #[tokio::test]
    async fn test_explicit_multi_use_execute_query() {
        let mut mock = create_mock_with_session();

        // Mock BeginTransaction
        mock.expect_begin_transaction().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.session,
                "projects/test-project/instances/test-instance/databases/test-db/sessions/123"
            );
            assert!(req.options.is_some());
            let options = req.options.unwrap();
            match options.mode.unwrap() {
                spanner_grpc_mock::google::spanner::v1::transaction_options::Mode::ReadOnly(_) => {}
                _ => panic!("Expected ReadOnly mode"),
            }

            Ok(gaxi::grpc::tonic::Response::new(
                spanner_grpc_mock::google::spanner::v1::Transaction {
                    id: vec![5, 6, 7, 8],
                    read_timestamp: None,
                    precommit_token: None,
                },
            ))
        });

        // Mock ExecuteStreamingSql
        mock.expect_execute_streaming_sql().once().returning(|req| {
            let req = req.into_inner();
            assert!(req.transaction.is_some());
            let selector = req.transaction.as_ref().unwrap().selector.as_ref().unwrap();
            match selector {
                spanner_grpc_mock::google::spanner::v1::transaction_selector::Selector::Id(id) => {
                    assert_eq!(id, &vec![5, 6, 7, 8]);
                }
                _ => panic!("Expected Id selector"),
            }

            Ok(gaxi::grpc::tonic::Response::new(create_mock_stream()))
        });

        let (db_client, _server) = setup_mock_db_client!(mock);

        let tx = db_client
            .read_only_transaction()
            .with_explicit_begin_transaction(true)
            .build()
            .await
            .expect("Failed to start read-only transaction");

        let mut rs = tx
            .execute_query("SELECT 1")
            .await
            .expect("Failed to call execute_query");

        let row1 = rs.next().await.expect("Failed to get next row");
        assert!(row1.is_none());
    }
    #[tokio::test]
    async fn test_multi_use_execute_query() {
        let mut mock = create_mock_with_session();

        // Mock ExecuteStreamingSql (inline BeginTransaction happens here)
        mock.expect_execute_streaming_sql().times(2).returning(|req| {
            let req = req.into_inner();
            assert!(req.transaction.is_some());
            let selector = req.transaction.as_ref().unwrap().selector.as_ref().unwrap();

            let is_first_request = if let spanner_grpc_mock::google::spanner::v1::transaction_selector::Selector::Begin(_) = selector {
                true
            } else if let spanner_grpc_mock::google::spanner::v1::transaction_selector::Selector::Id(id) = selector {
                assert_eq!(id, &vec![1, 2, 3, 4]);
                false
            } else {
                panic!("Expected Begin or Id selector");
            };

            let stream = tokio_stream::iter(vec![
                Ok(spanner_grpc_mock::google::spanner::v1::PartialResultSet {
                    metadata: Some(spanner_grpc_mock::google::spanner::v1::ResultSetMetadata {
                        row_type: None,
                        transaction: if is_first_request {
                            Some(spanner_grpc_mock::google::spanner::v1::Transaction {
                                id: vec![1, 2, 3, 4],
                                read_timestamp: None,
                                precommit_token: None,
                            })
                        } else {
                            None
                        },
                        undeclared_parameters: None,
                    }),
                    values: vec![],
                    chunked_value: false,
                    resume_token: vec![],
                    stats: None,
                    precommit_token: None,
                    cache_update: None,
                    last: true,
                })
            ]);
            Ok(gaxi::grpc::tonic::Response::new(
                Box::pin(stream) as <MockSpanner as spanner_grpc_mock::google::spanner::v1::spanner_server::Spanner>::ExecuteStreamingSqlStream
            ))
        });

        let (db_client, _server) = setup_mock_db_client!(mock);

        let tx = db_client
            .read_only_transaction()
            .build()
            .await
            .expect("Failed to start read-only transaction");

        // The first execution uses `Begin(...)` and loads the ID into the mutex
        let mut rs1 = tx
            .execute_query("SELECT 1")
            .await
            .expect("Failed to call execute_query");

        let row1 = rs1.next().await.expect("Failed to get next row");
        assert!(row1.is_none());

        // The second execution uses `Id([1, 2, 3, 4])` from the resolved mutex state
        let mut rs2 = tx
            .execute_query("SELECT 2")
            .await
            .expect("Failed to call execute_query");

        let row2 = rs2.next().await.expect("Failed to get next row");
        assert!(row2.is_none());
    }
}
