use crate::client::Spanner;
use crate::model::{ExecuteSqlRequest, Session, TransactionOptions, TransactionSelector};
use std::sync::Arc;

pub struct DatabaseClient {
    pub(crate) client: Arc<Spanner>,
    pub(crate) session: Arc<Session>,
}

impl DatabaseClient {
    pub fn single_use(&self) -> SingleUseReadOnlyTransaction {
        SingleUseReadOnlyTransaction {
            client: Arc::clone(&self.client),
            session: Arc::clone(&self.session),
        }
    }
}

pub struct SingleUseReadOnlyTransaction {
    client: Arc<Spanner>,
    session: Arc<Session>,
}

impl SingleUseReadOnlyTransaction {
    pub async fn execute_query(&self, sql: impl Into<String>) -> Result<crate::result_set::ResultSet, crate::Error> {
        let mut request = ExecuteSqlRequest::new();
        request.session = self.session.name.clone();
        request.sql = sql.into();
        
        use crate::model::transaction_options::{ReadOnly, Mode};
        use crate::model::transaction_selector::Selector;

        let mut tx_options = TransactionOptions::new();
        let mut read_only = ReadOnly::new();
        read_only.return_read_timestamp = true;
        // The default strong concurrency does not set any of the oneof fields in `TimestampBound`.
        tx_options.mode = Some(Mode::ReadOnly(Box::new(read_only)));
        
        let mut tx_selector = TransactionSelector::new();
        tx_selector.selector = Some(Selector::SingleUse(Box::new(tx_options)));
        request.transaction = Some(tx_selector);

        let stream = self.client.execute_streaming_sql(request).send().await?;
        Ok(crate::result_set::ResultSet::new(stream))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
    use spanner_grpc_mock::{MockSpanner, start};

    #[tokio::test]
    async fn test_database_client_new_multiplexed() {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().once().returning(|req| {
            let req = req.into_inner();
            assert!(req.session.is_some());
            assert!(req.session.as_ref().unwrap().multiplexed);
            
            Ok(gaxi::grpc::tonic::Response::new(spanner_grpc_mock::google::spanner::v1::Session {
                name: "projects/test-project/instances/test-instance/databases/test-db/sessions/123".to_string(),
                multiplexed: true,
                ..Default::default()
            }))
        });

        let (address, _server) = start("0.0.0.0:0", mock).await.expect("Failed to start mock server");
        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let db_client = spanner.database_client(
            "projects/test-project/instances/test-instance/databases/test-db",
        ).await.expect("Failed to create DatabaseClient");

        assert_eq!(db_client.session.name, "projects/test-project/instances/test-instance/databases/test-db/sessions/123");
        assert!(db_client.session.multiplexed);
    }

    #[tokio::test]
    async fn test_single_use_execute_query() {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(spanner_grpc_mock::google::spanner::v1::Session {
                name: "projects/test-project/instances/test-instance/databases/test-db/sessions/123".to_string(),
                multiplexed: true,
                ..Default::default()
            }))
        });

        mock.expect_execute_streaming_sql().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.session, "projects/test-project/instances/test-instance/databases/test-db/sessions/123");
            assert_eq!(req.sql, "SELECT 1");
            
            assert!(req.transaction.is_some());
            let selector = req.transaction.as_ref().unwrap().selector.as_ref().unwrap();
            match selector {
                spanner_grpc_mock::google::spanner::v1::transaction_selector::Selector::SingleUse(tx_opts) => {
                    let mode = tx_opts.mode.as_ref().unwrap();
                    match mode {
                        spanner_grpc_mock::google::spanner::v1::transaction_options::Mode::ReadOnly(ro) => {
                            assert!(ro.return_read_timestamp);
                        }
                        _ => panic!("Expected ReadOnly mode"),
                    }
                }
                _ => panic!("Expected SingleUse selector"),
            }

            let stream = tokio_stream::iter(vec![
                Ok(spanner_grpc_mock::google::spanner::v1::PartialResultSet {
                    metadata: None,
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

        let (address, _server) = start("0.0.0.0:0", mock).await.expect("Failed to start mock server");
        let client = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let db_client = client.database_client(
            "projects/test-project/instances/test-instance/databases/test-db",
        ).await.expect("Failed to create DatabaseClient");

        let tx = db_client.single_use();
        let mut rs = tx.execute_query("SELECT 1").await.expect("Failed to call execute_query");

        let row1 = rs
            .next()
            .await
            .expect("Failed to get next row");
        assert!(row1.is_none());
    }
}

