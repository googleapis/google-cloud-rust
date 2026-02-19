use crate::client::Spanner;
use crate::model::Session;

use crate::read_context::SingleUseReadOnlyTransactionBuilder;
use crate::read_only_transaction::ReadOnlyTransactionBuilder;
use crate::read_write_transaction::ReadWriteTransactionBuilder;
use std::sync::Arc;

pub struct DatabaseClient {
    pub(crate) client: Arc<Spanner>,
    pub(crate) session: Arc<Session>,
}

impl DatabaseClient {
    pub fn single_use(&self) -> SingleUseReadOnlyTransactionBuilder {
        SingleUseReadOnlyTransactionBuilder::new(self.client.clone(), self.session.clone())
    }

    pub fn read_only_transaction(&self) -> ReadOnlyTransactionBuilder {
        ReadOnlyTransactionBuilder::new(self.client.clone(), self.session.clone())
    }

    pub fn read_write_transaction(&self) -> ReadWriteTransactionBuilder {
        ReadWriteTransactionBuilder::new(self.client.clone(), self.session.clone())
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
            .database_client("projects/test-project/instances/test-instance/databases/test-db")
            .await
            .expect("Failed to create DatabaseClient");

        assert_eq!(
            db_client.session.name,
            "projects/test-project/instances/test-instance/databases/test-db/sessions/123"
        );
        assert!(db_client.session.multiplexed);
    }
}
