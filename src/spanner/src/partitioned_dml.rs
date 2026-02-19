use crate::client::Spanner;
use crate::model::Session;
use crate::statement::Statement;
use std::sync::Arc;

pub struct PartitionedDmlTransactionBuilder {
    client: Arc<Spanner>,
    session: Arc<Session>,
    options: crate::generated::gapic_dataplane::model::TransactionOptions,
}

impl PartitionedDmlTransactionBuilder {
    pub(crate) fn new(client: Arc<Spanner>, session: Arc<Session>) -> Self {
        Self {
            client,
            session,
            options: crate::generated::gapic_dataplane::model::TransactionOptions {
                mode: Some(crate::generated::gapic_dataplane::model::transaction_options::Mode::PartitionedDml(Box::new(
                    crate::generated::gapic_dataplane::model::transaction_options::PartitionedDml::default(),
                ))),
                ..Default::default()
            },
        }
    }

    pub fn exclude_txn_from_change_streams(mut self, exclude: bool) -> Self {
        self.options.exclude_txn_from_change_streams = exclude;
        self
    }

    pub async fn build(self) -> Result<PartitionedDmlTransaction, crate::Error> {
        let mut request = crate::model::BeginTransactionRequest::new();
        request.session = self.session.name.clone();
        request.options = Some(self.options);

        // Begin transaction does not use the timeout for PDML usually, but we could add it if desired.
        // For now, only applying to execute_sql as per user request (ExecuteSqlRequest).
        let response = self.client.begin_transaction(request, crate::RequestOptions::default()).await?;
        let tx_selector = crate::model::TransactionSelector {
            selector: Some(crate::model::transaction_selector::Selector::Id(response.id)),
             ..Default::default()
        };
        
        Ok(PartitionedDmlTransaction {
            client: self.client,
            session: self.session,
            tx_selector,
        })
    }
}

pub struct PartitionedDmlTransaction {
    client: Arc<Spanner>,
    session: Arc<Session>,
    tx_selector: crate::model::TransactionSelector,
}

impl PartitionedDmlTransaction {
    pub async fn execute(self, statement: impl Into<Statement>) -> Result<i64, crate::Error> {
        let statement: Statement = statement.into();
        let (mut request, options) = statement.build_request(self.session.name.clone());
        request.transaction = Some(self.tx_selector);

        let result_set = self.client.execute_sql(request, options).await?;
        
        // PDML returns the lower bound of the number of modified rows in the stats.
        Ok(result_set.stats.and_then(|s| s.row_count).map(|rc| {
            match rc {
                crate::model::result_set_stats::RowCount::RowCountLowerBound(cnt) => cnt,
                crate::model::result_set_stats::RowCount::RowCountExact(cnt) => cnt,
            }
        }).unwrap_or(0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
    use spanner_grpc_mock::{MockSpanner, start};

    #[tokio::test]
    async fn test_partitioned_dml_execute() {
        let mut mock = MockSpanner::new();

        // Expect CreateSession
        mock.expect_create_session().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(spanner_grpc_mock::google::spanner::v1::Session {
                name: "projects/p/instances/i/databases/d/sessions/s".to_string(),
                ..Default::default()
            }))
        });
        
        // Expect BeginTransaction
        mock.expect_begin_transaction().once().returning(|req| {
            let req = req.into_inner();
            assert!(req.options.is_some());
             match req.options.unwrap().mode {
                Some(spanner_grpc_mock::google::spanner::v1::transaction_options::Mode::PartitionedDml(_)) => {},
                _ => panic!("Expected PartitionedDml mode"),
            }
            Ok(gaxi::grpc::tonic::Response::new(spanner_grpc_mock::google::spanner::v1::Transaction {
                id: vec![1, 2, 3],
                ..Default::default()
            }))
        });

        // Expect ExecuteSql
        mock.expect_execute_sql().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.sql, "UPDATE users SET active = true WHERE active = false");
            assert!(req.transaction.is_some());
             match req.transaction.unwrap().selector {
                Some(spanner_grpc_mock::google::spanner::v1::transaction_selector::Selector::Id(id)) => {
                    assert_eq!(id, vec![1, 2, 3]);
                },
                _ => panic!("Expected Transaction ID"),
            }
            Ok(gaxi::grpc::tonic::Response::new(spanner_grpc_mock::google::spanner::v1::ResultSet {
                stats: Some(spanner_grpc_mock::google::spanner::v1::ResultSetStats {
                     row_count: Some(spanner_grpc_mock::google::spanner::v1::result_set_stats::RowCount::RowCountLowerBound(100)),
                     ..Default::default()
                }),
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

        let db_client = spanner.database_client("projects/p/instances/i/databases/d").await.expect("Failed to create db client");

        let tx = db_client.partitioned_dml()
            .build()
            .await
            .expect("Failed to build PDML transaction");
        
        let stmt = Statement::new("UPDATE users SET active = true WHERE active = false")
            .timeout(std::time::Duration::from_secs(60));

        let count = tx.execute(stmt)
            .await
            .expect("Failed to execute PDML");
        
        assert_eq!(count, 100);
    }
}
