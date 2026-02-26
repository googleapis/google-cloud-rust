use crate::client::Spanner;
use crate::model::Session;
use crate::read_context::SingleUseReadOnlyTransactionBuilder;
use crate::read_only_transaction::ReadOnlyTransactionBuilder;
use crate::read_write_transaction::ReadWriteTransactionBuilder;
use crate::partitioned_dml::PartitionedDmlTransactionBuilder;
use std::sync::Arc;

/// A client for interacting with a specific Spanner database.
///
/// `DatabaseClient` provides methods to execute transactions and queries.
/// It holds a pool of gRPC channels to the Spanner service and a single
/// multiplexed session for the database.
///
/// # Examples
///
/// ```rust
/// # use google_cloud_spanner::client::Spanner;
/// # use google_cloud_spanner::database_client::DatabaseClient;
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let spanner = Spanner::builder().build().await?;
/// let database_client = spanner
///     .database_client("projects/my-project/instances/my-instance/databases/my-database")
///     .await?;
/// # Ok(()) }
/// ```
pub struct DatabaseClient {
    pub(crate) client: Arc<Spanner>,
    pub(crate) session: Arc<Session>,
}

impl DatabaseClient {
    /// Creates a builder for a single-use read-only transaction.
    ///
    /// Single-use read-only transactions can be used to execute a single query or read operation.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::database_client::DatabaseClient;
    /// # use google_cloud_spanner::statement::Statement;
    /// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
    /// # let spanner = Spanner::builder().build().await?;
    /// # let database_client = spanner
    /// #    .database_client("projects/my-project/instances/my-instance/databases/my-database")
    /// #    .await?;
    /// let mut rs = database_client
    ///     .single_use()
    ///     .build()
    ///     .execute_query("SELECT 1 AS MyCol")
    ///     .await?;
    ///
    /// while let Some(row) = rs.next().await {
    ///     let row = row?;
    ///     // Access data by column index
    ///     let val: i64 = row.get(0);
    ///     
    ///     // Or by column name
    ///     let val_by_name: i64 = row.get("MyCol");
    /// }
    /// # Ok(()) }
    /// ```
    pub fn single_use(&self) -> SingleUseReadOnlyTransactionBuilder {
        SingleUseReadOnlyTransactionBuilder::new(self.client.clone(), self.session.clone())
    }

    /// Creates a builder for a multi-use read-only transaction.
    ///
    /// Multi-use read-only transactions allow you to execute multiple queries or reads
    /// at a consistent snapshot of the database.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::database_client::DatabaseClient;
    /// # use google_cloud_spanner::statement::Statement;
    /// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
    /// # let spanner = Spanner::builder().build().await?;
    /// # let database_client = spanner
    /// #    .database_client("projects/my-project/instances/my-instance/databases/my-database")
    /// #    .await?;
    /// let mut tx = database_client.read_only_transaction().build().await?;
    /// let mut rs1 = tx.execute_query("SELECT * FROM MyTable1").await?;
    /// let mut rs2 = tx.execute_query("SELECT * FROM MyTable2").await?;
    ///
    /// // Read result sets
    /// while let Some(row) = rs1.next().await {
    ///     let val: i64 = row?.get("MyTable1Col");
    /// }
    /// while let Some(row) = rs2.next().await {
    ///     let val: i64 = row?.get("MyTable2Col");
    /// }
    /// # Ok(()) }
    /// ```
    pub fn read_only_transaction(&self) -> ReadOnlyTransactionBuilder {
        ReadOnlyTransactionBuilder::new(self.client.clone(), self.session.clone())
    }

    /// Creates a builder for a read-write transaction.
    ///
    /// Read-write transactions allow you to read and write data atomically.
    ///
    /// # Example
    ///
    /// ```
    /// # use google_cloud_spanner::statement::Statement;
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::value::ToValue;
    /// # use google_cloud_spanner::value::FromValue;
    /// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
    /// # let spanner = Spanner::builder().build().await?;
    /// # let database_client = spanner
    /// #    .database_client("projects/my-project/instances/my-instance/databases/my-database")
    /// #    .await?;
    /// let (row_count, commit_response) = database_client.read_write_transaction().build().await?.run(|tx| {
    ///     async move {
    ///         // 1. Read
    ///         let mut rs = tx.execute_query("SELECT SingerId FROM Singers WHERE FirstName = 'Alice'").await?;
    ///         let row = match rs.next().await {
    ///             Some(row) => row?,
    ///             None => return Ok(0),
    ///         };
    ///         let singer_id: i64 = row.get("SingerId");
    ///
    ///         // 2. Update
    ///         let rows_updated = tx.execute_update(
    ///             Statement::new("UPDATE Singers SET FirstName = 'Bob' WHERE SingerId = @id")
    ///                 .add_param("id", &singer_id)
    ///         ).await?;
    ///
    ///         Ok(rows_updated)
    ///     }
    /// }).await?;
    /// # Ok(()) }
    /// ```
    pub fn read_write_transaction(&self) -> ReadWriteTransactionBuilder {
        ReadWriteTransactionBuilder::new(self.client.clone(), self.session.clone())
    }

    /// Creates a builder for a partitioned DML transaction.
    ///
    /// Partitioned DML transactions are used for executing non-atomic bulk updates or deletes.
    /// Partitioned DML transactions are not subject to the same mutation limits as regular transactions.
    ///
    /// See also: [Partitioned DML](https://docs.cloud.google.com/spanner/docs/dml-partitioned)
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::statement::Statement;
    /// #
    /// # async fn run(spanner: Spanner) -> Result<(), Box<dyn std::error::Error>> {
    /// # let database_client = spanner
    /// #    .database_client("projects/my-project/instances/my-instance/databases/my-database")
    /// #    .await?;
    /// let stmt = Statement::new("UPDATE Singers SET Active = true WHERE Active IS NULL");
    /// let row_count = database_client.partitioned_dml().build().await?.execute(stmt).await?;
    /// println!("Lower bound of rows updated: {}", row_count);
    /// # Ok(()) }
    /// ```
    pub fn partitioned_dml(&self) -> PartitionedDmlTransactionBuilder {
        PartitionedDmlTransactionBuilder::new(self.client.clone(), self.session.clone())
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
