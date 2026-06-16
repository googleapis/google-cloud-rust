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
use crate::connection::SavepointSupport;
use crate::connection::batch::{ConnectionBatch, DdlBatch};
use crate::connection::connectionproperties::{
    AUTOCOMMIT, READ_ONLY_STALENESS, READONLY, RETRY_ABORTS_INTERNALLY, SAVEPOINT_SUPPORT,
    TRANSACTION_TAG, get_registry,
};
use crate::connection::connectionstate::ConnectionState;
use crate::connection::parser::{ClientSideCommand, parse_statement};
use crate::connection::pool::{ClientPool, ClientPoolKey, parse_dsn};
use crate::connection::statements::StatementStatus;
use crate::connection::transaction::{
    AutocommitTransactionUnit, ConnectionTransaction, ReadOnlyTransactionUnit,
    ReadWriteTransactionUnit,
};
use crate::database_client::DatabaseClient;
use crate::model::execute_sql_request::QueryMode;
use crate::read_only_transaction::BeginTransactionOption;
use crate::read_write_transaction::ReadWriteTransactionBuilder;
use crate::result_set::ResultSet;
use crate::statement::Statement;
use crate::timestamp_bound::TimestampBound;

/// Stateful connection to a Google Cloud Spanner database.
pub struct Connection {
    pub(crate) client: DatabaseClient,
    pub(crate) state: ConnectionState,
    pub(crate) transaction: Option<Box<dyn ConnectionTransaction>>,
    pub(crate) batch: ConnectionBatch,
    pub(crate) db_path: String,
    pub(crate) pool_key: ClientPoolKey,
    pub(crate) database_admin: Option<crate::client::DatabaseAdmin>,
    pub(crate) prepared_statements: std::collections::HashMap<String, String>,
}

/// Result of executing a SQL statement via the connection.
#[non_exhaustive]
#[derive(Debug)]
pub enum ExecutionResult {
    /// Result set returned by DQL queries.
    QueryResult(Box<ResultSet>),
    /// Number of rows modified by DML updates.
    UpdateResult(i64),
    /// Individual update counts returned by Batch DML execution.
    BatchUpdateResult(Vec<i64>),
    /// Success result for DDL, client-side, or batch statements.
    Success,
}

impl Connection {
    /// Establish a new stateful connection to Spanner using a DSN connection string.
    pub async fn connect(dsn: &str) -> Result<Self, Error> {
        let parsed = parse_dsn(dsn)?;
        let key = ClientPoolKey::from_dsn(&parsed);

        let db_path = format!(
            "projects/{}/instances/{}/databases/{}",
            parsed.project, parsed.instance, parsed.database
        );
        let client = ClientPool::get_or_create_db_client(&key, &db_path)
            .await
            .map_err(Error::connect)?;

        let dialect = client.dialect().await.map_err(Error::connect)?;
        let mut state = ConnectionState::new(dialect, get_registry(dialect));

        for (key, val) in &parsed.params {
            let key_lower = key.to_ascii_lowercase();
            if state.has_property(&key_lower) || key.contains('.') {
                state.set_startup(key, val)?;
            }
        }

        Ok(Self {
            client,
            state,
            transaction: None,
            batch: ConnectionBatch::None,
            db_path,
            pool_key: key,
            database_admin: None,
            prepared_statements: std::collections::HashMap::new(),
        })
    }

    /// Retrieve the current connection state.
    pub fn state(&self) -> &ConnectionState {
        &self.state
    }

    /// Retrieve the current connection state mutably.
    pub fn state_mut(&mut self) -> &mut ConnectionState {
        &mut self.state
    }

    /// Check if autocommit is enabled.
    pub fn autocommit(&self) -> bool {
        AUTOCOMMIT.get_value(&self.state)
    }

    /// Retrieve the read-only staleness.
    pub fn read_only_staleness(&self) -> Option<TimestampBound> {
        READ_ONLY_STALENESS.get_value(&self.state)
    }

    /// Retrieve the savepoint support configuration.
    pub fn savepoint_support(&self) -> SavepointSupport {
        SAVEPOINT_SUPPORT.get_value(&self.state)
    }

    /// Execute a SQL query, DML, DDL, client-side command, or batch statement.
    pub async fn execute(
        &mut self,
        statement: impl Into<Statement>,
    ) -> Result<ExecutionResult, Error> {
        let mut statement = statement.into();
        loop {
            let stmt_type = parse_statement(statement.sql(), self.state.dialect())?;

            if let Some(res) = self.batch.handle_statement(statement.clone(), &stmt_type)? {
                return Ok(res);
            }

            let executable = stmt_type.into_executable();
            match executable.execute(self, &mut statement).await? {
                StatementStatus::Done(result) => return Ok(result),
                StatementStatus::Continue => {
                    // statement reference was updated; loop continues
                }
            }
        }
    }

    pub(crate) async fn execute_client_side(
        &mut self,
        cmd: ClientSideCommand,
        statement: &mut Statement,
    ) -> Result<StatementStatus, Error> {
        cmd.execute(self, statement).await
    }

    pub(crate) async fn start_transaction(
        &mut self,
        readonly_override: Option<bool>,
    ) -> Result<(), Error> {
        self.state.begin();

        let is_readonly = readonly_override.unwrap_or_else(|| READONLY.get_value(&self.state));
        if is_readonly {
            let mut builder = self.client.read_only_transaction();
            if let Some(staleness) = self.read_only_staleness() {
                builder = builder.set_timestamp_bound(staleness);
            }
            let tx = builder.build().await?;
            self.transaction = Some(Box::new(ReadOnlyTransactionUnit::new(tx)));
        } else {
            let mut builder = ReadWriteTransactionBuilder::new(self.client.clone());
            let transaction_tag = TRANSACTION_TAG.get_value(&self.state);
            if let Some(ref tag) = transaction_tag {
                builder = builder.set_transaction_tag(tag.clone());
            }
            builder = builder.with_begin_transaction_option(BeginTransactionOption::ExplicitBegin);
            let tx = builder.build(None).await?;
            let retry_aborts_internally = RETRY_ABORTS_INTERNALLY.get_value(&self.state);
            let savepoint_support = self.savepoint_support();
            self.transaction = Some(Box::new(ReadWriteTransactionUnit::new(
                tx,
                self.client.clone(),
                transaction_tag,
                retry_aborts_internally,
                savepoint_support,
            )));
        }
        Ok(())
    }

    pub(crate) async fn commit_active_transaction(&mut self) -> Result<(), Error> {
        if let Some(tx) = self.transaction.take() {
            tx.commit().await?;
        }
        self.state.commit();
        Ok(())
    }

    pub(crate) async fn rollback_active_transaction(&mut self) -> Result<(), Error> {
        if let Some(tx) = self.transaction.take() {
            tx.rollback().await?;
        }
        self.state.rollback();
        Ok(())
    }

    pub(crate) async fn execute_query_or_update(
        &mut self,
        statement: Statement,
        is_update: bool,
        has_returning: bool,
    ) -> Result<ExecutionResult, Error> {
        if self.transaction.is_none() {
            if self.autocommit() {
                self.transaction = Some(Box::new(AutocommitTransactionUnit::new(
                    self.client.clone(),
                    self.state.dialect(),
                )));
            } else {
                self.start_transaction(None).await?;
            }
        }
        let mut active_tx = self.transaction.take().expect("active transaction");
        let is_plan_or_profile = matches!(
            statement.query_mode,
            Some(QueryMode::Plan) | Some(QueryMode::Profile)
        );
        let res = if is_update && !has_returning && !is_plan_or_profile {
            active_tx.execute_update(statement).await
        } else {
            let staleness = self.read_only_staleness();
            active_tx.execute_query(staleness, statement).await
        };
        self.transaction = Some(active_tx);

        if self
            .transaction
            .as_ref()
            .map(|tx| tx.is_autocommit())
            .unwrap_or(false)
        {
            self.transaction = None;
        }

        res
    }

    pub(crate) async fn get_database_admin(
        &mut self,
    ) -> Result<&crate::client::DatabaseAdmin, Error> {
        if self.database_admin.is_none() {
            let admin =
                ClientPool::get_or_create_admin_global(&self.pool_key, &self.client.spanner)
                    .await?;
            self.database_admin = Some(admin);
        }
        Ok(self.database_admin.as_ref().unwrap())
    }

    pub(crate) async fn get_database_admin_client(
        &self,
    ) -> Result<crate::client::DatabaseAdmin, Error> {
        let admin =
            ClientPool::get_or_create_admin_global(&self.pool_key, &self.client.spanner).await?;
        Ok(admin)
    }

    pub(crate) async fn execute_ddl(&mut self, ddl: String) -> Result<ExecutionResult, Error> {
        let mut batch = DdlBatch::new();
        batch.add(ddl);
        let db_path = self.db_path.clone();
        let admin = self.get_database_admin_client().await?;
        batch.run(&admin, &db_path).await
    }

    pub(crate) async fn execute_savepoint(
        &mut self,
        name: String,
    ) -> Result<ExecutionResult, Error> {
        let savepoint_support = self.savepoint_support();
        if savepoint_support == SavepointSupport::Disabled {
            return Err(Error::deser(
                "Savepoint creation is not allowed when savepoint support is disabled",
            ));
        }

        if self.transaction.is_none() {
            if self.autocommit() {
                return Err(Error::deser(
                    "Savepoints are only supported in manual transactions. Turn autocommit off.",
                ));
            } else {
                self.start_transaction(None).await?;
            }
        }

        let dialect = self.state.dialect();
        let active_tx = self.transaction.as_mut().expect("active transaction");
        active_tx.savepoint(&name, dialect)?;
        Ok(ExecutionResult::Success)
    }

    pub(crate) async fn execute_release_savepoint(
        &mut self,
        name: String,
    ) -> Result<ExecutionResult, Error> {
        let Some(active_tx) = self.transaction.as_mut() else {
            return Err(Error::deser("This connection has no active transaction"));
        };

        active_tx.release_savepoint(&name)?;
        Ok(ExecutionResult::Success)
    }

    pub(crate) async fn execute_rollback_to_savepoint(
        &mut self,
        name: String,
    ) -> Result<ExecutionResult, Error> {
        let savepoint_support = self.savepoint_support();
        if savepoint_support == SavepointSupport::Disabled {
            return Err(Error::deser("Savepoints are disabled"));
        }

        let Some(active_tx) = self.transaction.as_mut() else {
            return Err(Error::deser("This connection has no active transaction"));
        };

        active_tx
            .rollback_to_savepoint(&name, savepoint_support)
            .await?;
        Ok(ExecutionResult::Success)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::DatabaseAdmin;
    use crate::connection::Dialect;
    use crate::connection::pool::{CLIENT_POOL, ClientPoolKey, parse_dsn};
    use crate::read_only_transaction::tests::setup_db_client;
    use crate::result_set::tests::adapt;
    use crate::types::TypeCode;
    use prost_types::value::Kind;
    use spanner_grpc_mock::MockSpanner;
    use spanner_grpc_mock::google::spanner::v1::{
        CommitResponse, ExecuteBatchDmlResponse, PartialResultSet, ResultSetMetadata, Session,
        StructType, Transaction, Type, TypeCode as ProtoTypeCode, commit_request,
        struct_type::Field, transaction_selector,
    };

    fn setup_dialect_query_result(dialect: &str) -> PartialResultSet {
        PartialResultSet {
            metadata: Some(ResultSetMetadata {
                row_type: Some(StructType {
                    fields: vec![Field {
                        name: "option_value".to_string(),
                        r#type: Some(Type {
                            code: ProtoTypeCode::String as i32,
                            ..Default::default()
                        }),
                    }],
                }),
                ..Default::default()
            }),
            values: vec![prost_types::Value {
                kind: Some(prost_types::value::Kind::StringValue(dialect.to_string())),
            }],
            last: true,
            ..Default::default()
        }
    }

    fn setup_query_result_metadata() -> PartialResultSet {
        PartialResultSet {
            metadata: Some(ResultSetMetadata {
                row_type: Some(StructType {
                    fields: vec![Field {
                        name: "name".to_string(),
                        r#type: Some(Type {
                            code: ProtoTypeCode::String as i32,
                            ..Default::default()
                        }),
                    }],
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn setup_query_result_row(values: Vec<prost_types::value::Kind>) -> PartialResultSet {
        PartialResultSet {
            values: values
                .into_iter()
                .map(|kind| prost_types::Value { kind: Some(kind) })
                .collect(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_connection_autocommit_dql_query() {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        mock.expect_execute_streaming_sql().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(
                setup_dialect_query_result("POSTGRESQL"),
            )])))
        });

        mock.expect_execute_streaming_sql().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.sql, "SELECT 1");
            Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(
                PartialResultSet {
                    metadata: Some(ResultSetMetadata {
                        row_type: Some(StructType {
                            fields: vec![Default::default()],
                        }),
                        ..Default::default()
                    }),
                    values: vec![prost_types::Value {
                        kind: Some(prost_types::value::Kind::StringValue("abc".to_string())),
                    }],
                    last: true,
                    ..Default::default()
                },
            )])))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let address = db_client.spanner.config.endpoint.as_ref().unwrap();
        let dsn = format!(
            "{}/projects/p/instances/i/databases/d;useplaintext=true;readonly=true",
            address
        );

        let mut conn = Connection::connect(&dsn).await.expect("connection failed");
        assert_eq!(conn.state.dialect(), Dialect::PostgreSql);
        assert!(READONLY.get_value(&conn.state));

        let res = conn.execute("SELECT 1").await.expect("execution failed");
        if let ExecutionResult::QueryResult(mut rs) = res {
            let row = rs.next().await.unwrap().unwrap();
            let val: String = row.get(0);
            assert_eq!(val, "abc");
        } else {
            panic!("Expected query result");
        }
    }

    #[tokio::test]
    async fn test_prepare_and_execute_postgresql() {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        // Dialect check query
        mock.expect_execute_streaming_sql().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(
                setup_dialect_query_result("POSTGRESQL"),
            )])))
        });

        // Expected query executed when EXECUTE runs the prepared statement:
        mock.expect_execute_streaming_sql().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.sql, "SELECT 1 WHERE col = $1");
            let param_val = req.params.unwrap().fields.get("$1").cloned().unwrap();
            assert_eq!(
                param_val.kind.unwrap(),
                Kind::StringValue("test_val".to_string())
            );

            Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(
                PartialResultSet {
                    metadata: Some(ResultSetMetadata {
                        row_type: Some(StructType {
                            fields: vec![Default::default()],
                        }),
                        ..Default::default()
                    }),
                    values: vec![prost_types::Value {
                        kind: Some(prost_types::value::Kind::StringValue(
                            "resolved_val".to_string(),
                        )),
                    }],
                    last: true,
                    ..Default::default()
                },
            )])))
        });

        let (db_client, _server) = setup_db_client(mock).await;
        let address = db_client.spanner.config.endpoint.as_ref().unwrap();
        let dsn = format!(
            "{}/projects/p/instances/i/databases/d;useplaintext=true;readonly=true",
            address
        );

        let mut conn = Connection::connect(&dsn).await.expect("connection failed");

        // 1. Prepare statement
        let prep_res = conn
            .execute("PREPARE my_stmt AS SELECT 1 WHERE col = $1;")
            .await
            .expect("prepare failed");
        assert!(matches!(prep_res, ExecutionResult::Success));

        // 2. Execute statement with inline parameters
        let exec_stmt = Statement::builder("EXECUTE my_stmt('test_val');")
            .add_param("$1", &"ignored_val") // should be ignored
            .build();
        let exec_res = conn.execute(exec_stmt).await.expect("execute failed");

        if let ExecutionResult::QueryResult(mut rs) = exec_res {
            let row = rs.next().await.unwrap().unwrap();
            let val: String = row.get(0);
            assert_eq!(val, "resolved_val");
        } else {
            panic!("Expected query result");
        }
    }

    #[tokio::test]
    async fn test_connection_show_property() {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        // Dialect check query
        mock.expect_execute_streaming_sql().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(
                setup_dialect_query_result("POSTGRESQL"),
            )])))
        });

        let (db_client, _server) = setup_db_client(mock).await;
        let address = db_client.spanner.config.endpoint.as_ref().unwrap();
        let dsn = format!(
            "{}/projects/p/instances/i/databases/d;useplaintext=true;autocommit=false",
            address
        );

        let mut conn = Connection::connect(&dsn).await.expect("connection failed");
        assert_eq!(conn.state.dialect(), Dialect::PostgreSql);
        assert!(!AUTOCOMMIT.get_value(&conn.state));

        let res = conn
            .execute("SHOW autocommit")
            .await
            .expect("execution failed");
        if let ExecutionResult::QueryResult(mut rs) = res {
            // Check metadata
            let metadata = rs.metadata().expect("metadata available");
            assert_eq!(metadata.column_names(), &["autocommit".to_string()]);
            assert_eq!(metadata.column_types()[0].code(), TypeCode::Bool);

            // Check row value
            let row = rs.next().await.unwrap().unwrap();
            let val: bool = row.get(0);
            assert!(!val);

            assert!(rs.next().await.is_none());
        } else {
            panic!("Expected query result");
        }
    }

    #[tokio::test]
    async fn test_connection_autocommit_dml_returning() {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        // Dialect check query
        mock.expect_execute_streaming_sql().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(
                setup_dialect_query_result("GOOGLE_STANDARD_SQL"),
            )])))
        });

        // DML returning query
        mock.expect_execute_streaming_sql().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.sql, "INSERT INTO Users (id) VALUES (1) THEN RETURN id");

            // Check that it began a transaction inline
            let transaction = req
                .transaction
                .as_ref()
                .expect("transaction options required for inline begin");
            let selector = transaction.selector.as_ref().expect("selector required");
            assert!(matches!(selector, transaction_selector::Selector::Begin(_)));

            Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(
                PartialResultSet {
                    metadata: Some(ResultSetMetadata {
                        row_type: Some(StructType {
                            fields: vec![Field {
                                name: "id".to_string(),
                                r#type: Some(Type {
                                    code: ProtoTypeCode::Int64 as i32,
                                    ..Default::default()
                                }),
                            }],
                        }),
                        transaction: Some(Transaction {
                            id: vec![1, 2, 3],
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    values: vec![prost_types::Value {
                        kind: Some(prost_types::value::Kind::StringValue("1".to_string())),
                    }],
                    last: true,
                    ..Default::default()
                },
            )])))
        });

        // Commit expect
        mock.expect_commit().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.transaction,
                Some(commit_request::Transaction::TransactionId(vec![1, 2, 3]))
            );
            Ok(gaxi::grpc::tonic::Response::new(CommitResponse {
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;
        let address = db_client.spanner.config.endpoint.as_ref().unwrap();
        let dsn = format!(
            "{}/projects/p/instances/i/databases/d;useplaintext=true;autocommit=true",
            address
        );

        let mut conn = Connection::connect(&dsn).await.expect("connection failed");
        assert_eq!(conn.state.dialect(), Dialect::GoogleSql);
        assert!(AUTOCOMMIT.get_value(&conn.state));

        let res = conn
            .execute("INSERT INTO Users (id) VALUES (1) THEN RETURN id")
            .await
            .expect("execution failed");

        if let ExecutionResult::QueryResult(mut rs) = res {
            let row = rs.next().await.unwrap().unwrap();
            let val: i64 = row.get(0);
            assert_eq!(val, 1);
            assert!(rs.next().await.is_none());
        } else {
            panic!("Expected query result");
        }
    }

    fn setup_dml_response(rows_updated: i64) -> spanner_grpc_mock::google::spanner::v1::ResultSet {
        use spanner_grpc_mock::google::spanner::v1::{
            ResultSet, ResultSetMetadata, ResultSetStats, StructType, result_set_stats::RowCount,
        };
        ResultSet {
            metadata: Some(ResultSetMetadata {
                row_type: Some(StructType { fields: vec![] }),
                ..Default::default()
            }),
            stats: Some(ResultSetStats {
                row_count: Some(RowCount::RowCountExact(rows_updated)),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_retry_commit_aborted() {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        mock.expect_execute_streaming_sql().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(
                setup_dialect_query_result("GOOGLE_STANDARD_SQL"),
            )])))
        });

        mock.expect_begin_transaction().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Transaction {
                id: vec![1],
                ..Default::default()
            }))
        });

        mock.expect_execute_sql().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.sql, "UPDATE Users SET active = true WHERE id = 1");
            assert_eq!(
                req.transaction.unwrap().selector,
                Some(transaction_selector::Selector::Id(vec![1]))
            );
            Ok(gaxi::grpc::tonic::Response::new(setup_dml_response(1)))
        });

        mock.expect_commit().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.transaction,
                Some(commit_request::Transaction::TransactionId(vec![1]))
            );
            Err(gaxi::grpc::tonic::Status::aborted("Transaction aborted"))
        });

        mock.expect_begin_transaction().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Transaction {
                id: vec![2],
                ..Default::default()
            }))
        });

        mock.expect_execute_sql().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.sql, "UPDATE Users SET active = true WHERE id = 1");
            assert_eq!(
                req.transaction.unwrap().selector,
                Some(transaction_selector::Selector::Id(vec![2]))
            );
            Ok(gaxi::grpc::tonic::Response::new(setup_dml_response(1)))
        });

        mock.expect_commit().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.transaction,
                Some(commit_request::Transaction::TransactionId(vec![2]))
            );
            Ok(gaxi::grpc::tonic::Response::new(CommitResponse {
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;
        let address = db_client.spanner.config.endpoint.as_ref().unwrap();
        let dsn = format!(
            "{}/projects/p/instances/i/databases/d;useplaintext=true;autocommit=false",
            address
        );

        let mut conn = Connection::connect(&dsn).await.expect("connection failed");

        conn.execute("UPDATE Users SET active = true WHERE id = 1")
            .await
            .expect("update failed");
        conn.execute("COMMIT").await.expect("commit failed");
    }

    #[tokio::test]
    async fn test_retry_disabled_propagates_aborted() {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        mock.expect_execute_streaming_sql().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(
                setup_dialect_query_result("GOOGLE_STANDARD_SQL"),
            )])))
        });

        mock.expect_begin_transaction().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Transaction {
                id: vec![1],
                ..Default::default()
            }))
        });

        mock.expect_execute_sql().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.sql, "UPDATE Users SET active = true WHERE id = 1");
            Ok(gaxi::grpc::tonic::Response::new(setup_dml_response(1)))
        });

        mock.expect_commit()
            .once()
            .returning(|_| Err(gaxi::grpc::tonic::Status::aborted("Transaction aborted")));

        let (db_client, _server) = setup_db_client(mock).await;
        let address = db_client.spanner.config.endpoint.as_ref().unwrap();
        let dsn = format!(
            "{}/projects/p/instances/i/databases/d;useplaintext=true;autocommit=false;retry_aborts_internally=false",
            address
        );

        let mut conn = Connection::connect(&dsn).await.expect("connection failed");
        assert!(!RETRY_ABORTS_INTERNALLY.get_value(&conn.state));

        conn.execute("UPDATE Users SET active = true WHERE id = 1")
            .await
            .expect("update failed");

        let commit_res = conn.execute("COMMIT").await;
        assert!(commit_res.is_err());
        let err = commit_res.unwrap_err();
        assert_eq!(
            err.status().unwrap().code,
            google_cloud_gax::error::rpc::Code::Aborted
        );
    }

    #[tokio::test]
    async fn test_retry_update_aborted() {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        mock.expect_execute_streaming_sql().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(
                setup_dialect_query_result("GOOGLE_STANDARD_SQL"),
            )])))
        });

        mock.expect_begin_transaction().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Transaction {
                id: vec![1],
                ..Default::default()
            }))
        });

        mock.expect_execute_sql()
            .once()
            .returning(|_| Err(gaxi::grpc::tonic::Status::aborted("Transaction aborted")));

        mock.expect_begin_transaction().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Transaction {
                id: vec![2],
                ..Default::default()
            }))
        });

        mock.expect_execute_sql().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.sql, "UPDATE Users SET active = true WHERE id = 1");
            assert_eq!(
                req.transaction.unwrap().selector,
                Some(transaction_selector::Selector::Id(vec![2]))
            );
            Ok(gaxi::grpc::tonic::Response::new(setup_dml_response(1)))
        });

        mock.expect_commit().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.transaction,
                Some(commit_request::Transaction::TransactionId(vec![2]))
            );
            Ok(gaxi::grpc::tonic::Response::new(CommitResponse {
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;
        let address = db_client.spanner.config.endpoint.as_ref().unwrap();
        let dsn = format!(
            "{}/projects/p/instances/i/databases/d;useplaintext=true;autocommit=false",
            address
        );

        let mut conn = Connection::connect(&dsn).await.expect("connection failed");

        conn.execute("UPDATE Users SET active = true WHERE id = 1")
            .await
            .expect("update failed");
        conn.execute("COMMIT").await.expect("commit failed");
    }

    #[tokio::test]
    async fn test_retry_query_aborted_on_startup() {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        mock.expect_execute_streaming_sql().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(
                setup_dialect_query_result("GOOGLE_STANDARD_SQL"),
            )])))
        });

        mock.expect_begin_transaction().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Transaction {
                id: vec![1],
                ..Default::default()
            }))
        });

        mock.expect_execute_streaming_sql()
            .once()
            .returning(|_| Err(gaxi::grpc::tonic::Status::aborted("Transaction aborted")));

        mock.expect_begin_transaction().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Transaction {
                id: vec![2],
                ..Default::default()
            }))
        });

        mock.expect_execute_streaming_sql().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.sql, "SELECT name FROM Users WHERE id = 1");
            assert_eq!(
                req.transaction.unwrap().selector,
                Some(transaction_selector::Selector::Id(vec![2]))
            );
            Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(
                PartialResultSet {
                    metadata: Some(ResultSetMetadata {
                        row_type: Some(StructType {
                            fields: vec![Field {
                                name: "name".to_string(),
                                r#type: Some(Type {
                                    code: ProtoTypeCode::String as i32,
                                    ..Default::default()
                                }),
                            }],
                        }),
                        ..Default::default()
                    }),
                    values: vec![prost_types::Value {
                        kind: Some(prost_types::value::Kind::StringValue("Alice".to_string())),
                    }],
                    last: true,
                    ..Default::default()
                },
            )])))
        });

        mock.expect_commit().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.transaction,
                Some(commit_request::Transaction::TransactionId(vec![2]))
            );
            Ok(gaxi::grpc::tonic::Response::new(CommitResponse {
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;
        let address = db_client.spanner.config.endpoint.as_ref().unwrap();
        let dsn = format!(
            "{}/projects/p/instances/i/databases/d;useplaintext=true;autocommit=false",
            address
        );

        let mut conn = Connection::connect(&dsn).await.expect("connection failed");

        let res = conn
            .execute("SELECT name FROM Users WHERE id = 1")
            .await
            .expect("query failed");
        if let ExecutionResult::QueryResult(mut rs) = res {
            let row = rs.next().await.unwrap().unwrap();
            let val: String = row.get(0);
            assert_eq!(val, "Alice");
            assert!(rs.next().await.is_none());
        } else {
            panic!("Expected query result");
        }

        conn.execute("COMMIT").await.expect("commit failed");
    }

    #[tokio::test]
    async fn test_retry_query_aborted_halfway() {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        mock.expect_execute_streaming_sql().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(
                setup_dialect_query_result("GOOGLE_STANDARD_SQL"),
            )])))
        });

        mock.expect_begin_transaction().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Transaction {
                id: vec![1],
                ..Default::default()
            }))
        });

        mock.expect_execute_streaming_sql().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::from(adapt([
                Ok(PartialResultSet {
                    metadata: Some(ResultSetMetadata {
                        row_type: Some(StructType {
                            fields: vec![Field {
                                name: "name".to_string(),
                                r#type: Some(Type {
                                    code: ProtoTypeCode::String as i32,
                                    ..Default::default()
                                }),
                            }],
                        }),
                        ..Default::default()
                    }),
                    values: vec![prost_types::Value {
                        kind: Some(prost_types::value::Kind::StringValue("Alice".to_string())),
                    }],
                    resume_token: b"token1".to_vec(),
                    ..Default::default()
                }),
                Err(gaxi::grpc::tonic::Status::aborted("Transaction aborted")),
            ])))
        });

        mock.expect_begin_transaction().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Transaction {
                id: vec![2],
                ..Default::default()
            }))
        });

        mock.expect_execute_streaming_sql().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.sql, "SELECT name FROM Users");
            assert_eq!(
                req.transaction.unwrap().selector,
                Some(transaction_selector::Selector::Id(vec![2]))
            );
            Ok(gaxi::grpc::tonic::Response::from(adapt([
                Ok(PartialResultSet {
                    metadata: Some(ResultSetMetadata {
                        row_type: Some(StructType {
                            fields: vec![Field {
                                name: "name".to_string(),
                                r#type: Some(Type {
                                    code: ProtoTypeCode::String as i32,
                                    ..Default::default()
                                }),
                            }],
                        }),
                        ..Default::default()
                    }),
                    values: vec![prost_types::Value {
                        kind: Some(prost_types::value::Kind::StringValue("Alice".to_string())),
                    }],
                    resume_token: b"token1".to_vec(),
                    ..Default::default()
                }),
                Ok(PartialResultSet {
                    values: vec![prost_types::Value {
                        kind: Some(prost_types::value::Kind::StringValue("Bob".to_string())),
                    }],
                    last: true,
                    ..Default::default()
                }),
            ])))
        });

        mock.expect_commit().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.transaction,
                Some(commit_request::Transaction::TransactionId(vec![2]))
            );
            Ok(gaxi::grpc::tonic::Response::new(CommitResponse {
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;
        let address = db_client.spanner.config.endpoint.as_ref().unwrap();
        let dsn = format!(
            "{}/projects/p/instances/i/databases/d;useplaintext=true;autocommit=false",
            address
        );

        let mut conn = Connection::connect(&dsn).await.expect("connection failed");

        let res = conn
            .execute("SELECT name FROM Users")
            .await
            .expect("query failed");
        if let ExecutionResult::QueryResult(mut rs) = res {
            let row = rs.next().await.unwrap().unwrap();
            let val: String = row.get(0);
            assert_eq!(val, "Alice");

            let row = rs.next().await.unwrap().unwrap();
            let val: String = row.get(0);
            assert_eq!(val, "Bob");

            assert!(rs.next().await.is_none());
        } else {
            panic!("Expected query result");
        }

        conn.execute("COMMIT").await.expect("commit failed");
    }

    #[tokio::test]
    async fn test_retry_query_with_different_data_fails() {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        mock.expect_execute_streaming_sql().returning(|req| {
            let req = req.into_inner();
            if req.sql.contains("option_value") {
                return Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(
                    setup_dialect_query_result("GOOGLE_STANDARD_SQL"),
                )])));
            } else if req.sql == "SELECT name FROM Users" {
                let transaction = req.transaction.unwrap();
                let selector = transaction.selector.unwrap();
                if selector == transaction_selector::Selector::Id(vec![1]) {
                    return Ok(gaxi::grpc::tonic::Response::from(adapt([
                        Ok(PartialResultSet {
                            metadata: Some(ResultSetMetadata {
                                row_type: Some(StructType {
                                    fields: vec![Field {
                                        name: "name".to_string(),
                                        r#type: Some(Type {
                                            code: ProtoTypeCode::String as i32,
                                            ..Default::default()
                                        }),
                                    }],
                                }),
                                ..Default::default()
                            }),
                            values: vec![prost_types::Value {
                                kind: Some(prost_types::value::Kind::StringValue(
                                    "Alice".to_string(),
                                )),
                            }],
                            resume_token: b"token1".to_vec(),
                            ..Default::default()
                        }),
                        Err(gaxi::grpc::tonic::Status::aborted("Transaction aborted")),
                    ])));
                } else if selector == transaction_selector::Selector::Id(vec![2]) {
                    return Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(
                        PartialResultSet {
                            metadata: Some(ResultSetMetadata {
                                row_type: Some(StructType {
                                    fields: vec![Field {
                                        name: "name".to_string(),
                                        r#type: Some(Type {
                                            code: ProtoTypeCode::String as i32,
                                            ..Default::default()
                                        }),
                                    }],
                                }),
                                ..Default::default()
                            }),
                            values: vec![prost_types::Value {
                                kind: Some(prost_types::value::Kind::StringValue(
                                    "Charlie".to_string(),
                                )),
                            }],
                            resume_token: b"token1".to_vec(),
                            last: true,
                            ..Default::default()
                        },
                    )])));
                }
            }
            panic!("Unexpected query request: {:?}", req.sql);
        });

        mock.expect_begin_transaction().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Transaction {
                id: vec![1],
                ..Default::default()
            }))
        });

        mock.expect_begin_transaction().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Transaction {
                id: vec![2],
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;
        let address = db_client.spanner.config.endpoint.as_ref().unwrap();
        let dsn = format!(
            "{}/projects/p/instances/i/databases/d;useplaintext=true;autocommit=false",
            address
        );

        let mut conn = Connection::connect(&dsn).await.expect("connection failed");

        let res = conn
            .execute("SELECT name FROM Users")
            .await
            .expect("query failed");
        if let ExecutionResult::QueryResult(mut rs) = res {
            let row = rs.next().await.unwrap().unwrap();
            let val: String = row.get(0);
            assert_eq!(val, "Alice");

            let retry_res = rs.next().await;
            assert!(retry_res.is_some());
            let err = retry_res.unwrap().unwrap_err();
            assert!(
                err.to_string().contains("checksum mismatch"),
                "Expected checksum mismatch error, got: {:?}",
                err
            );
        } else {
            panic!("Expected query result");
        }
    }

    #[tokio::test]
    async fn test_retry_query_with_fewer_rows_fails() {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        mock.expect_execute_streaming_sql().returning(|req| {
            let req = req.into_inner();
            if req.sql.contains("option_value") {
                return Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(
                    setup_dialect_query_result("GOOGLE_STANDARD_SQL"),
                )])));
            } else if req.sql == "SELECT name FROM Users" {
                let transaction = req.transaction.unwrap();
                let selector = transaction.selector.unwrap();
                if selector == transaction_selector::Selector::Id(vec![1]) {
                    return Ok(gaxi::grpc::tonic::Response::from(adapt([
                        Ok(PartialResultSet {
                            metadata: Some(ResultSetMetadata {
                                row_type: Some(StructType {
                                    fields: vec![Field {
                                        name: "name".to_string(),
                                        r#type: Some(Type {
                                            code: ProtoTypeCode::String as i32,
                                            ..Default::default()
                                        }),
                                    }],
                                }),
                                ..Default::default()
                            }),
                            values: vec![prost_types::Value {
                                kind: Some(prost_types::value::Kind::StringValue(
                                    "Alice".to_string(),
                                )),
                            }],
                            resume_token: b"token1".to_vec(),
                            ..Default::default()
                        }),
                        Err(gaxi::grpc::tonic::Status::aborted("Transaction aborted")),
                    ])));
                } else if selector == transaction_selector::Selector::Id(vec![2]) {
                    return Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(
                        PartialResultSet {
                            metadata: Some(ResultSetMetadata {
                                row_type: Some(StructType {
                                    fields: vec![Field {
                                        name: "name".to_string(),
                                        r#type: Some(Type {
                                            code: ProtoTypeCode::String as i32,
                                            ..Default::default()
                                        }),
                                    }],
                                }),
                                ..Default::default()
                            }),
                            values: vec![],
                            last: true,
                            ..Default::default()
                        },
                    )])));
                }
            }
            panic!("Unexpected query request: {:?}", req.sql);
        });

        mock.expect_begin_transaction().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Transaction {
                id: vec![1],
                ..Default::default()
            }))
        });

        mock.expect_begin_transaction().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Transaction {
                id: vec![2],
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;
        let address = db_client.spanner.config.endpoint.as_ref().unwrap();
        let dsn = format!(
            "{}/projects/p/instances/i/databases/d;useplaintext=true;autocommit=false",
            address
        );

        let mut conn = Connection::connect(&dsn).await.expect("connection failed");

        let res = conn
            .execute("SELECT name FROM Users")
            .await
            .expect("query failed");
        if let ExecutionResult::QueryResult(mut rs) = res {
            let row = rs.next().await.unwrap().unwrap();
            let val: String = row.get(0);
            assert_eq!(val, "Alice");

            let retry_res = rs.next().await;
            assert!(retry_res.is_some());
            let err = retry_res.unwrap().unwrap_err();
            assert!(
                err.to_string().contains("returned fewer rows on retry"),
                "Expected fewer rows error, got: {:?}",
                err
            );
        } else {
            panic!("Expected query result");
        }
    }

    #[tokio::test]
    async fn test_retry_query_aborted_twice() {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        mock.expect_execute_streaming_sql().returning(|req| {
            let req = req.into_inner();
            if req.sql.contains("option_value") {
                return Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(
                    setup_dialect_query_result("GOOGLE_STANDARD_SQL"),
                )])));
            } else if req.sql == "SELECT name FROM Users" {
                let transaction = req.transaction.unwrap();
                let selector = transaction.selector.unwrap();
                if selector == transaction_selector::Selector::Id(vec![1]) {
                    return Ok(gaxi::grpc::tonic::Response::from(adapt([
                        Ok(PartialResultSet {
                            metadata: Some(ResultSetMetadata {
                                row_type: Some(StructType {
                                    fields: vec![Field {
                                        name: "name".to_string(),
                                        r#type: Some(Type {
                                            code: ProtoTypeCode::String as i32,
                                            ..Default::default()
                                        }),
                                    }],
                                }),
                                ..Default::default()
                            }),
                            values: vec![prost_types::Value {
                                kind: Some(prost_types::value::Kind::StringValue(
                                    "Alice".to_string(),
                                )),
                            }],
                            resume_token: b"token1".to_vec(),
                            ..Default::default()
                        }),
                        Err(gaxi::grpc::tonic::Status::aborted("Transaction aborted")),
                    ])));
                } else if selector == transaction_selector::Selector::Id(vec![3]) {
                    return Ok(gaxi::grpc::tonic::Response::from(adapt([
                        Ok(PartialResultSet {
                            metadata: Some(ResultSetMetadata {
                                row_type: Some(StructType {
                                    fields: vec![Field {
                                        name: "name".to_string(),
                                        r#type: Some(Type {
                                            code: ProtoTypeCode::String as i32,
                                            ..Default::default()
                                        }),
                                    }],
                                }),
                                ..Default::default()
                            }),
                            values: vec![prost_types::Value {
                                kind: Some(prost_types::value::Kind::StringValue(
                                    "Alice".to_string(),
                                )),
                            }],
                            resume_token: b"token1".to_vec(),
                            ..Default::default()
                        }),
                        Ok(PartialResultSet {
                            values: vec![prost_types::Value {
                                kind: Some(prost_types::value::Kind::StringValue(
                                    "Bob".to_string(),
                                )),
                            }],
                            last: true,
                            ..Default::default()
                        }),
                    ])));
                }
            }
            panic!("Unexpected query request: {:?}", req.sql);
        });

        mock.expect_begin_transaction().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Transaction {
                id: vec![1],
                ..Default::default()
            }))
        });

        mock.expect_begin_transaction().once().returning(|_| {
            Err(gaxi::grpc::tonic::Status::aborted(
                "BeginTransaction aborted on retry",
            ))
        });

        mock.expect_begin_transaction().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Transaction {
                id: vec![3],
                ..Default::default()
            }))
        });

        mock.expect_commit().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.transaction,
                Some(commit_request::Transaction::TransactionId(vec![3]))
            );
            Ok(gaxi::grpc::tonic::Response::new(CommitResponse {
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;
        let address = db_client.spanner.config.endpoint.as_ref().unwrap();
        let dsn = format!(
            "{}/projects/p/instances/i/databases/d;useplaintext=true;autocommit=false",
            address
        );

        let mut conn = Connection::connect(&dsn).await.expect("connection failed");

        let res = conn
            .execute("SELECT name FROM Users")
            .await
            .expect("query failed");
        if let ExecutionResult::QueryResult(mut rs) = res {
            let row = rs.next().await.unwrap().unwrap();
            let val: String = row.get(0);
            assert_eq!(val, "Alice");

            let row = rs.next().await.unwrap().unwrap();
            let val: String = row.get(0);
            assert_eq!(val, "Bob");

            assert!(rs.next().await.is_none());
        } else {
            panic!("Expected query result");
        }

        conn.execute("COMMIT").await.expect("commit failed");
    }

    #[tokio::test]
    async fn test_retry_aborted_during_retry_of_failed_query() {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        mock.expect_execute_streaming_sql().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(
                setup_dialect_query_result("GOOGLE_STANDARD_SQL"),
            )])))
        });

        mock.expect_begin_transaction().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Transaction {
                id: vec![1],
                ..Default::default()
            }))
        });

        mock.expect_execute_streaming_sql()
            .once()
            .returning(|_| Err(gaxi::grpc::tonic::Status::not_found("Table not found")));

        mock.expect_execute_sql().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.sql, "UPDATE Users SET active = true WHERE id = 1");
            Ok(gaxi::grpc::tonic::Response::new(setup_dml_response(1)))
        });

        mock.expect_commit()
            .once()
            .returning(|_| Err(gaxi::grpc::tonic::Status::aborted("Transaction aborted")));

        mock.expect_begin_transaction().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Transaction {
                id: vec![2],
                ..Default::default()
            }))
        });

        mock.expect_execute_streaming_sql().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.sql, "SELECT * FROM NonExistingTable");
            Err(gaxi::grpc::tonic::Status::not_found("Table not found"))
        });

        mock.expect_execute_sql().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.sql, "UPDATE Users SET active = true WHERE id = 1");
            Ok(gaxi::grpc::tonic::Response::new(setup_dml_response(1)))
        });

        mock.expect_commit().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(CommitResponse {
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;
        let address = db_client.spanner.config.endpoint.as_ref().unwrap();
        let dsn = format!(
            "{}/projects/p/instances/i/databases/d;useplaintext=true;autocommit=false",
            address
        );

        let mut conn = Connection::connect(&dsn).await.expect("connection failed");

        let query_res = conn.execute("SELECT * FROM NonExistingTable").await;
        assert!(query_res.is_err());
        assert_eq!(
            query_res.unwrap_err().status().unwrap().code,
            google_cloud_gax::error::rpc::Code::NotFound
        );

        conn.execute("UPDATE Users SET active = true WHERE id = 1")
            .await
            .expect("update failed");

        conn.execute("COMMIT").await.expect("commit failed");
    }

    #[derive(Debug)]
    struct MockDatabaseAdmin {
        ddl_statements: std::sync::Mutex<Vec<String>>,
    }

    impl MockDatabaseAdmin {
        fn new() -> Self {
            Self {
                ddl_statements: std::sync::Mutex::new(Vec::new()),
            }
        }
    }

    impl google_cloud_spanner_admin_database_v1::stub::DatabaseAdmin for MockDatabaseAdmin {
        fn update_database_ddl(
            &self,
            req: google_cloud_spanner_admin_database_v1::model::UpdateDatabaseDdlRequest,
            _options: google_cloud_gax::options::RequestOptions,
        ) -> impl std::future::Future<
            Output = google_cloud_spanner_admin_database_v1::Result<
                google_cloud_gax::response::Response<google_cloud_longrunning::model::Operation>,
            >,
        > + Send {
            let mut stmts = self.ddl_statements.lock().unwrap();
            stmts.extend(req.statements);

            let response = wkt::Any::from_msg(&wkt::Empty {}).expect("failed to serialize empty");
            let operation = google_cloud_longrunning::model::Operation::new()
                .set_name("operations/update-ddl-123")
                .set_done(true)
                .set_result(Some(
                    google_cloud_longrunning::model::operation::Result::Response(Box::new(
                        response,
                    )),
                ));

            async move { Ok(google_cloud_gax::response::Response::from(operation)) }
        }
    }

    #[tokio::test]
    async fn test_connection_execute_ddl() {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        mock.expect_execute_streaming_sql().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(
                setup_dialect_query_result("GOOGLE_STANDARD_SQL"),
            )])))
        });

        let (db_client, _server) = setup_db_client(mock).await;
        let address = db_client.spanner.config.endpoint.as_ref().unwrap();
        let dsn = format!(
            "{}/projects/p/instances/i/databases/d;useplaintext=true",
            address
        );

        let parsed = parse_dsn(&dsn).unwrap();
        let key = ClientPoolKey::from_dsn(&parsed);

        let mock_admin = std::sync::Arc::new(MockDatabaseAdmin::new());
        let admin_client = DatabaseAdmin::from_stub::<MockDatabaseAdmin>(mock_admin.clone());

        {
            let mut pool = CLIENT_POOL.lock().unwrap();
            let cell = tokio::sync::OnceCell::new();
            let _ = cell.set(admin_client);
            pool.admin_clients.insert(key, std::sync::Arc::new(cell));
        }

        let mut conn = Connection::connect(&dsn).await.expect("connection failed");
        let ddl_sql = "CREATE TABLE Users (id INT64) PRIMARY KEY(id)";
        let res = conn.execute(ddl_sql).await.expect("execute failed");

        assert!(matches!(res, ExecutionResult::Success));

        let stmts = mock_admin.ddl_statements.lock().unwrap();
        assert_eq!(stmts.len(), 1);
        assert_eq!(stmts[0], ddl_sql);
    }

    #[tokio::test]
    async fn test_connection_batch_ddl_run() {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        mock.expect_execute_streaming_sql().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(
                setup_dialect_query_result("GOOGLE_STANDARD_SQL"),
            )])))
        });

        let (db_client, _server) = setup_db_client(mock).await;
        let address = db_client.spanner.config.endpoint.as_ref().unwrap();
        let dsn = format!(
            "{}/projects/p/instances/i/databases/d;useplaintext=true",
            address
        );

        let parsed = parse_dsn(&dsn).unwrap();
        let key = ClientPoolKey::from_dsn(&parsed);

        let mock_admin = std::sync::Arc::new(MockDatabaseAdmin::new());
        let admin_client = DatabaseAdmin::from_stub::<MockDatabaseAdmin>(mock_admin.clone());

        {
            let mut pool = CLIENT_POOL.lock().unwrap();
            let cell = tokio::sync::OnceCell::new();
            let _ = cell.set(admin_client);
            pool.admin_clients.insert(key, std::sync::Arc::new(cell));
        }

        let mut conn = Connection::connect(&dsn).await.expect("connection failed");

        let res = conn
            .execute("START BATCH DDL")
            .await
            .expect("start batch failed");
        assert!(matches!(res, ExecutionResult::Success));

        let ddl_1 = "CREATE TABLE Users (id INT64) PRIMARY KEY(id)";
        let res = conn.execute(ddl_1).await.expect("add statement 1 failed");
        assert!(matches!(res, ExecutionResult::Success));

        let ddl_2 = "CREATE TABLE Posts (id INT64) PRIMARY KEY(id)";
        let res = conn.execute(ddl_2).await.expect("add statement 2 failed");
        assert!(matches!(res, ExecutionResult::Success));

        {
            let stmts = mock_admin.ddl_statements.lock().unwrap();
            assert!(stmts.is_empty());
        }

        let res = conn.execute("RUN BATCH").await.expect("run batch failed");
        assert!(matches!(res, ExecutionResult::Success));

        let stmts = mock_admin.ddl_statements.lock().unwrap();
        assert_eq!(stmts.len(), 2);
        assert_eq!(stmts[0], ddl_1);
        assert_eq!(stmts[1], ddl_2);
    }

    #[tokio::test]
    async fn test_connection_batch_ddl_abort() {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        mock.expect_execute_streaming_sql().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(
                setup_dialect_query_result("GOOGLE_STANDARD_SQL"),
            )])))
        });

        let (db_client, _server) = setup_db_client(mock).await;
        let address = db_client.spanner.config.endpoint.as_ref().unwrap();
        let dsn = format!(
            "{}/projects/p/instances/i/databases/d;useplaintext=true",
            address
        );

        let mut conn = Connection::connect(&dsn).await.expect("connection failed");

        conn.execute("START BATCH DDL")
            .await
            .expect("start batch failed");
        conn.execute("CREATE TABLE Users (id INT64) PRIMARY KEY(id)")
            .await
            .expect("add failed");

        let res = conn
            .execute("ABORT BATCH")
            .await
            .expect("abort batch failed");
        assert!(matches!(res, ExecutionResult::Success));

        let res = conn.execute("RUN BATCH").await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_connection_batch_dml_run() {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        mock.expect_execute_streaming_sql().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(
                setup_dialect_query_result("GOOGLE_STANDARD_SQL"),
            )])))
        });

        mock.expect_begin_transaction().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Transaction {
                id: vec![1, 2, 3],
                ..Default::default()
            }))
        });

        mock.expect_execute_batch_dml().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.statements.len(), 2);
            assert_eq!(req.statements[0].sql, "INSERT INTO Users (id) VALUES (1)");
            assert_eq!(req.statements[1].sql, "INSERT INTO Users (id) VALUES (2)");

            use spanner_grpc_mock::google::spanner::v1::{
                ResultSet, ResultSetMetadata, ResultSetStats, Transaction as ProtoTransaction,
                result_set_stats::RowCount,
            };

            Ok(gaxi::grpc::tonic::Response::new(ExecuteBatchDmlResponse {
                result_sets: vec![
                    ResultSet {
                        metadata: Some(ResultSetMetadata {
                            transaction: Some(ProtoTransaction {
                                id: vec![1, 2, 3],
                                ..Default::default()
                            }),
                            ..Default::default()
                        }),
                        stats: Some(ResultSetStats {
                            row_count: Some(RowCount::RowCountExact(1)),
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                    ResultSet {
                        stats: Some(ResultSetStats {
                            row_count: Some(RowCount::RowCountExact(1)),
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                ],
                status: Some(spanner_grpc_mock::google::rpc::Status::default()),
                precommit_token: None,
            }))
        });

        mock.expect_commit().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.transaction,
                Some(commit_request::Transaction::TransactionId(vec![1, 2, 3]))
            );
            Ok(gaxi::grpc::tonic::Response::new(CommitResponse {
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;
        let address = db_client.spanner.config.endpoint.as_ref().unwrap();
        let dsn = format!(
            "{}/projects/p/instances/i/databases/d;useplaintext=true",
            address
        );

        let mut conn = Connection::connect(&dsn).await.expect("connection failed");

        let res = conn
            .execute("START BATCH DML")
            .await
            .expect("start batch failed");
        assert!(matches!(res, ExecutionResult::Success));

        let res = conn
            .execute("INSERT INTO Users (id) VALUES (1)")
            .await
            .expect("add failed");
        assert!(matches!(res, ExecutionResult::Success));

        let res = conn
            .execute("INSERT INTO Users (id) VALUES (2)")
            .await
            .expect("add failed");
        assert!(matches!(res, ExecutionResult::Success));

        let res = conn.execute("RUN BATCH").await.expect("run batch failed");
        if let ExecutionResult::BatchUpdateResult(counts) = res {
            assert_eq!(counts.iter().sum::<i64>(), 2);
        } else {
            panic!("Expected batch update result");
        }
    }

    #[tokio::test]
    async fn test_connection_explicit_transaction_commit() {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        mock.expect_execute_streaming_sql().once().returning(|req| {
            let req = req.into_inner();
            assert!(req.sql.starts_with("SELECT"));
            Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(
                setup_dialect_query_result("GOOGLE_STANDARD_SQL"),
            )])))
        });

        mock.expect_execute_sql().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.sql, "UPDATE Users SET name = 'John' WHERE id = 1");
            let transaction = req.transaction.expect("transaction required");
            let selector = transaction.selector.expect("selector required");
            assert!(matches!(
                selector,
                transaction_selector::Selector::Id(id) if id == vec![4, 5, 6]
            ));
            Ok(gaxi::grpc::tonic::Response::new(setup_dml_response(1)))
        });

        mock.expect_begin_transaction().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Transaction {
                id: vec![4, 5, 6],
                ..Default::default()
            }))
        });

        mock.expect_commit().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.transaction,
                Some(commit_request::Transaction::TransactionId(vec![4, 5, 6]))
            );
            Ok(gaxi::grpc::tonic::Response::new(CommitResponse {
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;
        let address = db_client.spanner.config.endpoint.as_ref().unwrap();
        let dsn = format!(
            "{}/projects/p/instances/i/databases/d;useplaintext=true;autocommit=false",
            address
        );

        let mut conn = Connection::connect(&dsn).await.expect("connection failed");

        let res = conn
            .execute("UPDATE Users SET name = 'John' WHERE id = 1")
            .await
            .expect("update failed");
        if let ExecutionResult::UpdateResult(count) = res {
            assert_eq!(count, 1);
        } else {
            panic!("Expected update result");
        }

        let res = conn.execute("COMMIT").await.expect("commit failed");
        assert!(matches!(res, ExecutionResult::Success));
    }

    #[tokio::test]
    async fn test_connection_explicit_transaction_rollback() {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        mock.expect_execute_streaming_sql().once().returning(|req| {
            let req = req.into_inner();
            assert!(req.sql.starts_with("SELECT"));
            Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(
                setup_dialect_query_result("GOOGLE_STANDARD_SQL"),
            )])))
        });

        mock.expect_execute_sql().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.sql, "UPDATE Users SET name = 'John' WHERE id = 1");
            let transaction = req.transaction.expect("transaction required");
            let selector = transaction.selector.expect("selector required");
            assert!(matches!(
                selector,
                transaction_selector::Selector::Id(id) if id == vec![7, 8, 9]
            ));
            Ok(gaxi::grpc::tonic::Response::new(setup_dml_response(1)))
        });

        mock.expect_begin_transaction().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Transaction {
                id: vec![7, 8, 9],
                ..Default::default()
            }))
        });

        mock.expect_rollback().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.transaction_id, vec![7, 8, 9]);
            Ok(gaxi::grpc::tonic::Response::new(()))
        });

        let (db_client, _server) = setup_db_client(mock).await;
        let address = db_client.spanner.config.endpoint.as_ref().unwrap();
        let dsn = format!(
            "{}/projects/p/instances/i/databases/d;useplaintext=true;autocommit=false",
            address
        );

        let mut conn = Connection::connect(&dsn).await.expect("connection failed");

        conn.execute("UPDATE Users SET name = 'John' WHERE id = 1")
            .await
            .expect("update failed");

        let res = conn.execute("ROLLBACK").await.expect("rollback failed");
        assert!(matches!(res, ExecutionResult::Success));
    }

    #[tokio::test]
    async fn test_connection_set_properties_flow() {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        mock.expect_execute_streaming_sql().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(
                setup_dialect_query_result("GOOGLE_STANDARD_SQL"),
            )])))
        });

        mock.expect_execute_streaming_sql().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(
                PartialResultSet {
                    metadata: Some(ResultSetMetadata {
                        row_type: Some(StructType {
                            fields: vec![Default::default()],
                        }),
                        ..Default::default()
                    }),
                    values: vec![prost_types::Value {
                        kind: Some(prost_types::value::Kind::StringValue("res".to_string())),
                    }],
                    last: true,
                    ..Default::default()
                },
            )])))
        });

        mock.expect_begin_transaction().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Transaction {
                id: vec![100],
                ..Default::default()
            }))
        });

        mock.expect_commit().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.transaction,
                Some(commit_request::Transaction::TransactionId(vec![100]))
            );
            Ok(gaxi::grpc::tonic::Response::new(CommitResponse {
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;
        let address = db_client.spanner.config.endpoint.as_ref().unwrap();
        let dsn = format!(
            "{}/projects/p/instances/i/databases/d;useplaintext=true;autocommit=true",
            address
        );

        let mut conn = Connection::connect(&dsn).await.expect("connection failed");
        assert!(AUTOCOMMIT.get_value(&conn.state));

        let res = conn.execute("SELECT 1").await.expect("query failed");
        assert!(matches!(res, ExecutionResult::QueryResult(_)));

        let res = conn
            .execute("SET autocommit = false")
            .await
            .expect("set failed");
        assert!(matches!(res, ExecutionResult::Success));
        assert!(!AUTOCOMMIT.get_value(&conn.state));

        conn.start_transaction(None)
            .await
            .expect("start transaction failed");

        let res = conn.execute("COMMIT").await.expect("commit failed");
        assert!(matches!(res, ExecutionResult::Success));
    }

    async fn get_property_value(conn: &mut Connection, key: &str) -> String {
        let sql = match conn.state.dialect() {
            Dialect::GoogleSql => format!("SHOW VARIABLE {}", key),
            Dialect::PostgreSql => format!("SHOW {}", key),
        };
        let res = conn.execute(sql).await.unwrap();
        if let ExecutionResult::QueryResult(mut rs) = res {
            let row = rs.next().await.unwrap().unwrap();
            row.get::<String, usize>(0)
        } else {
            panic!("Expected query result");
        }
    }

    #[tokio::test]
    async fn test_connection_set_local_properties() {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        mock.expect_execute_streaming_sql().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(
                setup_dialect_query_result("GOOGLE_STANDARD_SQL"),
            )])))
        });

        mock.expect_begin_transaction().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Transaction {
                id: vec![200],
                ..Default::default()
            }))
        });

        mock.expect_commit().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.transaction,
                Some(commit_request::Transaction::TransactionId(vec![200]))
            );
            Ok(gaxi::grpc::tonic::Response::new(CommitResponse {
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;
        let address = db_client.spanner.config.endpoint.as_ref().unwrap();
        let dsn = format!(
            "{}/projects/p/instances/i/databases/d;useplaintext=true;autocommit=false",
            address
        );

        let mut conn = Connection::connect(&dsn).await.expect("connection failed");

        assert_eq!(
            get_property_value(&mut conn, "transaction_tag").await,
            "null"
        );

        conn.execute("BEGIN").await.unwrap();

        let res = conn
            .execute("SET LOCAL transaction_tag = 'local-tag-val'")
            .await
            .expect("set local failed");
        assert!(matches!(res, ExecutionResult::Success));

        assert_eq!(
            get_property_value(&mut conn, "transaction_tag").await,
            "local-tag-val"
        );

        conn.execute("COMMIT").await.unwrap();

        assert_eq!(
            get_property_value(&mut conn, "transaction_tag").await,
            "null"
        );
    }

    #[tokio::test]
    async fn test_connection_set_local_properties_rollback() {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        mock.expect_execute_streaming_sql().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(
                setup_dialect_query_result("GOOGLE_STANDARD_SQL"),
            )])))
        });

        mock.expect_begin_transaction().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Transaction {
                id: vec![200],
                ..Default::default()
            }))
        });

        mock.expect_rollback()
            .once()
            .returning(|_| Ok(gaxi::grpc::tonic::Response::new(())));

        let (db_client, _server) = setup_db_client(mock).await;
        let address = db_client.spanner.config.endpoint.as_ref().unwrap();
        let dsn = format!(
            "{}/projects/p/instances/i/databases/d;useplaintext=true;autocommit=false",
            address
        );

        let mut conn = Connection::connect(&dsn).await.expect("connection failed");

        conn.execute("BEGIN").await.unwrap();

        conn.execute("SET LOCAL transaction_tag = 'local-tag-val'")
            .await
            .unwrap();
        assert_eq!(
            get_property_value(&mut conn, "transaction_tag").await,
            "local-tag-val"
        );

        conn.execute("ROLLBACK").await.unwrap();

        assert_eq!(
            get_property_value(&mut conn, "transaction_tag").await,
            "null"
        );
    }

    #[tokio::test]
    async fn test_connection_set_local_properties_outside_tx() {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        mock.expect_execute_streaming_sql().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(
                setup_dialect_query_result("GOOGLE_STANDARD_SQL"),
            )])))
        });

        let (db_client, _server) = setup_db_client(mock).await;
        let address = db_client.spanner.config.endpoint.as_ref().unwrap();
        let dsn = format!(
            "{}/projects/p/instances/i/databases/d;useplaintext=true;autocommit=true",
            address
        );

        let mut conn = Connection::connect(&dsn).await.expect("connection failed");

        let res = conn
            .execute("SET LOCAL transaction_tag = 'local-tag-val'")
            .await
            .expect("set local failed");
        assert!(matches!(res, ExecutionResult::Success));

        assert_eq!(
            get_property_value(&mut conn, "transaction_tag").await,
            "null"
        );
    }

    #[tokio::test]
    async fn test_retry_batch_update_aborted() {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        mock.expect_execute_streaming_sql().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(
                setup_dialect_query_result("GOOGLE_STANDARD_SQL"),
            )])))
        });

        mock.expect_begin_transaction().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Transaction {
                id: vec![1],
                ..Default::default()
            }))
        });

        mock.expect_execute_batch_dml()
            .once()
            .returning(|_| Err(gaxi::grpc::tonic::Status::aborted("Transaction aborted")));

        mock.expect_begin_transaction().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Transaction {
                id: vec![2],
                ..Default::default()
            }))
        });

        mock.expect_execute_batch_dml().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.statements.len(), 2);
            assert_eq!(
                req.transaction.unwrap().selector,
                Some(transaction_selector::Selector::Id(vec![2]))
            );

            use spanner_grpc_mock::google::spanner::v1::{
                ResultSet, ResultSetMetadata, ResultSetStats, Transaction as ProtoTransaction,
                result_set_stats::RowCount,
            };

            Ok(gaxi::grpc::tonic::Response::new(ExecuteBatchDmlResponse {
                result_sets: vec![
                    ResultSet {
                        metadata: Some(ResultSetMetadata {
                            transaction: Some(ProtoTransaction {
                                id: vec![2],
                                ..Default::default()
                            }),
                            ..Default::default()
                        }),
                        stats: Some(ResultSetStats {
                            row_count: Some(RowCount::RowCountExact(1)),
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                    ResultSet {
                        stats: Some(ResultSetStats {
                            row_count: Some(RowCount::RowCountExact(1)),
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                ],
                status: Some(spanner_grpc_mock::google::rpc::Status::default()),
                precommit_token: None,
            }))
        });

        mock.expect_commit().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.transaction,
                Some(commit_request::Transaction::TransactionId(vec![2]))
            );
            Ok(gaxi::grpc::tonic::Response::new(CommitResponse {
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;
        let address = db_client.spanner.config.endpoint.as_ref().unwrap();
        let dsn = format!(
            "{}/projects/p/instances/i/databases/d;useplaintext=true;autocommit=false",
            address
        );

        let mut conn = Connection::connect(&dsn).await.expect("connection failed");

        let res = conn
            .execute("START BATCH DML")
            .await
            .expect("start batch failed");
        assert!(matches!(res, ExecutionResult::Success));

        conn.execute("INSERT INTO Users (id) VALUES (1)")
            .await
            .expect("add failed");
        conn.execute("INSERT INTO Users (id) VALUES (2)")
            .await
            .expect("add failed");

        let res = conn.execute("RUN BATCH").await.expect("run batch failed");
        if let ExecutionResult::BatchUpdateResult(counts) = res {
            assert_eq!(counts.iter().sum::<i64>(), 2);
        } else {
            panic!("Expected batch update result");
        }

        conn.execute("COMMIT").await.expect("commit failed");
    }

    #[tokio::test]
    async fn test_retry_aborted_during_retry_of_failed_batch_update() {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        mock.expect_execute_streaming_sql().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(
                setup_dialect_query_result("GOOGLE_STANDARD_SQL"),
            )])))
        });

        mock.expect_begin_transaction().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Transaction {
                id: vec![1],
                ..Default::default()
            }))
        });

        mock.expect_execute_batch_dml().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(ExecuteBatchDmlResponse {
                result_sets: vec![],
                status: Some(spanner_grpc_mock::google::rpc::Status {
                    code: google_cloud_gax::error::rpc::Code::InvalidArgument as i32,
                    message: "Invalid table".to_string(),
                    details: vec![],
                }),
                precommit_token: None,
            }))
        });

        mock.expect_commit()
            .once()
            .returning(|_| Err(gaxi::grpc::tonic::Status::aborted("Transaction aborted")));

        mock.expect_begin_transaction().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Transaction {
                id: vec![2],
                ..Default::default()
            }))
        });

        mock.expect_execute_batch_dml().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(ExecuteBatchDmlResponse {
                result_sets: vec![],
                status: Some(spanner_grpc_mock::google::rpc::Status {
                    code: google_cloud_gax::error::rpc::Code::InvalidArgument as i32,
                    message: "Invalid table".to_string(),
                    details: vec![],
                }),
                precommit_token: None,
            }))
        });

        mock.expect_commit().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.transaction,
                Some(commit_request::Transaction::TransactionId(vec![2]))
            );
            Ok(gaxi::grpc::tonic::Response::new(CommitResponse {
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;
        let address = db_client.spanner.config.endpoint.as_ref().unwrap();
        let dsn = format!(
            "{}/projects/p/instances/i/databases/d;useplaintext=true;autocommit=false",
            address
        );

        let mut conn = Connection::connect(&dsn).await.expect("connection failed");

        let res = conn
            .execute("START BATCH DML")
            .await
            .expect("start batch failed");
        assert!(matches!(res, ExecutionResult::Success));

        conn.execute("INSERT INTO NonExistingTable (id) VALUES (1)")
            .await
            .expect("add failed");

        let res = conn.execute("RUN BATCH").await;
        assert!(res.is_err());
        assert_eq!(
            res.unwrap_err().status().unwrap().code,
            google_cloud_gax::error::rpc::Code::InvalidArgument
        );

        conn.execute("COMMIT").await.expect("commit failed");
    }

    #[tokio::test]
    async fn test_retry_query_consumed_halfway_commit_aborted() {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        mock.expect_execute_streaming_sql().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(
                setup_dialect_query_result("GOOGLE_STANDARD_SQL"),
            )])))
        });

        mock.expect_begin_transaction().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Transaction {
                id: vec![1],
                ..Default::default()
            }))
        });

        mock.expect_execute_streaming_sql().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.sql, "SELECT name FROM Users");
            assert_eq!(
                req.transaction.unwrap().selector,
                Some(transaction_selector::Selector::Id(vec![1]))
            );

            Ok(gaxi::grpc::tonic::Response::from(adapt([
                Ok(setup_query_result_metadata()),
                Ok(setup_query_result_row(vec![Kind::StringValue(
                    "Alice".to_string(),
                )])),
                Ok(setup_query_result_row(vec![Kind::StringValue(
                    "Bob".to_string(),
                )])),
            ])))
        });

        mock.expect_commit()
            .once()
            .returning(|_| Err(gaxi::grpc::tonic::Status::aborted("Transaction aborted")));

        mock.expect_begin_transaction().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Transaction {
                id: vec![2],
                ..Default::default()
            }))
        });

        mock.expect_execute_streaming_sql().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.sql, "SELECT name FROM Users");
            assert_eq!(
                req.transaction.unwrap().selector,
                Some(transaction_selector::Selector::Id(vec![2]))
            );

            Ok(gaxi::grpc::tonic::Response::from(adapt([
                Ok(setup_query_result_metadata()),
                Ok(setup_query_result_row(vec![Kind::StringValue(
                    "Alice".to_string(),
                )])),
                Ok(setup_query_result_row(vec![Kind::StringValue(
                    "Bob".to_string(),
                )])),
            ])))
        });

        mock.expect_commit().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.transaction,
                Some(commit_request::Transaction::TransactionId(vec![2]))
            );
            Ok(gaxi::grpc::tonic::Response::new(CommitResponse {
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;
        let address = db_client.spanner.config.endpoint.as_ref().unwrap();
        let dsn = format!(
            "{}/projects/p/instances/i/databases/d;useplaintext=true;autocommit=false",
            address
        );

        let mut conn = Connection::connect(&dsn).await.expect("connection failed");

        let res = conn
            .execute("SELECT name FROM Users")
            .await
            .expect("query failed");
        if let ExecutionResult::QueryResult(mut rs) = res {
            let row = rs.next().await.unwrap().unwrap();
            let val: String = row.get(0);
            assert_eq!(val, "Alice");

            let row = rs.next().await.unwrap().unwrap();
            let val: String = row.get(0);
            assert_eq!(val, "Bob");

            assert!(rs.next().await.is_none());
        } else {
            panic!("Expected query result");
        }

        conn.execute("COMMIT").await.expect("commit failed");
    }

    #[tokio::test]
    async fn test_retry_query_consumed_halfway_with_extra_rows() {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        mock.expect_execute_streaming_sql().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(
                setup_dialect_query_result("GOOGLE_STANDARD_SQL"),
            )])))
        });

        mock.expect_begin_transaction().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Transaction {
                id: vec![1],
                ..Default::default()
            }))
        });

        mock.expect_execute_streaming_sql().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.sql, "SELECT name FROM Users");
            assert_eq!(
                req.transaction.unwrap().selector,
                Some(transaction_selector::Selector::Id(vec![1]))
            );

            Ok(gaxi::grpc::tonic::Response::from(adapt([
                Ok(setup_query_result_metadata()),
                Ok(setup_query_result_row(vec![Kind::StringValue(
                    "Alice".to_string(),
                )])),
                Ok(setup_query_result_row(vec![Kind::StringValue(
                    "Bob".to_string(),
                )])),
            ])))
        });

        mock.expect_commit()
            .once()
            .returning(|_| Err(gaxi::grpc::tonic::Status::aborted("Transaction aborted")));

        mock.expect_begin_transaction().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Transaction {
                id: vec![2],
                ..Default::default()
            }))
        });

        mock.expect_execute_streaming_sql().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.sql, "SELECT name FROM Users");
            assert_eq!(
                req.transaction.unwrap().selector,
                Some(transaction_selector::Selector::Id(vec![2]))
            );

            Ok(gaxi::grpc::tonic::Response::from(adapt([
                Ok(setup_query_result_metadata()),
                Ok(setup_query_result_row(vec![Kind::StringValue(
                    "Alice".to_string(),
                )])),
                Ok(setup_query_result_row(vec![Kind::StringValue(
                    "Bob".to_string(),
                )])),
                Ok(setup_query_result_row(vec![Kind::StringValue(
                    "Charlie".to_string(),
                )])),
            ])))
        });

        mock.expect_commit().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.transaction,
                Some(commit_request::Transaction::TransactionId(vec![2]))
            );
            Ok(gaxi::grpc::tonic::Response::new(CommitResponse {
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;
        let address = db_client.spanner.config.endpoint.as_ref().unwrap();
        let dsn = format!(
            "{}/projects/p/instances/i/databases/d;useplaintext=true;autocommit=false",
            address
        );

        let mut conn = Connection::connect(&dsn).await.expect("connection failed");

        let res = conn
            .execute("SELECT name FROM Users")
            .await
            .expect("query failed");
        if let ExecutionResult::QueryResult(mut rs) = res {
            let row = rs.next().await.unwrap().unwrap();
            let val: String = row.get(0);
            assert_eq!(val, "Alice");

            let row = rs.next().await.unwrap().unwrap();
            let val: String = row.get(0);
            assert_eq!(val, "Bob");

            assert!(rs.next().await.is_none());
        } else {
            panic!("Expected query result");
        }

        conn.execute("COMMIT").await.expect("commit failed");
    }

    #[tokio::test]
    async fn test_retry_aborted_during_retry_of_failed_query_as_first_statement() {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        mock.expect_execute_streaming_sql().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(
                setup_dialect_query_result("GOOGLE_STANDARD_SQL"),
            )])))
        });

        mock.expect_begin_transaction().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Transaction {
                id: vec![1],
                ..Default::default()
            }))
        });

        mock.expect_execute_streaming_sql()
            .once()
            .returning(|_| Err(gaxi::grpc::tonic::Status::not_found("Table not found")));

        mock.expect_commit()
            .once()
            .returning(|_| Err(gaxi::grpc::tonic::Status::aborted("Transaction aborted")));

        mock.expect_begin_transaction().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Transaction {
                id: vec![2],
                ..Default::default()
            }))
        });

        mock.expect_execute_streaming_sql().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.sql, "SELECT * FROM NonExistingTable");
            Err(gaxi::grpc::tonic::Status::not_found("Table not found"))
        });

        mock.expect_commit().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(CommitResponse {
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;
        let address = db_client.spanner.config.endpoint.as_ref().unwrap();
        let dsn = format!(
            "{}/projects/p/instances/i/databases/d;useplaintext=true;autocommit=false",
            address
        );

        let mut conn = Connection::connect(&dsn).await.expect("connection failed");

        let query_res = conn.execute("SELECT * FROM NonExistingTable").await;
        assert!(query_res.is_err());
        assert_eq!(
            query_res.unwrap_err().status().unwrap().code,
            google_cloud_gax::error::rpc::Code::NotFound
        );

        conn.execute("COMMIT").await.expect("commit failed");
    }

    #[tokio::test]
    async fn test_retry_exceeds_max_attempts_on_commit() {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        mock.expect_execute_streaming_sql().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(
                setup_dialect_query_result("GOOGLE_STANDARD_SQL"),
            )])))
        });

        // 6 begin transaction calls (1 original + 5 retries)
        mock.expect_begin_transaction().times(6).returning(|req| {
            let req = req.into_inner();
            let id = if req.options.is_none() { 1 } else { 2 };
            Ok(gaxi::grpc::tonic::Response::new(Transaction {
                id: vec![id],
                ..Default::default()
            }))
        });

        // 6 update statement attempts (1 original + 5 retries)
        mock.expect_execute_sql().times(6).returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.sql, "UPDATE Users SET active = true WHERE id = 1");
            Ok(gaxi::grpc::tonic::Response::new(setup_dml_response(1)))
        });

        // Commit aborts 6 times (1 original + 5 retries)
        mock.expect_commit()
            .times(6)
            .returning(|_| Err(gaxi::grpc::tonic::Status::aborted("Transaction aborted")));

        let (db_client, _server) = setup_db_client(mock).await;
        let address = db_client.spanner.config.endpoint.as_ref().unwrap();
        let dsn = format!(
            "{}/projects/p/instances/i/databases/d;useplaintext=true;autocommit=false",
            address
        );

        let mut conn = Connection::connect(&dsn).await.expect("connection failed");

        conn.execute("UPDATE Users SET active = true WHERE id = 1")
            .await
            .expect("update failed");

        let res = conn.execute("COMMIT").await;
        assert!(res.is_err());
        assert_eq!(
            res.unwrap_err().status().unwrap().code,
            google_cloud_gax::error::rpc::Code::Aborted
        );
    }

    #[tokio::test]
    async fn test_retry_exceeds_max_attempts_on_statement() {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        mock.expect_execute_streaming_sql().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(
                setup_dialect_query_result("GOOGLE_STANDARD_SQL"),
            )])))
        });

        // 6 begin transaction calls (1 original + 5 retries)
        mock.expect_begin_transaction().times(6).returning(|req| {
            let req = req.into_inner();
            let id = if req.options.is_none() { 1 } else { 2 };
            Ok(gaxi::grpc::tonic::Response::new(Transaction {
                id: vec![id],
                ..Default::default()
            }))
        });

        // 6 update statement attempts (1 original + 5 retries)
        let attempt = std::sync::Arc::new(std::sync::Mutex::new(0));
        mock.expect_execute_sql().times(6).returning(move |req| {
            let mut guard = attempt.lock().unwrap();
            *guard += 1;
            let current = *guard;
            drop(guard);

            let req = req.into_inner();
            assert_eq!(req.sql, "UPDATE Users SET active = true WHERE id = 1");
            if current == 1 {
                Ok(gaxi::grpc::tonic::Response::new(setup_dml_response(1)))
            } else {
                Err(gaxi::grpc::tonic::Status::aborted("Transaction aborted"))
            }
        });

        // The first commit aborts
        mock.expect_commit()
            .once()
            .returning(|_| Err(gaxi::grpc::tonic::Status::aborted("Transaction aborted")));

        let (db_client, _server) = setup_db_client(mock).await;
        let address = db_client.spanner.config.endpoint.as_ref().unwrap();
        let dsn = format!(
            "{}/projects/p/instances/i/databases/d;useplaintext=true;autocommit=false",
            address
        );

        let mut conn = Connection::connect(&dsn).await.expect("connection failed");

        conn.execute("UPDATE Users SET active = true WHERE id = 1")
            .await
            .expect("update failed");

        let res = conn.execute("COMMIT").await;
        assert!(res.is_err());
        assert_eq!(
            res.unwrap_err().to_string(),
            "cannot deserialize the response Transaction aborted: too many retry attempts"
        );
    }

    #[tokio::test]
    async fn test_savepoint_rollback_replay() {
        let mut mock = MockSpanner::default();

        mock.expect_create_session().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Session {
                name: "projects/p/instances/i/databases/d/sessions/s".to_string(),
                ..Default::default()
            }))
        });

        mock.expect_execute_streaming_sql().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(
                setup_dialect_query_result("GOOGLE_STANDARD_SQL"),
            )])))
        });

        // Transaction 1 begin
        mock.expect_begin_transaction().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Transaction {
                id: b"tx-1".to_vec(),
                ..Default::default()
            }))
        });

        // Update 1 (first transaction)
        mock.expect_execute_sql().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.sql, "UPDATE Users SET active = true WHERE id = 1");
            assert_eq!(
                req.transaction.unwrap().selector,
                Some(transaction_selector::Selector::Id(b"tx-1".to_vec()))
            );
            Ok(gaxi::grpc::tonic::Response::new(setup_dml_response(1)))
        });

        // Update 2 (first transaction)
        mock.expect_execute_sql().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.sql, "UPDATE Users SET active = true WHERE id = 2");
            assert_eq!(
                req.transaction.unwrap().selector,
                Some(transaction_selector::Selector::Id(b"tx-1".to_vec()))
            );
            Ok(gaxi::grpc::tonic::Response::new(setup_dml_response(1)))
        });

        // Rollback of transaction 1 (triggered by rollback to savepoint)
        mock.expect_rollback().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.transaction_id, b"tx-1".to_vec());
            Ok(gaxi::grpc::tonic::Response::new(()))
        });

        // Transaction 2 begin (triggered by retry after rollback to savepoint)
        mock.expect_begin_transaction().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Transaction {
                id: b"tx-2".to_vec(),
                ..Default::default()
            }))
        });

        // Replay Update 1 on transaction 2
        mock.expect_execute_sql().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.sql, "UPDATE Users SET active = true WHERE id = 1");
            assert_eq!(
                req.transaction.unwrap().selector,
                Some(transaction_selector::Selector::Id(b"tx-2".to_vec()))
            );
            Ok(gaxi::grpc::tonic::Response::new(setup_dml_response(1)))
        });

        // Update 3 on transaction 2 (the current statement)
        mock.expect_execute_sql().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.sql, "UPDATE Users SET active = true WHERE id = 3");
            assert_eq!(
                req.transaction.unwrap().selector,
                Some(transaction_selector::Selector::Id(b"tx-2".to_vec()))
            );
            Ok(gaxi::grpc::tonic::Response::new(setup_dml_response(1)))
        });

        // Commit on transaction 2
        mock.expect_commit().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.transaction,
                Some(commit_request::Transaction::TransactionId(b"tx-2".to_vec()))
            );
            Ok(gaxi::grpc::tonic::Response::new(CommitResponse::default()))
        });

        let (db_client, _server) = setup_db_client(mock).await;
        let address = db_client.spanner.config.endpoint.as_ref().unwrap();
        let dsn = format!(
            "{}/projects/p/instances/i/databases/d;useplaintext=true;autocommit=false",
            address
        );

        tokio::time::timeout(std::time::Duration::from_secs(5), async move {
            let mut conn = Connection::connect(&dsn).await.expect("connection failed");

            // 1. Run update 1
            conn.execute("UPDATE Users SET active = true WHERE id = 1")
                .await
                .expect("update 1 failed");

            // 2. Set savepoint s1
            conn.execute("SAVEPOINT s1")
                .await
                .expect("savepoint s1 failed");

            // 3. Run update 2
            conn.execute("UPDATE Users SET active = true WHERE id = 2")
                .await
                .expect("update 2 failed");

            // 4. Rollback to savepoint s1
            conn.execute("ROLLBACK TO SAVEPOINT s1")
                .await
                .expect("rollback to savepoint failed");

            // 5. Run update 3 (should trigger retry & replay update 1)
            conn.execute("UPDATE Users SET active = true WHERE id = 3")
                .await
                .expect("update 3 failed");

            // 6. Commit
            conn.execute("COMMIT").await.expect("commit failed");
        })
        .await
        .expect("test timed out");
    }

    #[tokio::test]
    async fn test_savepoint_disabled() {
        let mut mock = MockSpanner::default();
        mock.expect_create_session().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Session {
                name: "projects/p/instances/i/databases/d/sessions/s".to_string(),
                ..Default::default()
            }))
        });
        mock.expect_execute_streaming_sql().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(
                setup_dialect_query_result("GOOGLE_STANDARD_SQL"),
            )])))
        });
        let (db_client, _server) = setup_db_client(mock).await;
        let address = db_client.spanner.config.endpoint.as_ref().unwrap();
        let dsn = format!(
            "{}/projects/p/instances/i/databases/d;useplaintext=true;autocommit=false",
            address
        );
        let mut conn = Connection::connect(&dsn).await.expect("connection failed");

        // Disable savepoint support
        conn.execute("SET SAVEPOINT_SUPPORT = 'disabled'")
            .await
            .expect("set failed");

        // Creating savepoint should fail
        let res = conn.execute("SAVEPOINT s1").await;
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("Savepoint creation is not allowed")
        );
    }

    #[tokio::test]
    async fn test_savepoint_autocommit() {
        let mut mock = MockSpanner::default();
        mock.expect_create_session().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Session {
                name: "projects/p/instances/i/databases/d/sessions/s".to_string(),
                ..Default::default()
            }))
        });
        mock.expect_execute_streaming_sql().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(
                setup_dialect_query_result("GOOGLE_STANDARD_SQL"),
            )])))
        });
        let (db_client, _server) = setup_db_client(mock).await;
        let address = db_client.spanner.config.endpoint.as_ref().unwrap();
        let dsn = format!(
            "{}/projects/p/instances/i/databases/d;useplaintext=true;autocommit=true",
            address
        );
        let mut conn = Connection::connect(&dsn).await.expect("connection failed");

        let res = conn.execute("SAVEPOINT s1").await;
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("Savepoints are only supported in manual transactions")
        );
    }

    #[tokio::test]
    async fn test_savepoint_release() {
        let mut mock = MockSpanner::default();
        mock.expect_create_session().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Session {
                name: "projects/p/instances/i/databases/d/sessions/s".to_string(),
                ..Default::default()
            }))
        });
        mock.expect_execute_streaming_sql().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(
                setup_dialect_query_result("GOOGLE_STANDARD_SQL"),
            )])))
        });
        mock.expect_begin_transaction().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Transaction {
                id: b"tx-1".to_vec(),
                ..Default::default()
            }))
        });
        let (db_client, _server) = setup_db_client(mock).await;
        let address = db_client.spanner.config.endpoint.as_ref().unwrap();
        let dsn = format!(
            "{}/projects/p/instances/i/databases/d;useplaintext=true;autocommit=false",
            address
        );
        let mut conn = Connection::connect(&dsn).await.expect("connection failed");

        // Set and release savepoint s1
        conn.execute("SAVEPOINT s1")
            .await
            .expect("savepoint s1 failed");
        conn.execute("RELEASE SAVEPOINT s1")
            .await
            .expect("release s1 failed");

        // Re-releasing s1 should fail
        let res = conn.execute("RELEASE SAVEPOINT s1").await;
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("does not exist"));

        // Releasing s1 should also release nested s2
        conn.execute("SAVEPOINT s1")
            .await
            .expect("savepoint s1 failed");
        conn.execute("SAVEPOINT s2")
            .await
            .expect("savepoint s2 failed");
        conn.execute("RELEASE SAVEPOINT s1")
            .await
            .expect("release s1 failed");

        let res = conn.execute("RELEASE SAVEPOINT s2").await;
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("does not exist"));
    }

    #[tokio::test]
    async fn test_rollback_to_savepoint_multiple_times() {
        let mut mock = MockSpanner::default();
        mock.expect_create_session().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Session {
                name: "projects/p/instances/i/databases/d/sessions/s".to_string(),
                ..Default::default()
            }))
        });
        mock.expect_execute_streaming_sql().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(
                setup_dialect_query_result("GOOGLE_STANDARD_SQL"),
            )])))
        });
        mock.expect_begin_transaction().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Transaction {
                id: b"tx-1".to_vec(),
                ..Default::default()
            }))
        });
        // Expect rollback call three times (since we roll back three times in total)
        mock.expect_rollback()
            .times(3)
            .returning(|_| Ok(gaxi::grpc::tonic::Response::new(())));
        let (db_client, _server) = setup_db_client(mock).await;
        let address = db_client.spanner.config.endpoint.as_ref().unwrap();
        let dsn = format!(
            "{}/projects/p/instances/i/databases/d;useplaintext=true;autocommit=false",
            address
        );
        let mut conn = Connection::connect(&dsn).await.expect("connection failed");

        conn.execute("SAVEPOINT s1")
            .await
            .expect("savepoint s1 failed");
        conn.execute("ROLLBACK TO SAVEPOINT s1")
            .await
            .expect("rollback 1 failed");
        conn.execute("ROLLBACK TO SAVEPOINT s1")
            .await
            .expect("rollback 2 failed");

        // Set nested savepoint s2
        conn.execute("SAVEPOINT s2")
            .await
            .expect("savepoint s2 failed");
        conn.execute("ROLLBACK TO SAVEPOINT s1")
            .await
            .expect("rollback to s1 failed");

        // Since we rolled back to s1, s2 is removed
        let res = conn.execute("ROLLBACK TO SAVEPOINT s2").await;
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("does not exist"));
    }

    #[tokio::test]
    async fn test_rollback_to_savepoint_fail_after_rollback() {
        let mut mock = MockSpanner::default();
        mock.expect_create_session().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Session {
                name: "projects/p/instances/i/databases/d/sessions/s".to_string(),
                ..Default::default()
            }))
        });
        mock.expect_execute_streaming_sql().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(
                setup_dialect_query_result("GOOGLE_STANDARD_SQL"),
            )])))
        });
        mock.expect_begin_transaction().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Transaction {
                id: b"tx-1".to_vec(),
                ..Default::default()
            }))
        });
        mock.expect_execute_sql()
            .once()
            .returning(|_| Ok(gaxi::grpc::tonic::Response::new(setup_dml_response(1))));
        mock.expect_rollback()
            .once()
            .returning(|_| Ok(gaxi::grpc::tonic::Response::new(())));
        let (db_client, _server) = setup_db_client(mock).await;
        let address = db_client.spanner.config.endpoint.as_ref().unwrap();
        let dsn = format!(
            "{}/projects/p/instances/i/databases/d;useplaintext=true;autocommit=false",
            address
        );
        let mut conn = Connection::connect(&dsn).await.expect("connection failed");

        conn.execute("SET SAVEPOINT_SUPPORT = 'fail_after_rollback'")
            .await
            .expect("set failed");

        conn.execute("UPDATE Users SET active = true WHERE id = 1")
            .await
            .expect("update failed");

        conn.execute("SAVEPOINT s1")
            .await
            .expect("savepoint failed");
        conn.execute("ROLLBACK TO SAVEPOINT s1")
            .await
            .expect("rollback to savepoint failed");

        // Doing subsequent operations (or commit) should fail
        let res = conn.execute("COMMIT").await;
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("is not supported with SavepointSupport=FailAfterRollback")
        );
    }

    #[tokio::test]
    async fn test_rollback_to_savepoint_name_validation() {
        let mut mock = MockSpanner::default();
        mock.expect_create_session().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Session {
                name: "projects/p/instances/i/databases/d/sessions/s".to_string(),
                ..Default::default()
            }))
        });
        mock.expect_execute_streaming_sql().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(
                setup_dialect_query_result("GOOGLE_STANDARD_SQL"),
            )])))
        });
        let (db_client, _server) = setup_db_client(mock).await;
        let address = db_client.spanner.config.endpoint.as_ref().unwrap();
        let dsn = format!(
            "{}/projects/p/instances/i/databases/d;useplaintext=true;autocommit=false",
            address
        );
        let mut conn = Connection::connect(&dsn).await.expect("connection failed");

        let res = conn.execute("SAVEPOINT 1s").await;
        assert!(res.is_err());

        let res2 = conn.execute("SAVEPOINT -foo").await;
        assert!(res2.is_err());

        let res3 = conn.execute(format!("SAVEPOINT {}", "a".repeat(129))).await;
        assert!(res3.is_err());
    }

    #[tokio::test]
    async fn test_connection_autocommit_dml_last_statement() {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        // Dialect check query
        mock.expect_execute_streaming_sql().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(
                setup_dialect_query_result("GOOGLE_STANDARD_SQL"),
            )])))
        });

        // DML update
        mock.expect_execute_streaming_sql().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.sql, "UPDATE Users SET Name = 'Alice' WHERE Id = 1");
            assert!(req.last_statement, "last_statement should be automatically set to true in autocommit DML");

            // Check that it began a transaction inline
            let transaction = req
                .transaction
                .as_ref()
                .expect("transaction options required for inline begin");
            let selector = transaction.selector.as_ref().expect("selector required");
            assert!(matches!(selector, transaction_selector::Selector::Begin(_)));

            Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(
                PartialResultSet {
                    metadata: Some(ResultSetMetadata {
                        row_type: Some(StructType { fields: vec![] }),
                        transaction: Some(Transaction {
                            id: vec![1, 2, 3],
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    stats: Some(spanner_grpc_mock::google::spanner::v1::ResultSetStats {
                        row_count: Some(spanner_grpc_mock::google::spanner::v1::result_set_stats::RowCount::RowCountExact(1)),
                        ..Default::default()
                    }),
                    last: true,
                    ..Default::default()
                },
            )])))
        });

        // Commit expect
        mock.expect_commit().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.transaction,
                Some(commit_request::Transaction::TransactionId(vec![1, 2, 3]))
            );
            Ok(gaxi::grpc::tonic::Response::new(CommitResponse {
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;
        let address = db_client.spanner.config.endpoint.as_ref().unwrap();
        let dsn = format!(
            "{}/projects/p/instances/i/databases/d;useplaintext=true;autocommit=true",
            address
        );

        let mut conn = Connection::connect(&dsn).await.expect("connection failed");
        let res = conn
            .execute("UPDATE Users SET Name = 'Alice' WHERE Id = 1")
            .await
            .expect("execution failed");

        if let ExecutionResult::UpdateResult(count) = res {
            assert_eq!(count, 1);
        } else {
            panic!("Expected update result");
        }
    }

    #[tokio::test]
    async fn test_connection_autocommit_batch_dml_last_statements() {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        // Dialect check query
        mock.expect_execute_streaming_sql().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(
                setup_dialect_query_result("GOOGLE_STANDARD_SQL"),
            )])))
        });

        // Execute batch DML
        mock.expect_execute_batch_dml().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.statements.len(), 2);
            assert!(req.last_statements, "last_statements should be automatically set to true in autocommit Batch DML");

            let selector = req
                .transaction
                .expect("missing transaction selector")
                .selector
                .expect("missing selector");
            assert!(matches!(
                selector,
                transaction_selector::Selector::Begin(_)
            ));

            Ok(gaxi::grpc::tonic::Response::new(ExecuteBatchDmlResponse {
                result_sets: vec![
                    spanner_grpc_mock::google::spanner::v1::ResultSet {
                        metadata: Some(ResultSetMetadata {
                            transaction: Some(Transaction {
                                id: vec![1, 2, 3],
                                ..Default::default()
                            }),
                            ..Default::default()
                        }),
                        stats: Some(spanner_grpc_mock::google::spanner::v1::ResultSetStats {
                            row_count: Some(spanner_grpc_mock::google::spanner::v1::result_set_stats::RowCount::RowCountExact(1)),
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                    spanner_grpc_mock::google::spanner::v1::ResultSet {
                        stats: Some(spanner_grpc_mock::google::spanner::v1::ResultSetStats {
                            row_count: Some(spanner_grpc_mock::google::spanner::v1::result_set_stats::RowCount::RowCountExact(1)),
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                ],
                status: Some(spanner_grpc_mock::google::rpc::Status {
                    code: 0,
                    message: "OK".into(),
                    details: vec![],
                }),
                ..Default::default()
            }))
        });

        // Commit expect
        mock.expect_commit().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.transaction,
                Some(commit_request::Transaction::TransactionId(vec![1, 2, 3]))
            );
            Ok(gaxi::grpc::tonic::Response::new(CommitResponse {
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;
        let address = db_client.spanner.config.endpoint.as_ref().unwrap();
        let dsn = format!(
            "{}/projects/p/instances/i/databases/d;useplaintext=true;autocommit=true",
            address
        );

        let mut conn = Connection::connect(&dsn).await.expect("connection failed");

        conn.execute("START BATCH DML")
            .await
            .expect("start batch failed");
        conn.execute("UPDATE Users SET Name = 'Alice' WHERE Id = 1")
            .await
            .expect("statement 1 failed");
        conn.execute("UPDATE Users SET Name = 'Bob' WHERE Id = 2")
            .await
            .expect("statement 2 failed");
        let res = conn.execute("RUN BATCH").await.expect("run batch failed");

        if let ExecutionResult::BatchUpdateResult(counts) = res {
            assert_eq!(counts, vec![1, 1]);
        } else {
            panic!("Expected batch update result");
        }
    }
}
