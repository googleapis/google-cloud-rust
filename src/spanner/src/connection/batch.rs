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
use crate::batch::BatchDml;
use crate::connection::connection::{Connection, ExecutionResult};
use crate::connection::parser::{ClientSideCommand, StatementType};
use crate::statement::Statement;
use crate::transaction_runner::TransactionResult;
use google_cloud_lro::Poller;
use google_cloud_spanner_admin_database_v1::client::DatabaseAdmin;

pub(crate) enum ConnectionBatch {
    None,
    Dml(DmlBatch),
    Ddl(DdlBatch),
}

pub(crate) struct DmlBatch {
    statements: Vec<Statement>,
}

impl DmlBatch {
    pub(crate) fn new() -> Self {
        Self {
            statements: Vec::new(),
        }
    }

    fn add(&mut self, statement: Statement) {
        self.statements.push(statement);
    }

    pub(crate) fn handle_statement(
        &mut self,
        statement: Statement,
        stmt_type: &StatementType,
    ) -> Result<Option<ExecutionResult>, Error> {
        match stmt_type {
            StatementType::Update { .. } => {
                self.add(statement);
                Ok(Some(ExecutionResult::Success))
            }
            StatementType::ClientSide(ClientSideCommand::RunBatch)
            | StatementType::ClientSide(ClientSideCommand::AbortBatch) => Ok(None),
            _ => Err(Error::deser(
                "Only DML updates or batch control statements can be run inside a DML batch",
            )),
        }
    }

    pub(crate) async fn run(self, conn: &mut Connection) -> Result<ExecutionResult, Error> {
        if self.statements.is_empty() {
            return Ok(ExecutionResult::Success);
        }
        let mut builder = BatchDml::builder();
        for stmt in self.statements {
            builder = builder.add_statement(stmt);
        }
        if conn.transaction.is_none() && conn.autocommit() {
            builder = builder.set_last_statements(true);
        }
        let batch_dml = builder.build();

        if conn.transaction.is_none() && !conn.autocommit() {
            conn.start_transaction(None).await?;
        }

        if let Some(active_tx) = conn.transaction.as_mut() {
            active_tx.execute_batch_update(batch_dml).await
        } else {
            let runner = conn.client.read_write_transaction().build().await?;
            let counts: TransactionResult<Vec<i64>> = runner
                .run(async move |tx| tx.execute_batch_update(batch_dml.clone()).await)
                .await?;

            Ok(ExecutionResult::BatchUpdateResult(counts.result))
        }
    }
}

pub(crate) struct DdlBatch {
    statements: Vec<String>,
}

impl DdlBatch {
    pub(crate) fn new() -> Self {
        Self {
            statements: Vec::new(),
        }
    }

    pub(crate) fn add(&mut self, sql: String) {
        self.statements.push(sql);
    }

    pub(crate) fn handle_statement(
        &mut self,
        statement: Statement,
        stmt_type: &StatementType,
    ) -> Result<Option<ExecutionResult>, Error> {
        match stmt_type {
            StatementType::Ddl => {
                self.add(statement.sql().to_string());
                Ok(Some(ExecutionResult::Success))
            }
            StatementType::ClientSide(ClientSideCommand::RunBatch)
            | StatementType::ClientSide(ClientSideCommand::AbortBatch) => Ok(None),
            _ => Err(Error::deser(
                "Only DDL statements or batch control statements can be run inside a DDL batch",
            )),
        }
    }

    pub(crate) async fn run(
        self,
        admin_client: &DatabaseAdmin,
        db_path: &str,
    ) -> Result<ExecutionResult, Error> {
        if self.statements.is_empty() {
            return Ok(ExecutionResult::Success);
        }

        admin_client
            .update_database_ddl()
            .set_database(db_path.to_string())
            .set_statements(self.statements)
            .poller()
            .until_done()
            .await?;

        Ok(ExecutionResult::Success)
    }
}

impl ConnectionBatch {
    pub(crate) fn handle_statement(
        &mut self,
        statement: Statement,
        stmt_type: &StatementType,
    ) -> Result<Option<ExecutionResult>, Error> {
        match self {
            ConnectionBatch::None => Ok(None),
            ConnectionBatch::Dml(batch) => batch.handle_statement(statement, stmt_type),
            ConnectionBatch::Ddl(batch) => batch.handle_statement(statement, stmt_type),
        }
    }
}
