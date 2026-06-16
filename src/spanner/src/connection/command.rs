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

use crate::connection::commands;
use crate::connection::connection::{Connection, ExecutionResult};
use crate::connection::parser::ClientSideCommand;
use crate::connection::statements::StatementStatus;
use crate::statement::Statement;

impl ClientSideCommand {
    pub(crate) async fn execute(
        self,
        conn: &mut Connection,
        statement: &mut Statement,
    ) -> Result<StatementStatus, crate::Error> {
        match self {
            ClientSideCommand::Set {
                key,
                value,
                is_local,
                is_transaction,
            } => {
                let res =
                    commands::set::execute(conn, key, value, is_local, is_transaction).await?;
                Ok(StatementStatus::Done(res))
            }
            ClientSideCommand::Show { key } => {
                let res = commands::show::execute(conn, key).await?;
                Ok(StatementStatus::Done(res))
            }
            ClientSideCommand::Begin { readonly } => {
                let res = commands::begin::execute_begin(conn, readonly).await?;
                Ok(StatementStatus::Done(res))
            }
            ClientSideCommand::Commit => {
                let res = commands::commit::execute(conn).await?;
                Ok(StatementStatus::Done(res))
            }
            ClientSideCommand::Rollback => {
                let res = commands::rollback::execute(conn).await?;
                Ok(StatementStatus::Done(res))
            }
            ClientSideCommand::StartBatchDml => {
                let res = commands::begin::execute_start_batch_dml(conn).await?;
                Ok(StatementStatus::Done(res))
            }
            ClientSideCommand::StartBatchDdl => {
                let res = commands::begin::execute_start_batch_ddl(conn).await?;
                Ok(StatementStatus::Done(res))
            }
            ClientSideCommand::RunBatch => {
                let res = commands::batch::execute_run(conn).await?;
                Ok(StatementStatus::Done(res))
            }
            ClientSideCommand::AbortBatch => {
                let res = commands::batch::execute_abort(conn).await?;
                Ok(StatementStatus::Done(res))
            }
            ClientSideCommand::Savepoint { name } => {
                let res = conn.execute_savepoint(name).await?;
                Ok(StatementStatus::Done(res))
            }
            ClientSideCommand::ReleaseSavepoint { name } => {
                let res = conn.execute_release_savepoint(name).await?;
                Ok(StatementStatus::Done(res))
            }
            ClientSideCommand::RollbackToSavepoint { name } => {
                let res = conn.execute_rollback_to_savepoint(name).await?;
                Ok(StatementStatus::Done(res))
            }
            ClientSideCommand::Prepare { name, sql } => {
                conn.prepared_statements.insert(name, sql);
                Ok(StatementStatus::Done(ExecutionResult::Success))
            }
            ClientSideCommand::Execute { name, params } => {
                let sql = conn
                    .prepared_statements
                    .get(&name)
                    .ok_or_else(|| {
                        crate::Error::deser(format!("Prepared statement '{}' does not exist", name))
                    })?
                    .clone();

                // Construct a new Statement with the prepared SQL query string,
                // mapping positional parameter values to $1, $2, etc., and ignoring
                // parameters bound on the EXECUTE statement itself.
                let mut mapped_params = std::collections::BTreeMap::new();
                for (idx, val) in params.into_iter().enumerate() {
                    let param_name = format!("${}", idx + 1);
                    mapped_params.insert(param_name, val);
                }

                *statement = statement.clone_with_sql_and_params(sql, mapped_params);

                Ok(StatementStatus::Continue)
            }
        }
    }
}
