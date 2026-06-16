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

pub(crate) mod clientside;
pub(crate) mod ddl;
pub(crate) mod query;
pub(crate) mod update;

pub(crate) use clientside::ClientSideStatement;
pub(crate) use ddl::DdlStatement;
pub(crate) use query::QueryStatement;
pub(crate) use update::UpdateStatement;

use crate::Error;
use crate::connection::{Connection, ExecutionResult};
use crate::statement::Statement;

/// Represents control flow status after running an ExecutableStatement.
pub(crate) enum StatementStatus {
    /// The statement completed and returned the final result.
    Done(ExecutionResult),
    /// The statement modified target statement; run loop should continue.
    Continue,
}

/// Represents a statement classified and dispatchable for execution.
pub(crate) enum ExecutableStatement {
    ClientSide(ClientSideStatement),
    Query(QueryStatement),
    Update(UpdateStatement),
    Ddl(DdlStatement),
}

impl ExecutableStatement {
    pub(crate) async fn execute(
        self,
        conn: &mut Connection,
        statement: &mut Statement,
    ) -> Result<StatementStatus, Error> {
        match self {
            ExecutableStatement::ClientSide(s) => s.execute(conn, statement).await,
            ExecutableStatement::Query(s) => s.execute(conn, statement).await,
            ExecutableStatement::Update(s) => s.execute(conn, statement).await,
            ExecutableStatement::Ddl(s) => s.execute(conn, statement).await,
        }
    }
}
