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

use crate::connection::batch::ConnectionBatch;
use crate::connection::connection::{Connection, ExecutionResult};
use crate::connection::parser::{ClientSideCommand, SimpleParser};

pub(crate) fn parse_run(parser: &mut SimpleParser<'_>) -> Result<ClientSideCommand, crate::Error> {
    if parser.eat_keyword("BATCH") {
        let _ = parser.eat_token(b';');
        Ok(ClientSideCommand::RunBatch)
    } else {
        Err(crate::Error::deser(
            crate::connection::ConnectionError::SyntaxError(
                "Invalid RUN statement. Expected RUN BATCH".to_string(),
            ),
        ))
    }
}

pub(crate) fn parse_abort(
    parser: &mut SimpleParser<'_>,
) -> Result<ClientSideCommand, crate::Error> {
    if parser.eat_keyword("BATCH") {
        let _ = parser.eat_token(b';');
        Ok(ClientSideCommand::AbortBatch)
    } else {
        Err(crate::Error::deser(
            crate::connection::ConnectionError::SyntaxError(
                "Invalid ABORT statement. Expected ABORT BATCH".to_string(),
            ),
        ))
    }
}

pub(crate) async fn execute_run(conn: &mut Connection) -> Result<ExecutionResult, crate::Error> {
    let current_batch = std::mem::replace(&mut conn.batch, ConnectionBatch::None);
    match current_batch {
        ConnectionBatch::None => Err(crate::Error::deser("No active batch to run")),
        ConnectionBatch::Dml(batch) => batch.run(conn).await,
        ConnectionBatch::Ddl(batch) => {
            let db_path = conn.db_path.clone();
            let admin = conn.get_database_admin().await?;
            batch.run(admin, &db_path).await
        }
    }
}

pub(crate) async fn execute_abort(conn: &mut Connection) -> Result<ExecutionResult, crate::Error> {
    let current_batch = std::mem::replace(&mut conn.batch, ConnectionBatch::None);
    match current_batch {
        ConnectionBatch::None => Err(crate::Error::deser("No active batch to abort")),
        ConnectionBatch::Dml(_) | ConnectionBatch::Ddl(_) => Ok(ExecutionResult::Success),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::Dialect;
    use crate::connection::parser::SimpleParser;

    #[test]
    fn test_parse_run_batch() {
        let mut parser = SimpleParser::new("BATCH;", Dialect::GoogleSql);
        let cmd = parse_run(&mut parser).unwrap();
        assert!(matches!(cmd, ClientSideCommand::RunBatch));

        let mut parser = SimpleParser::new("OTHER;", Dialect::GoogleSql);
        let cmd = parse_run(&mut parser);
        assert!(cmd.is_err());
    }

    #[test]
    fn test_parse_abort_batch() {
        let mut parser = SimpleParser::new("BATCH;", Dialect::GoogleSql);
        let cmd = parse_abort(&mut parser).unwrap();
        assert!(matches!(cmd, ClientSideCommand::AbortBatch));

        let mut parser = SimpleParser::new("OTHER;", Dialect::GoogleSql);
        let cmd = parse_abort(&mut parser);
        assert!(cmd.is_err());
    }
}
