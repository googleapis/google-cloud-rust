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

use crate::connection::connection::{Connection, ExecutionResult};
use crate::connection::parser::{ClientSideCommand, SimpleParser};

pub(crate) fn parse(parser: &mut SimpleParser<'_>) -> Result<ClientSideCommand, crate::Error> {
    if parser.eat_keyword("TO") {
        let _ = parser.eat_keyword("SAVEPOINT");
        let name = parser.eat_identifier().ok_or_else(|| {
            crate::Error::deser(crate::connection::ConnectionError::SyntaxError(
                "Missing savepoint name".to_string(),
            ))
        })?;
        super::savepoint::validate_savepoint_name(&name)?;
        let _ = parser.eat_token(b';');
        return Ok(ClientSideCommand::RollbackToSavepoint { name });
    }
    let _ = parser.eat_keyword("TRANSACTION");
    let _ = parser.eat_token(b';');
    Ok(ClientSideCommand::Rollback)
}

pub(crate) async fn execute(conn: &mut Connection) -> Result<ExecutionResult, crate::Error> {
    if conn.transaction.is_none() {
        return Err(crate::Error::deser("No active transaction to rollback"));
    }
    conn.rollback_active_transaction().await?;
    Ok(ExecutionResult::Success)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::Dialect;
    use crate::connection::parser::SimpleParser;

    #[test]
    fn test_parse_rollback() {
        let mut parser = SimpleParser::new(";", Dialect::GoogleSql);
        let cmd = parse(&mut parser).unwrap();
        assert!(matches!(cmd, ClientSideCommand::Rollback));

        let mut parser = SimpleParser::new("TRANSACTION;", Dialect::GoogleSql);
        let cmd = parse(&mut parser).unwrap();
        assert!(matches!(cmd, ClientSideCommand::Rollback));
    }

    #[test]
    fn test_parse_rollback_to_savepoint() {
        let mut parser = SimpleParser::new("TO SAVEPOINT s1;", Dialect::GoogleSql);
        let cmd = parse(&mut parser).unwrap();
        assert!(matches!(cmd, ClientSideCommand::RollbackToSavepoint { name } if name == "s1"));

        // omit keyword SAVEPOINT
        let mut parser = SimpleParser::new("TO s2;", Dialect::GoogleSql);
        let cmd = parse(&mut parser).unwrap();
        assert!(matches!(cmd, ClientSideCommand::RollbackToSavepoint { name } if name == "s2"));

        // invalid savepoint name format
        let mut parser = SimpleParser::new("TO 1s;", Dialect::GoogleSql);
        let cmd = parse(&mut parser);
        assert!(cmd.is_err());
    }
}
