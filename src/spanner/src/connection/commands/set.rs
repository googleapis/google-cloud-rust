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

use crate::connection::Dialect;
use crate::connection::connection::{Connection, ExecutionResult};
use crate::connection::parser::{ClientSideCommand, SimpleParser};

pub(crate) fn parse(
    parser: &mut SimpleParser<'_>,
    dialect: Dialect,
) -> Result<ClientSideCommand, crate::Error> {
    let is_local = parser.eat_keyword("LOCAL");
    let mut is_transaction = false;
    if !is_local {
        is_transaction = parser.eat_keyword("TRANSACTION");
    }
    if !is_local && !is_transaction && dialect == Dialect::PostgreSql {
        let _ = parser.eat_keyword("SESSION");
    }

    if is_transaction {
        let start = parser.pos;
        while parser.pos < parser.sql.len() && parser.sql[parser.pos] != b';' {
            parser.pos += 1;
        }
        let value = String::from_utf8_lossy(&parser.sql[start..parser.pos])
            .trim()
            .to_string();
        return Ok(ClientSideCommand::Set {
            key: "transaction".to_string(),
            value,
            is_local: true,
            is_transaction: true,
        });
    }

    let key = match parser.eat_identifier() {
        Some(k) => k,
        None => {
            return Err(crate::Error::deser(
                crate::connection::ConnectionError::SyntaxError(
                    "Missing property name in SET statement".to_string(),
                ),
            ));
        }
    };

    let mut has_assign = parser.eat_token(b'=');
    if !has_assign && dialect == Dialect::PostgreSql {
        has_assign = parser.eat_keyword("TO");
    }
    if !has_assign {
        return Err(crate::Error::deser(
            crate::connection::ConnectionError::SyntaxError(
                "Missing '=' or 'TO' in SET statement".to_string(),
            ),
        ));
    }

    let value = parser.eat_literal()?;
    let _ = parser.eat_token(b';');

    parser.skip_whitespace_and_comments();
    if parser.pos < parser.sql.len() {
        return Err(crate::Error::deser(
            crate::connection::ConnectionError::SyntaxError(
                "Extra tokens after SET statement".to_string(),
            ),
        ));
    }

    Ok(ClientSideCommand::Set {
        key,
        value,
        is_local,
        is_transaction: false,
    })
}

pub(crate) async fn execute(
    conn: &mut Connection,
    key: String,
    value: String,
    is_local: bool,
    _is_transaction: bool,
) -> Result<ExecutionResult, crate::Error> {
    let is_in_manual_tx = !conn.autocommit() && conn.transaction.is_some();
    if is_local && !is_in_manual_tx {
        return Ok(ExecutionResult::Success);
    }

    conn.state.set(&key, &value, is_local, false)?;

    Ok(ExecutionResult::Success)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::Dialect;
    use crate::connection::parser::SimpleParser;

    #[test]
    fn test_parse_set() {
        let mut parser = SimpleParser::new("autocommit = false;", Dialect::GoogleSql);
        let cmd = parse(&mut parser, Dialect::GoogleSql).unwrap();
        assert!(matches!(
            cmd,
            ClientSideCommand::Set {
                ref key,
                ref value,
                is_local: false,
                is_transaction: false
            } if key == "autocommit" && value == "false"
        ));

        // PG TO syntax
        let mut parser = SimpleParser::new("autocommit TO false;", Dialect::PostgreSql);
        let cmd = parse(&mut parser, Dialect::PostgreSql).unwrap();
        assert!(matches!(
            cmd,
            ClientSideCommand::Set {
                ref key,
                ref value,
                is_local: false,
                is_transaction: false
            } if key == "autocommit" && value == "false"
        ));

        // LOCAL transaction syntax is invalid and should fail
        let mut parser = SimpleParser::new(
            "LOCAL TRANSACTION isolation level read committed;",
            Dialect::GoogleSql,
        );
        let cmd = parse(&mut parser, Dialect::GoogleSql);
        assert!(cmd.is_err());
    }
}
