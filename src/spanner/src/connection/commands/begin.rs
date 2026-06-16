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

use crate::connection::batch::{ConnectionBatch, DdlBatch, DmlBatch};
use crate::connection::connection::{Connection, ExecutionResult};
use crate::connection::parser::{ClientSideCommand, SimpleParser};

pub(crate) fn parse(
    parser: &mut SimpleParser<'_>,
    first_keyword: &str,
) -> Result<ClientSideCommand, crate::Error> {
    if first_keyword == "START" {
        if !parser.eat_keyword("TRANSACTION") {
            if parser.eat_keyword("BATCH") {
                if parser.eat_keyword("DML") {
                    let _ = parser.eat_token(b';');
                    return Ok(ClientSideCommand::StartBatchDml);
                } else if parser.eat_keyword("DDL") {
                    let _ = parser.eat_token(b';');
                    return Ok(ClientSideCommand::StartBatchDdl);
                }
            }
            return Err(crate::Error::deser(
                crate::connection::ConnectionError::SyntaxError(
                    "Invalid START statement".to_string(),
                ),
            ));
        }
    } else {
        let _ = parser.eat_keyword("TRANSACTION");
    }

    let mut readonly = None;
    loop {
        if parser.eat_keyword("READ") {
            if parser.eat_keyword("ONLY") {
                readonly = Some(true);
            } else if parser.eat_keyword("WRITE") {
                readonly = Some(false);
            } else {
                return Err(crate::Error::deser(
                    crate::connection::ConnectionError::SyntaxError(
                        "Expected ONLY or WRITE after READ".to_string(),
                    ),
                ));
            }
        } else if parser.eat_keyword("ISOLATION") {
            if !parser.eat_keyword("LEVEL") {
                return Err(crate::Error::deser(
                    crate::connection::ConnectionError::SyntaxError(
                        "Expected LEVEL after ISOLATION".to_string(),
                    ),
                ));
            }
            let _ = parser.read_keyword();
        } else if parser.eat_keyword("NOT") {
            if parser.eat_keyword("DEFERRABLE") {
                // ignore
            } else {
                return Err(crate::Error::deser(
                    crate::connection::ConnectionError::SyntaxError(
                        "Expected DEFERRABLE after NOT".to_string(),
                    ),
                ));
            }
        } else if parser.eat_keyword("DEFERRABLE") {
            // ignore
        } else {
            break;
        }
        let _ = parser.eat_token(b',');
    }

    let _ = parser.eat_token(b';');
    Ok(ClientSideCommand::Begin { readonly })
}

pub(crate) async fn execute_begin(
    conn: &mut Connection,
    readonly: Option<bool>,
) -> Result<ExecutionResult, crate::Error> {
    if conn.transaction.is_some() {
        return Err(crate::Error::deser("Transaction is already active"));
    }
    conn.start_transaction(readonly).await?;
    Ok(ExecutionResult::Success)
}

pub(crate) async fn execute_start_batch_dml(
    conn: &mut Connection,
) -> Result<ExecutionResult, crate::Error> {
    conn.batch = ConnectionBatch::Dml(DmlBatch::new());
    Ok(ExecutionResult::Success)
}

pub(crate) async fn execute_start_batch_ddl(
    conn: &mut Connection,
) -> Result<ExecutionResult, crate::Error> {
    if conn.transaction.is_some() {
        return Err(crate::Error::deser(
            "Cannot start a DDL batch inside an active transaction",
        ));
    }
    conn.batch = ConnectionBatch::Ddl(DdlBatch::new());
    Ok(ExecutionResult::Success)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::Dialect;
    use crate::connection::parser::SimpleParser;

    #[test]
    fn test_parse_begin() {
        let mut parser = SimpleParser::new(";", Dialect::GoogleSql);
        let cmd = parse(&mut parser, "BEGIN").unwrap();
        assert!(matches!(cmd, ClientSideCommand::Begin { .. }));

        let mut parser = SimpleParser::new("TRANSACTION;", Dialect::GoogleSql);
        let cmd = parse(&mut parser, "BEGIN").unwrap();
        assert!(matches!(cmd, ClientSideCommand::Begin { .. }));

        let mut parser = SimpleParser::new("TRANSACTION;", Dialect::GoogleSql);
        let cmd = parse(&mut parser, "START").unwrap();
        assert!(matches!(cmd, ClientSideCommand::Begin { .. }));
    }

    #[test]
    fn test_parse_start_batch() {
        let mut parser = SimpleParser::new("BATCH DML;", Dialect::GoogleSql);
        let cmd = parse(&mut parser, "START").unwrap();
        assert!(matches!(cmd, ClientSideCommand::StartBatchDml));

        let mut parser = SimpleParser::new("BATCH DDL;", Dialect::GoogleSql);
        let cmd = parse(&mut parser, "START").unwrap();
        assert!(matches!(cmd, ClientSideCommand::StartBatchDdl));

        // invalid start batch
        let mut parser = SimpleParser::new("BATCH OTHER;", Dialect::GoogleSql);
        let cmd = parse(&mut parser, "START");
        assert!(cmd.is_err());
    }
}
