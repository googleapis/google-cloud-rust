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

use super::SimpleParser;
use crate::connection::Dialect;
use crate::connection::commands;
use crate::connection::statements::*;

/// The type of SQL statement classified by the parser.
#[derive(Debug, PartialEq, Clone)]
#[non_exhaustive]
pub enum StatementType {
    /// A query statement (DQL) returning a result set.
    Query,
    /// An update statement (DML) returning a row count or a result set.
    Update {
        /// True if this DML has a returning / THEN RETURN clause.
        has_returning: bool,
    },
    /// A database definition statement (DDL) returning no rows.
    Ddl,
    /// A client-side execution control command.
    ClientSide(ClientSideCommand),
}

impl StatementType {
    pub(crate) fn into_executable(self) -> crate::connection::statements::ExecutableStatement {
        match self {
            StatementType::ClientSide(cmd) => {
                ExecutableStatement::ClientSide(ClientSideStatement(cmd))
            }
            StatementType::Query => ExecutableStatement::Query(QueryStatement),
            StatementType::Update { has_returning } => {
                ExecutableStatement::Update(UpdateStatement { has_returning })
            }
            StatementType::Ddl => ExecutableStatement::Ddl(DdlStatement),
        }
    }
}

/// Commands processed locally on the client connection.
#[derive(Debug, PartialEq, Clone)]
#[non_exhaustive]
pub enum ClientSideCommand {
    /// SET command to set connection options or extensions.
    Set {
        /// The property name or extension-prefixed variable name.
        key: String,
        /// The string representation of the value being assigned.
        value: String,
        /// True if this option is locally-scoped (e.g. SET LOCAL).
        is_local: bool,
        /// True if setting a transaction mode (e.g. SET TRANSACTION).
        is_transaction: bool,
    },
    /// SHOW command to view current options.
    Show {
        /// The property name to show.
        key: String,
    },
    /// BEGIN transaction.
    Begin {
        /// Optional readonly mode flag.
        readonly: Option<bool>,
    },
    /// COMMIT transaction.
    Commit,
    /// ROLLBACK transaction.
    Rollback,
    /// START BATCH DML.
    StartBatchDml,
    /// START BATCH DDL.
    StartBatchDdl,
    /// RUN BATCH.
    RunBatch,
    /// ABORT BATCH.
    AbortBatch,
    /// SAVEPOINT command.
    Savepoint {
        /// The name of the savepoint.
        name: String,
    },
    /// RELEASE SAVEPOINT command.
    ReleaseSavepoint {
        /// The name of the savepoint.
        name: String,
    },
    /// ROLLBACK TO SAVEPOINT command.
    RollbackToSavepoint {
        /// The name of the savepoint.
        name: String,
    },
    /// PREPARE statement (PostgreSQL only).
    Prepare {
        /// The prepared statement name.
        name: String,
        /// The actual SQL query string.
        sql: String,
    },
    /// EXECUTE statement (PostgreSQL only).
    Execute {
        /// The prepared statement name to execute.
        name: String,
        /// The inline parameter values.
        params: Vec<crate::value::Value>,
    },
}

/// Parse and classify a SQL query statement.
pub fn parse_statement(sql: &str, dialect: Dialect) -> Result<StatementType, crate::Error> {
    let mut parser = SimpleParser::new(sql, dialect);
    let first_keyword = match parser.read_keyword() {
        Some(kw) => kw.to_uppercase(),
        None => {
            return Err(crate::Error::deser(
                crate::connection::ConnectionError::SyntaxError("Empty SQL statement".to_string()),
            ));
        }
    };

    match first_keyword.as_str() {
        "SET" => Ok(StatementType::ClientSide(commands::set::parse(
            &mut parser,
            dialect,
        )?)),
        "SHOW" => {
            match commands::show::parse(&mut parser, dialect) {
                Ok(cmd) => Ok(StatementType::ClientSide(cmd)),
                Err(_) => {
                    // Fallback to Query (e.g. for native database SHOW queries or syntax errors)
                    Ok(StatementType::Query)
                }
            }
        }
        "BEGIN" | "START" => Ok(StatementType::ClientSide(commands::begin::parse(
            &mut parser,
            &first_keyword,
        )?)),
        "COMMIT" => Ok(StatementType::ClientSide(commands::commit::parse(
            &mut parser,
        )?)),
        "ROLLBACK" => Ok(StatementType::ClientSide(commands::rollback::parse(
            &mut parser,
        )?)),
        "SAVEPOINT" => Ok(StatementType::ClientSide(
            commands::savepoint::parse_savepoint(&mut parser)?,
        )),
        "RELEASE" => Ok(StatementType::ClientSide(
            commands::savepoint::parse_release(&mut parser)?,
        )),
        "RUN" => Ok(StatementType::ClientSide(commands::batch::parse_run(
            &mut parser,
        )?)),
        "ABORT" => Ok(StatementType::ClientSide(commands::batch::parse_abort(
            &mut parser,
        )?)),
        "PREPARE" if dialect == Dialect::PostgreSql => Ok(StatementType::ClientSide(
            commands::prepare::parse_prepare(&mut parser)?,
        )),
        "EXECUTE" if dialect == Dialect::PostgreSql => Ok(StatementType::ClientSide(
            commands::prepare::parse_execute(&mut parser)?,
        )),
        "SELECT" | "WITH" | "GRAPH" => Ok(StatementType::Query),
        "INSERT" | "UPDATE" | "DELETE" => {
            let has_returning = has_returning_clause(sql, dialect);
            Ok(StatementType::Update { has_returning })
        }
        "CREATE" | "ALTER" | "DROP" | "GRANT" | "REVOKE" | "RENAME" => Ok(StatementType::Ddl),
        _ => {
            // Default to Query or Update fallback depending on general sql format.
            // We assume standard statements are DML updates unless explicitly matches query keyword.
            let has_returning = has_returning_clause(sql, dialect);
            Ok(StatementType::Update { has_returning })
        }
    }
}

fn contains_case_insensitive(haystack: &str, needle: &str) -> bool {
    let needle_bytes = needle.as_bytes();
    if needle_bytes.is_empty() {
        return true;
    }
    if haystack.len() < needle.len() {
        return false;
    }
    haystack.as_bytes().windows(needle.len()).any(|window| {
        window
            .iter()
            .zip(needle_bytes.iter())
            .all(|(&h, &n)| h.eq_ignore_ascii_case(&n))
    })
}

fn has_returning_clause(sql: &str, dialect: Dialect) -> bool {
    match dialect {
        Dialect::GoogleSql => has_returning_clause_google_sql(sql),
        Dialect::PostgreSql => has_returning_clause_postgre_sql(sql),
    }
}

fn has_returning_clause_google_sql(sql: &str) -> bool {
    if !contains_case_insensitive(sql, "return") {
        return false;
    }

    let mut parser = SimpleParser::new(sql, Dialect::GoogleSql);
    // Skip the first keyword
    let _ = parser.read_keyword();

    while parser.pos < parser.sql.len() {
        let start_pos = parser.pos;
        parser.skip_whitespace_and_comments();
        if parser.pos == start_pos && parser.pos < parser.sql.len() {
            // Check if we can read a keyword
            if let Some(word) = parser.read_keyword() {
                let upper = word.to_uppercase();
                if upper == "THEN" {
                    let before_return = parser.pos;
                    parser.skip_whitespace_and_comments();
                    if let Some(next_word) = parser.read_keyword()
                        && next_word.to_uppercase() == "RETURN"
                    {
                        return true;
                    }
                    parser.pos = before_return;
                }
            } else {
                let c = parser.sql[parser.pos];
                if c == b'\'' || c == b'"' {
                    let _ = parser.eat_literal();
                } else {
                    parser.pos += 1;
                }
            }
        }
    }
    false
}

fn has_returning_clause_postgre_sql(sql: &str) -> bool {
    if !contains_case_insensitive(sql, "returning") {
        return false;
    }

    let mut parser = SimpleParser::new(sql, Dialect::PostgreSql);
    // Skip the first keyword
    let _ = parser.read_keyword();

    while parser.pos < parser.sql.len() {
        let start_pos = parser.pos;
        parser.skip_whitespace_and_comments();
        if parser.pos == start_pos && parser.pos < parser.sql.len() {
            if let Some(word) = parser.read_keyword() {
                if word.to_uppercase() == "RETURNING" {
                    return true;
                }
            } else {
                let c = parser.sql[parser.pos];
                if c == b'\'' || c == b'"' {
                    let _ = parser.eat_literal();
                } else {
                    parser.pos += 1;
                }
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_classify_standard_sql() {
        let q = parse_statement("SELECT * FROM Users;", Dialect::GoogleSql).expect("should parse");
        assert_eq!(q, StatementType::Query, "expected SELECT to be Query");

        let q2 = parse_statement(" WITH t AS (SELECT 1) SELECT * FROM t", Dialect::GoogleSql)
            .expect("should parse");
        assert_eq!(q2, StatementType::Query, "expected WITH to be Query");

        let u = parse_statement("INSERT INTO Users (id) VALUES (1);", Dialect::GoogleSql)
            .expect("should parse");
        assert_eq!(
            u,
            StatementType::Update {
                has_returning: false
            },
            "expected INSERT to be Update"
        );

        // DML with returning (GoogleSQL)
        let ur_gsql = parse_statement(
            "UPDATE Users SET name = 'foo' WHERE id = 1 THEN RETURN id, name;",
            Dialect::GoogleSql,
        )
        .expect("should parse");
        assert_eq!(
            ur_gsql,
            StatementType::Update {
                has_returning: true
            },
            "expected UPDATE with THEN RETURN to be Update with returning"
        );

        // DML with returning (PostgreSQL)
        let ur_pg = parse_statement(
            "UPDATE Users SET name = 'foo' WHERE id = 1 RETURNING id, name;",
            Dialect::PostgreSql,
        )
        .expect("should parse");
        assert_eq!(
            ur_pg,
            StatementType::Update {
                has_returning: true
            },
            "expected UPDATE with RETURNING to be Update with returning"
        );

        let d = parse_statement(
            "CREATE TABLE Users (id INT64) PRIMARY KEY (id);",
            Dialect::GoogleSql,
        )
        .expect("should parse");
        assert_eq!(d, StatementType::Ddl, "expected CREATE TABLE to be Ddl");
    }

    #[test]
    fn test_comments_skipping() {
        let q = parse_statement(" -- this is a comment \n SELECT 1;", Dialect::GoogleSql)
            .expect("should parse");
        assert_eq!(
            q,
            StatementType::Query,
            "expected SELECT after single line comment to be Query"
        );

        let q2 = parse_statement(
            " # this is a google sql comment \n SELECT 1;",
            Dialect::GoogleSql,
        )
        .expect("should parse");
        assert_eq!(
            q2,
            StatementType::Query,
            "expected SELECT after # comment to be Query"
        );

        let q3 = parse_statement(" /* block comment */ SELECT 1;", Dialect::GoogleSql)
            .expect("should parse");
        assert_eq!(
            q3,
            StatementType::Query,
            "expected SELECT after block comment to be Query"
        );

        let q4 = parse_statement(
            " /* nested /* block */ comment */ SELECT 1;",
            Dialect::GoogleSql,
        )
        .expect("should parse");
        assert_eq!(
            q4,
            StatementType::Query,
            "expected SELECT after nested block comment to be Query"
        );
    }

    #[test]
    fn test_returning_clause_detection() {
        // GoogleSQL checks
        assert!(has_returning_clause(
            "INSERT INTO Users (id) VALUES (1) THEN RETURN id",
            Dialect::GoogleSql
        ));
        assert!(has_returning_clause(
            "UPDATE Users SET name = 'foo' THEN   RETURN name",
            Dialect::GoogleSql
        ));
        assert!(has_returning_clause(
            "DELETE FROM Users THEN\nRETURN id",
            Dialect::GoogleSql
        ));
        assert!(!has_returning_clause(
            "INSERT INTO Users (id) VALUES (1)",
            Dialect::GoogleSql
        ));
        assert!(!has_returning_clause(
            "SELECT * FROM (UPDATE Users SET name = 'foo' THEN RETURN id)",
            Dialect::GoogleSql
        )); // nested? Yes, we start scan after first keyword, but let's make sure it checks basic.
        assert!(!has_returning_clause(
            "INSERT INTO Users (name) VALUES ('THEN RETURN')",
            Dialect::GoogleSql
        ));

        // PostgreSQL checks
        assert!(has_returning_clause(
            "INSERT INTO Users (id) VALUES (1) RETURNING id",
            Dialect::PostgreSql
        ));
        assert!(has_returning_clause(
            "UPDATE Users SET name = 'foo' RETURNING name",
            Dialect::PostgreSql
        ));
        assert!(has_returning_clause(
            "DELETE FROM Users RETURNING\nid",
            Dialect::PostgreSql
        ));
        assert!(!has_returning_clause(
            "INSERT INTO Users (id) VALUES (1)",
            Dialect::PostgreSql
        ));
        assert!(!has_returning_clause(
            "INSERT INTO Users (name) VALUES ('RETURNING')",
            Dialect::PostgreSql
        ));
    }

    #[test]
    fn test_parse_set_commands() {
        let s1 =
            parse_statement("SET AUTOCOMMIT = true", Dialect::GoogleSql).expect("should parse");
        assert_eq!(
            s1,
            StatementType::ClientSide(ClientSideCommand::Set {
                key: "autocommit".to_string(),
                value: "true".to_string(),
                is_local: false,
                is_transaction: false,
            }),
            "expected SET AUTOCOMMIT to match"
        );

        let s2 = parse_statement("SET LOCAL my_property = 'foo'", Dialect::GoogleSql)
            .expect("should parse");
        assert_eq!(
            s2,
            StatementType::ClientSide(ClientSideCommand::Set {
                key: "my_property".to_string(),
                value: "foo".to_string(),
                is_local: true,
                is_transaction: false,
            }),
            "expected SET LOCAL my_property to match"
        );
    }

    #[test]
    fn test_parse_show_commands() {
        let s1 = parse_statement("SHOW AUTOCOMMIT", Dialect::GoogleSql).expect("should parse");
        assert_eq!(
            s1,
            StatementType::ClientSide(ClientSideCommand::Show {
                key: "autocommit".to_string(),
            }),
            "expected SHOW AUTOCOMMIT to match"
        );
    }

    #[test]
    fn test_go_set_statement_cases() {
        // SET local my_property = 'foo' (case-insensitive local)
        let s1 = parse_statement("set local my_property = 'foo'", Dialect::GoogleSql)
            .expect("should parse");
        assert_eq!(
            s1,
            StatementType::ClientSide(ClientSideCommand::Set {
                key: "my_property".to_string(),
                value: "foo".to_string(),
                is_local: true,
                is_transaction: false,
            }),
            "expected set local my_property to match"
        );

        // SET LOCAL my_property = 'foo'
        let s2 = parse_statement("SET LOCAL my_property = 'foo'", Dialect::GoogleSql)
            .expect("should parse");
        assert_eq!(
            s2,
            StatementType::ClientSide(ClientSideCommand::Set {
                key: "my_property".to_string(),
                value: "foo".to_string(),
                is_local: true,
                is_transaction: false,
            }),
            "expected SET LOCAL my_property to match"
        );

        // set my_property = 1
        let s3 = parse_statement("set my_property = 1", Dialect::GoogleSql).expect("should parse");
        assert_eq!(
            s3,
            StatementType::ClientSide(ClientSideCommand::Set {
                key: "my_property".to_string(),
                value: "1".to_string(),
                is_local: false,
                is_transaction: false,
            }),
            "expected SET integer to match"
        );

        // comment spacing: set \n -- comment \n my_property /* yet more comments */ = \ntrue/*comment*/
        let s4 = parse_statement(
            "set \n -- comment \n my_property /* yet more comments */ = \ntrue/*comment*/",
            Dialect::GoogleSql,
        )
        .expect("should parse");
        assert_eq!(
            s4,
            StatementType::ClientSide(ClientSideCommand::Set {
                key: "my_property".to_string(),
                value: "true".to_string(),
                is_local: false,
                is_transaction: false,
            }),
            "expected comments-stripped SET to match"
        );

        // set transaction read write
        let s5 = parse_statement("set transaction read write", Dialect::GoogleSql)
            .expect("should parse");
        assert_eq!(
            s5,
            StatementType::ClientSide(ClientSideCommand::Set {
                key: "transaction".to_string(),
                value: "read write".to_string(),
                is_local: true,
                is_transaction: true,
            }),
            "expected SET TRANSACTION read write to match"
        );

        // invalid: set my_property =
        let s6 = parse_statement("set my_property =", Dialect::GoogleSql);
        assert!(s6.is_err(), "expected error for missing value");

        // invalid: set my_property = 'foo' bar
        let s7 = parse_statement("set my_property = 'foo' bar", Dialect::GoogleSql);
        assert!(s7.is_err(), "expected error for extra trailing tokens");

        // Non-standalone keyword checks: SET followed by non-ASCII unicode character, dot, or alphanumeric characters
        let s_unicode = parse_statement("SETä = true;", Dialect::GoogleSql).expect("should parse");
        assert_eq!(
            s_unicode,
            StatementType::Update {
                has_returning: false
            },
            "SET followed by unicode character should fallback to generic Update statement"
        );

        let s_dot = parse_statement("SET.foo = 1;", Dialect::GoogleSql).expect("should parse");
        assert_eq!(
            s_dot,
            StatementType::Update {
                has_returning: false
            },
            "SET followed by dot should fallback to generic Update statement (it is a path prefix)"
        );

        let s_alphanum = parse_statement("SET1 = 2;", Dialect::GoogleSql).expect("should parse");
        assert_eq!(
            s_alphanum,
            StatementType::Update {
                has_returning: false
            },
            "SET followed by digit should fallback to generic Update statement"
        );

        let s_invalid_set = parse_statement("SET  .foo = 1;", Dialect::GoogleSql);
        assert!(
            s_invalid_set.is_err(),
            "SET followed by whitespace and a dot should parse as SET command but fail with SyntaxError"
        );
    }

    #[test]
    fn test_parse_transaction_controls() {
        let b1 = parse_statement("BEGIN;", Dialect::GoogleSql).expect("should parse");
        assert_eq!(
            b1,
            StatementType::ClientSide(ClientSideCommand::Begin { readonly: None }),
            "expected BEGIN to match"
        );

        let b2 = parse_statement("START TRANSACTION readonly", Dialect::GoogleSql)
            .expect("should parse");
        assert_eq!(
            b2,
            StatementType::ClientSide(ClientSideCommand::Begin {
                readonly: Some(true)
            }),
            "expected START TRANSACTION readonly to match"
        );

        let c1 = parse_statement("COMMIT /*comment*/ ;", Dialect::GoogleSql).expect("should parse");
        assert_eq!(
            c1,
            StatementType::ClientSide(ClientSideCommand::Commit),
            "expected COMMIT to match"
        );

        let r1 = parse_statement("ROLLBACK", Dialect::GoogleSql).expect("should parse");
        assert_eq!(
            r1,
            StatementType::ClientSide(ClientSideCommand::Rollback),
            "expected ROLLBACK to match"
        );
    }

    #[test]
    fn test_parse_batch_commands() {
        let s1 = parse_statement("START BATCH DDL", Dialect::GoogleSql).expect("should parse");
        assert_eq!(
            s1,
            StatementType::ClientSide(ClientSideCommand::StartBatchDdl),
            "expected START BATCH DDL to match"
        );

        let s2 = parse_statement("START BATCH DML;", Dialect::GoogleSql).expect("should parse");
        assert_eq!(
            s2,
            StatementType::ClientSide(ClientSideCommand::StartBatchDml),
            "expected START BATCH DML to match"
        );

        let r1 = parse_statement("RUN BATCH", Dialect::GoogleSql).expect("should parse");
        assert_eq!(
            r1,
            StatementType::ClientSide(ClientSideCommand::RunBatch),
            "expected RUN BATCH to match"
        );

        let a1 = parse_statement("ABORT BATCH", Dialect::GoogleSql).expect("should parse");
        assert_eq!(
            a1,
            StatementType::ClientSide(ClientSideCommand::AbortBatch),
            "expected ABORT BATCH to match"
        );
    }

    #[test]
    fn test_parse_prepare_and_execute() {
        let p1 = parse_statement(
            "PREPARE my_statement(int, text) AS SELECT * FROM Users WHERE id = $1 AND name = $2;",
            Dialect::PostgreSql,
        )
        .expect("should parse");
        assert_eq!(
            p1,
            StatementType::ClientSide(ClientSideCommand::Prepare {
                name: "my_statement".to_string(),
                sql: "SELECT * FROM Users WHERE id = $1 AND name = $2".to_string()
            })
        );

        let e1 = parse_statement("EXECUTE my_statement(1, 'foo');", Dialect::PostgreSql)
            .expect("should parse");
        if let StatementType::ClientSide(ClientSideCommand::Execute { name, params }) = e1 {
            assert_eq!(name, "my_statement");
            assert_eq!(params.len(), 2);
        } else {
            panic!("expected StatementType::ClientSide(ClientSideCommand::Execute)");
        }
    }
}
