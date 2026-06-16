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

use crate::connection::parser::{ClientSideCommand, SimpleParser};

pub(crate) fn parse_prepare(
    parser: &mut SimpleParser<'_>,
) -> Result<ClientSideCommand, crate::Error> {
    let name = parser.eat_identifier().ok_or_else(|| {
        crate::Error::deser(crate::connection::ConnectionError::SyntaxError(
            "Missing prepared statement name".to_string(),
        ))
    })?;

    // Optionally parse parameter types list, e.g. (int, text). We can just skip/eat the parentheses contents.
    if parser.eat_token(b'(') {
        let mut depth = 1;
        while depth > 0 && parser.pos < parser.sql.len() {
            let c = parser.sql[parser.pos];
            if c == b'(' {
                depth += 1;
            } else if c == b')' {
                depth -= 1;
            }
            parser.pos += 1;
        }
    }

    // Parse AS keyword
    if !parser.eat_keyword("AS") {
        return Err(crate::Error::deser(
            crate::connection::ConnectionError::SyntaxError(
                "Missing 'AS' keyword in PREPARE statement".to_string(),
            ),
        ));
    }

    // The rest of the SQL string is the query statement.
    // Trim comments and trailing semicolons.
    let sql_start = parser.pos;
    let mut sql_end = parser.sql.len();

    // Scan to find if there is a trailing semicolon, or just take the rest.
    while parser.pos < parser.sql.len() {
        if parser.sql[parser.pos] == b';' {
            sql_end = parser.pos;
            break;
        }
        parser.pos += 1;
    }

    let sql = String::from_utf8_lossy(&parser.sql[sql_start..sql_end])
        .trim()
        .to_string();
    if sql.is_empty() {
        return Err(crate::Error::deser(
            crate::connection::ConnectionError::SyntaxError(
                "Missing SQL statement in PREPARE".to_string(),
            ),
        ));
    }

    Ok(ClientSideCommand::Prepare { name, sql })
}

pub(crate) fn parse_execute(
    parser: &mut SimpleParser<'_>,
) -> Result<ClientSideCommand, crate::Error> {
    let name = parser.eat_identifier().ok_or_else(|| {
        crate::Error::deser(crate::connection::ConnectionError::SyntaxError(
            "Missing prepared statement name to execute".to_string(),
        ))
    })?;

    let params = parse_inline_values(parser)?;
    let _ = parser.eat_token(b';');

    Ok(ClientSideCommand::Execute { name, params })
}

fn parse_inline_values(
    parser: &mut SimpleParser<'_>,
) -> Result<Vec<crate::value::Value>, crate::Error> {
    let mut values = Vec::new();
    parser.skip_whitespace_and_comments();
    if parser.eat_token(b'(') {
        parser.skip_whitespace_and_comments();
        if parser.eat_token(b')') {
            return Ok(values);
        }
        loop {
            parser.skip_whitespace_and_comments();
            let val = parser.eat_literal_value()?;
            values.push(val);
            parser.skip_whitespace_and_comments();
            if parser.eat_token(b')') {
                break;
            }
            if !parser.eat_token(b',') {
                return Err(crate::Error::deser(
                    crate::connection::ConnectionError::SyntaxError(
                        "Expected ',' or ')' in EXECUTE parameter list".to_string(),
                    ),
                ));
            }
        }
    }
    Ok(values)
}
