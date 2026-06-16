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
use crate::google::spanner::v1;
use crate::result_set::ResultSet;
use crate::result_set_metadata::ResultSetMetadata;
use crate::row::Row;
use crate::to_value::ToValue;
use crate::types::TypeCode;

pub(crate) fn parse(
    parser: &mut SimpleParser<'_>,
    dialect: Dialect,
) -> Result<ClientSideCommand, crate::Error> {
    let has_variable = parser.eat_keyword("VARIABLE");
    if dialect == Dialect::GoogleSql && !has_variable {
        return Err(crate::Error::deser(
            crate::connection::ConnectionError::SyntaxError(
                "GoogleSQL SHOW statements must use SHOW VARIABLE".to_string(),
            ),
        ));
    }

    let key = match parser.eat_identifier() {
        Some(k) => k,
        None => {
            return Err(crate::Error::deser(
                crate::connection::ConnectionError::SyntaxError(
                    "Missing property name in SHOW statement".to_string(),
                ),
            ));
        }
    };

    let _ = parser.eat_token(b';');
    parser.skip_whitespace_and_comments();
    if parser.pos < parser.sql.len() {
        return Err(crate::Error::deser(
            crate::connection::ConnectionError::SyntaxError(
                "Extra tokens after SHOW statement".to_string(),
            ),
        ));
    }

    Ok(ClientSideCommand::Show { key })
}

pub(crate) async fn execute(
    conn: &mut Connection,
    key: String,
) -> Result<ExecutionResult, crate::Error> {
    let value_str = conn.state.get(&key).unwrap_or_else(|| "null".to_string());

    let key_lower = key.to_ascii_lowercase();
    let (type_code, value) = if key_lower.contains('.') {
        (TypeCode::String, value_str.to_value())
    } else if let Some(prop) = conn.state.get_property(&key_lower) {
        (
            prop.type_code(),
            prop.to_value(&value_str, conn.state.dialect()),
        )
    } else {
        (TypeCode::String, value_str.to_value())
    };

    let proto_metadata = v1::ResultSetMetadata {
        row_type: Some(v1::StructType {
            fields: vec![v1::struct_type::Field {
                name: key.to_ascii_lowercase(),
                r#type: Some(v1::Type {
                    code: i32::from(type_code),
                    ..Default::default()
                }),
            }],
        }),
        ..Default::default()
    };
    let metadata = ResultSetMetadata::new(Some(proto_metadata));

    let row = Row {
        values: vec![value],
        metadata: metadata.clone(),
    };

    let rs = ResultSet::new_local(conn.client.clone(), metadata, vec![row]);

    Ok(ExecutionResult::QueryResult(Box::new(rs)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::Dialect;
    use crate::connection::parser::SimpleParser;

    #[test]
    fn test_parse_show() {
        // GoogleSQL variant
        let mut parser = SimpleParser::new("VARIABLE autocommit;", Dialect::GoogleSql);
        let cmd = parse(&mut parser, Dialect::GoogleSql).unwrap();
        assert!(matches!(cmd, ClientSideCommand::Show { ref key } if key == "autocommit"));

        // GoogleSQL error if missing VARIABLE
        let mut parser = SimpleParser::new("autocommit;", Dialect::GoogleSql);
        let cmd = parse(&mut parser, Dialect::GoogleSql);
        assert!(cmd.is_err());

        // PG variant (doesn't need VARIABLE)
        let mut parser = SimpleParser::new("autocommit;", Dialect::PostgreSql);
        let cmd = parse(&mut parser, Dialect::PostgreSql).unwrap();
        assert!(matches!(cmd, ClientSideCommand::Show { ref key } if key == "autocommit"));
    }
}
