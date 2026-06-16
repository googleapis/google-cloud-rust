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

pub(crate) fn validate_savepoint_name(name: &str) -> Result<(), crate::Error> {
    if name.is_empty() {
        return Err(crate::Error::deser("Savepoint name cannot be empty"));
    }
    if name.len() > 128 {
        return Err(crate::Error::deser(
            "Savepoint name cannot exceed 128 characters",
        ));
    }
    let first_char = name.chars().next().unwrap();
    if !first_char.is_ascii_alphabetic() && first_char != '_' {
        return Err(crate::Error::deser(
            "Savepoint name must start with an alphabetic character or underscore",
        ));
    }
    Ok(())
}

pub(crate) fn parse_savepoint(
    parser: &mut SimpleParser<'_>,
) -> Result<ClientSideCommand, crate::Error> {
    let name = parser.eat_identifier().ok_or_else(|| {
        crate::Error::deser(crate::connection::ConnectionError::SyntaxError(
            "Missing savepoint name".to_string(),
        ))
    })?;
    validate_savepoint_name(&name)?;
    let _ = parser.eat_token(b';');
    Ok(ClientSideCommand::Savepoint { name })
}

pub(crate) fn parse_release(
    parser: &mut SimpleParser<'_>,
) -> Result<ClientSideCommand, crate::Error> {
    let _ = parser.eat_keyword("SAVEPOINT");
    let name = parser.eat_identifier().ok_or_else(|| {
        crate::Error::deser(crate::connection::ConnectionError::SyntaxError(
            "Missing savepoint name".to_string(),
        ))
    })?;
    validate_savepoint_name(&name)?;
    let _ = parser.eat_token(b';');
    Ok(ClientSideCommand::ReleaseSavepoint { name })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::Dialect;
    use crate::connection::parser::SimpleParser;

    #[test]
    fn test_parse_savepoint() {
        let mut parser = SimpleParser::new("s1;", Dialect::GoogleSql);
        let cmd = parse_savepoint(&mut parser).unwrap();
        assert!(matches!(cmd, ClientSideCommand::Savepoint { name } if name == "s1"));

        // comments are ok
        let mut parser = SimpleParser::new(" /* comment */ s_2;", Dialect::GoogleSql);
        let cmd = parse_savepoint(&mut parser).unwrap();
        assert!(matches!(cmd, ClientSideCommand::Savepoint { name } if name == "s_2"));

        // invalid savepoint name format
        let mut parser = SimpleParser::new("1s;", Dialect::GoogleSql);
        let cmd = parse_savepoint(&mut parser);
        assert!(cmd.is_err());
    }

    #[test]
    fn test_parse_release() {
        let mut parser = SimpleParser::new("SAVEPOINT s1;", Dialect::GoogleSql);
        let cmd = parse_release(&mut parser).unwrap();
        assert!(matches!(cmd, ClientSideCommand::ReleaseSavepoint { name } if name == "s1"));

        // omit keyword SAVEPOINT
        let mut parser = SimpleParser::new("s2;", Dialect::GoogleSql);
        let cmd = parse_release(&mut parser).unwrap();
        assert!(matches!(cmd, ClientSideCommand::ReleaseSavepoint { name } if name == "s2"));
    }
}
