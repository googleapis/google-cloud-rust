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

/// Parses a string representation of a boolean value in a SET/option context.
pub fn parse_boolean_literal_str(value: &str, dialect: Dialect) -> Option<bool> {
    let val_lower = value.trim().to_ascii_lowercase();
    match dialect {
        Dialect::GoogleSql => match val_lower.as_str() {
            "true" | "on" | "1" | "yes" | "t" => Some(true),
            "false" | "off" | "0" | "no" | "f" => Some(false),
            _ => None,
        },
        Dialect::PostgreSql => match val_lower.as_str() {
            "t" | "tr" | "tru" | "true" | "y" | "ye" | "yes" | "on" | "1" => Some(true),
            "f" | "fa" | "fal" | "fals" | "false" | "n" | "no" | "of" | "off" | "0" => Some(false),
            _ => None,
        },
    }
}

/// A simple, token-based, regex-free SQL parser helper.
pub struct SimpleParser<'a> {
    pub(crate) sql: &'a [u8],
    pub(crate) pos: usize,
    pub(crate) dialect: Dialect,
}

impl<'a> SimpleParser<'a> {
    /// Construct a new SimpleParser for the SQL string and database dialect.
    pub fn new(sql: &'a str, dialect: Dialect) -> Self {
        Self {
            sql: sql.as_bytes(),
            pos: 0,
            dialect,
        }
    }

    /// Returns the current parsing position.
    pub fn pos(&self) -> usize {
        self.pos
    }

    /// Sets the parsing position.
    pub fn set_pos(&mut self, pos: usize) {
        self.pos = pos;
    }

    /// Returns the raw SQL bytes.
    pub fn sql(&self) -> &'a [u8] {
        self.sql
    }

    /// Returns the remaining slice of SQL string.
    pub fn remaining_sql(&self) -> &str {
        std::str::from_utf8(&self.sql[self.pos..]).unwrap_or("")
    }

    /// Peek if the next character matches the given byte token.
    pub fn peek_token(&self, token: u8) -> bool {
        self.pos < self.sql.len() && self.sql[self.pos] == token
    }

    /// Read the next keyword character group, skipping whitespace and comments.
    pub fn read_keyword(&mut self) -> Option<String> {
        self.skip_whitespace_and_comments();
        let start = self.pos;
        while self.pos < self.sql.len() {
            let c = self.sql[self.pos];
            if c.is_ascii_alphanumeric() || c == b'_' {
                self.pos += 1;
            } else {
                break;
            }
        }
        if self.pos > start {
            Some(String::from_utf8_lossy(&self.sql[start..self.pos]).to_string())
        } else {
            None
        }
    }

    /// Check if the next keyword matches the expected string (case-insensitive).
    pub fn eat_keyword(&mut self, keyword: &str) -> bool {
        self.skip_whitespace_and_comments();
        let kw_len = keyword.len();
        if self.pos + kw_len > self.sql.len() {
            return false;
        }
        let next_slice = &self.sql[self.pos..self.pos + kw_len];
        if next_slice.eq_ignore_ascii_case(keyword.as_bytes()) {
            if self.pos + kw_len < self.sql.len() {
                let next_char = self.sql[self.pos + kw_len];
                if is_identifier_char(next_char) || next_char == b'.' {
                    return false;
                }
            }
            self.pos += kw_len;
            return true;
        }
        false
    }

    /// Check if the next token is the expected single byte character, and advances if matched.
    pub fn eat_token(&mut self, token: u8) -> bool {
        self.skip_whitespace_and_comments();
        if self.pos < self.sql.len() && self.sql[self.pos] == token {
            self.pos += 1;
            return true;
        }
        false
    }

    /// Parse an identifier, which can be a single segment or a dot-separated path of multiple segments.
    pub fn eat_identifier(&mut self) -> Option<String> {
        let mut path = self.eat_identifier_part()?;
        loop {
            let pos_before_peek = self.pos;
            self.skip_whitespace_and_comments();
            if self.peek_token(b'.') {
                self.eat_token(b'.');
                if let Some(next_part) = self.eat_identifier_part() {
                    path = format!("{}.{}", path, next_part);
                } else {
                    self.pos = pos_before_peek;
                    break;
                }
            } else {
                self.pos = pos_before_peek;
                break;
            }
        }
        Some(path)
    }

    /// Parse a single identifier part, handling quoted strings and returning unquoted string.
    pub fn eat_identifier_part(&mut self) -> Option<String> {
        self.skip_whitespace_and_comments();
        if self.pos >= self.sql.len() {
            return None;
        }
        let quote = self.sql[self.pos];
        if self.dialect == Dialect::PostgreSql && quote == b'"' {
            self.eat_pg_quoted_identifier()
        } else if self.dialect == Dialect::GoogleSql && quote == b'`' {
            self.eat_google_quoted_identifier()
        } else {
            self.eat_unquoted_identifier()
        }
    }

    fn eat_pg_quoted_identifier(&mut self) -> Option<String> {
        self.pos += 1;
        let mut part = String::new();
        while self.pos < self.sql.len() {
            let c = self.sql[self.pos];
            if c == b'"' {
                if self.pos + 1 < self.sql.len() && self.sql[self.pos + 1] == b'"' {
                    part.push('"');
                    self.pos += 2;
                } else {
                    self.pos += 1;
                    return Some(truncate_to_63_bytes(part));
                }
            } else {
                part.push(c as char);
                self.pos += 1;
            }
        }
        None
    }

    fn eat_google_quoted_identifier(&mut self) -> Option<String> {
        self.pos += 1;
        let mut part = String::new();
        while self.pos < self.sql.len() {
            let c = self.sql[self.pos];
            if c == b'`' {
                self.pos += 1;
                return Some(part);
            } else if c == b'\\' {
                if self.pos + 1 < self.sql.len() {
                    let next_char = self.sql[self.pos + 1] as char;
                    part.push(next_char);
                    self.pos += 2;
                } else {
                    part.push('\\');
                    self.pos += 1;
                }
            } else {
                part.push(c as char);
                self.pos += 1;
            }
        }
        None
    }

    fn eat_unquoted_identifier(&mut self) -> Option<String> {
        let start = self.pos;
        if !self.sql[start].is_ascii_alphabetic() && self.sql[start] != b'_' {
            return None;
        }
        while self.pos < self.sql.len() {
            let c = self.sql[self.pos];
            if c.is_ascii_alphanumeric() || c == b'_' {
                self.pos += 1;
            } else {
                break;
            }
        }
        let end = self.pos;
        let s = std::str::from_utf8(&self.sql[start..end])
            .unwrap()
            .to_lowercase();
        if self.dialect == Dialect::PostgreSql {
            Some(truncate_to_63_bytes(s))
        } else {
            Some(s)
        }
    }

    /// Parse a boolean literal value.
    ///
    /// If `is_set_statement` is true, allows PostgreSQL-specific unquoted boolean literals
    /// such as `t`, `y`, `yes`, `on`, `1`, `f`, `n`, `no`, `off`, `0`.
    /// Otherwise, only allows the standard SQL keywords `true` and `false`.
    pub fn eat_boolean_literal(&mut self, is_set_statement: bool) -> Option<bool> {
        let start = self.pos;
        let word = match self.read_keyword() {
            Some(w) => w,
            None => return None,
        };

        if is_set_statement {
            if let Some(b) = parse_boolean_literal_str(&word, self.dialect) {
                return Some(b);
            }
        } else {
            let word_lower = word.to_lowercase();
            match word_lower.as_str() {
                "true" => return Some(true),
                "false" => return Some(false),
                _ => {}
            }
        }

        self.pos = start;
        None
    }
    /// Parse a string literal value or unquoted scalar value.
    pub fn eat_literal(&mut self) -> Result<String, crate::Error> {
        self.skip_whitespace_and_comments();
        if self.pos >= self.sql.len() {
            return Err(crate::Error::deser(
                crate::connection::ConnectionError::SyntaxError(format!(
                    "Missing literal value at position {}",
                    self.pos
                )),
            ));
        }

        let c = self.sql[self.pos];
        let is_quoted = c == b'\''
            || c == b'"'
            || (self.dialect == Dialect::PostgreSql
                && (c == b'E' || c == b'e')
                && self.pos + 1 < self.sql.len()
                && self.sql[self.pos + 1] == b'\'');

        if is_quoted {
            self.eat_string_literal()
        } else {
            self.eat_unquoted_literal()
        }
    }

    /// Parse a typed literal value (String, Bool, Null, or Number).
    pub fn eat_literal_value(&mut self) -> Result<crate::value::Value, crate::Error> {
        self.skip_whitespace_and_comments();
        if self.pos >= self.sql.len() {
            return Err(crate::Error::deser(
                crate::connection::ConnectionError::SyntaxError(
                    "Unexpected end of input while parsing literal value".to_string(),
                ),
            ));
        }

        let c = self.sql[self.pos];
        let is_string = c == b'\''
            || (self.dialect == Dialect::GoogleSql && c == b'"')
            || (self.dialect == Dialect::PostgreSql
                && (c == b'E' || c == b'e')
                && self.pos + 1 < self.sql.len()
                && self.sql[self.pos + 1] == b'\'');

        if is_string {
            use crate::to_value::ToValue;
            let s = self.eat_string_literal()?;
            return Ok(s.to_value());
        }

        // Try parsing a boolean literal
        if let Some(bool_val) = self.eat_boolean_literal(false) {
            use crate::to_value::ToValue;
            return Ok(bool_val.to_value());
        }

        // Check for NULL keyword
        let start = self.pos;
        if let Some(word) = self.read_keyword() {
            if word.to_uppercase() == "NULL" {
                return Ok(crate::value::Value(prost_types::Value {
                    kind: Some(prost_types::value::Kind::NullValue(0)),
                }));
            }
            self.pos = start; // back up if not NULL
        }

        // Try parsing a number
        self.eat_numeric_literal_value()
    }

    /// Parses and unescapes a string literal according to dialect rules.
    pub fn eat_string_literal(&mut self) -> Result<String, crate::Error> {
        self.skip_whitespace_and_comments();
        if self.pos >= self.sql.len() {
            return Err(crate::Error::deser(
                crate::connection::ConnectionError::SyntaxError(
                    "Missing string literal".to_string(),
                ),
            ));
        }

        let mut is_pg_escape = false;
        let start_pos = self.pos;
        if self.dialect == Dialect::PostgreSql {
            let c = self.sql[self.pos];
            if (c == b'E' || c == b'e')
                && self.pos + 1 < self.sql.len()
                && self.sql[self.pos + 1] == b'\''
            {
                is_pg_escape = true;
                self.pos += 1; // consume E/e
            }
        }

        let quote = self.sql[self.pos];
        if quote != b'\'' && (self.dialect == Dialect::PostgreSql || quote != b'"') {
            self.pos = start_pos;
            return Err(crate::Error::deser(
                crate::connection::ConnectionError::SyntaxError(
                    "Expected string literal".to_string(),
                ),
            ));
        }
        self.pos += 1; // consume opening quote

        let mut s = Vec::new();
        while self.pos < self.sql.len() {
            let ch = self.sql[self.pos];
            if ch == quote {
                // Check for double quote to escape (e.g. '' -> ')
                if self.pos + 1 < self.sql.len() && self.sql[self.pos + 1] == quote {
                    s.push(quote);
                    self.pos += 2;
                } else {
                    self.pos += 1; // consume closing quote
                    break;
                }
            } else if ch == b'\\' {
                // Escape handling:
                // GoogleSQL: always processes escapes.
                // PostgreSQL: only processes escapes if E'...' prefix is used.
                let process_escape = self.dialect == Dialect::GoogleSql || is_pg_escape;
                if process_escape {
                    if self.pos + 1 < self.sql.len() {
                        let next_ch = self.sql[self.pos + 1];
                        let escaped = match next_ch {
                            b'n' => b'\n',
                            b't' => b'\t',
                            b'r' => b'\r',
                            b'b' => 0x08,
                            b'f' => 0x0C,
                            other => other,
                        };
                        s.push(escaped);
                        self.pos += 2;
                    } else {
                        s.push(b'\\');
                        self.pos += 1;
                    }
                } else {
                    s.push(b'\\');
                    self.pos += 1;
                }
            } else {
                s.push(ch);
                self.pos += 1;
            }
        }

        let string_val = String::from_utf8(s)
            .map_err(|e| crate::Error::deser(format!("Invalid UTF-8 in string literal: {}", e)))?;
        Ok(string_val)
    }

    fn eat_numeric_literal_value(&mut self) -> Result<crate::value::Value, crate::Error> {
        let start = self.pos;
        if self.pos < self.sql.len() && (self.sql[self.pos] == b'-' || self.sql[self.pos] == b'+') {
            self.pos += 1;
        }
        let mut has_digits = false;
        let mut has_decimal = false;
        while self.pos < self.sql.len() {
            let ch = self.sql[self.pos];
            if ch.is_ascii_digit() {
                has_digits = true;
                self.pos += 1;
            } else if ch == b'.' {
                has_decimal = true;
                self.pos += 1;
            } else {
                break;
            }
        }

        // Support exponent (e or E)
        if has_digits
            && self.pos < self.sql.len()
            && (self.sql[self.pos] == b'e' || self.sql[self.pos] == b'E')
        {
            has_decimal = true; // exponents force float type
            self.pos += 1;
            if self.pos < self.sql.len()
                && (self.sql[self.pos] == b'-' || self.sql[self.pos] == b'+')
            {
                self.pos += 1;
            }
            while self.pos < self.sql.len() && self.sql[self.pos].is_ascii_digit() {
                self.pos += 1;
            }
        }

        if has_digits {
            let num_str = String::from_utf8_lossy(&self.sql[start..self.pos]);
            if has_decimal {
                if let Ok(f) = num_str.parse::<f64>() {
                    use crate::to_value::ToValue;
                    return Ok(f.to_value());
                }
            } else if let Ok(i) = num_str.parse::<i64>() {
                use crate::to_value::ToValue;
                return Ok(i.to_value());
            }
        }

        Err(crate::Error::deser(
            crate::connection::ConnectionError::SyntaxError(format!(
                "Unsupported or invalid literal parameter value at pos {}",
                start
            )),
        ))
    }

    fn eat_unquoted_literal(&mut self) -> Result<String, crate::Error> {
        let start = self.pos;
        while self.pos < self.sql.len() {
            let c = self.sql[self.pos];
            if c.is_ascii_whitespace() || c == b';' || c == b',' || c == b')' {
                break;
            }
            // Check if starting a comment block
            if self.pos + 1 < self.sql.len()
                && self.sql[self.pos] == b'/'
                && self.sql[self.pos + 1] == b'*'
            {
                break;
            }
            if self.pos + 1 < self.sql.len()
                && self.sql[self.pos] == b'-'
                && self.sql[self.pos + 1] == b'-'
            {
                break;
            }
            if self.dialect == Dialect::GoogleSql && self.sql[self.pos] == b'#' {
                break;
            }
            self.pos += 1;
        }
        if self.pos == start {
            return Err(crate::Error::deser(
                crate::connection::ConnectionError::SyntaxError(format!(
                    "Missing literal value at position {}",
                    self.pos
                )),
            ));
        }
        let val = String::from_utf8_lossy(&self.sql[start..self.pos]).to_string();
        Ok(val)
    }

    /// Skips any leading whitespace, line comments, or block comments.
    pub fn skip_whitespace_and_comments(&mut self) {
        while self.pos < self.sql.len() {
            if self.sql[self.pos].is_ascii_whitespace() {
                self.pos += 1;
            } else if self.pos + 1 < self.sql.len()
                && self.sql[self.pos] == b'-'
                && self.sql[self.pos + 1] == b'-'
            {
                self.pos += 2;
                while self.pos < self.sql.len() && self.sql[self.pos] != b'\n' {
                    self.pos += 1;
                }
            } else if self.dialect == Dialect::GoogleSql && self.sql[self.pos] == b'#' {
                self.pos += 1;
                while self.pos < self.sql.len() && self.sql[self.pos] != b'\n' {
                    self.pos += 1;
                }
            } else if self.pos + 1 < self.sql.len()
                && self.sql[self.pos] == b'/'
                && self.sql[self.pos + 1] == b'*'
            {
                self.pos += 2;
                let mut nesting = 1;
                while self.pos + 1 < self.sql.len() && nesting > 0 {
                    if self.sql[self.pos] == b'/' && self.sql[self.pos + 1] == b'*' {
                        nesting += 1;
                        self.pos += 2;
                    } else if self.sql[self.pos] == b'*' && self.sql[self.pos + 1] == b'/' {
                        nesting -= 1;
                        self.pos += 2;
                    } else {
                        self.pos += 1;
                    }
                }
            } else {
                break;
            }
        }
    }

    /// Advances the parser position beyond the closing quote boundary of a string literal.
    pub fn skip_quoted_string(&mut self, quote_char: u8) {
        self.pos += 1;
        while self.pos < self.sql.len() && self.sql[self.pos] != quote_char {
            self.pos += 1;
        }
        if self.pos < self.sql.len() {
            self.pos += 1;
        }
    }
}

fn is_identifier_char(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_' || c >= 0x80
}

fn truncate_to_63_bytes(s: String) -> String {
    if s.len() <= 63 {
        return s;
    }
    let mut end = 63;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    s[..end].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parser_internal_methods() {
        // Test eat_identifier_part (GoogleSQL)
        {
            let mut parser =
                SimpleParser::new("  /* comment */  foo_bar123   ", Dialect::GoogleSql);
            let result = parser.eat_identifier_part();
            assert_eq!(
                result,
                Some("foo_bar123".to_string()),
                "should parse unquoted identifier and lowercase it in GoogleSQL"
            );
            assert_eq!(
                parser.pos(),
                26,
                "should advance position past the identifier in GoogleSQL"
            );
        }

        {
            // GoogleSQL: double quotes is NOT a quoted identifier (it starts a string literal, so eat_identifier_part fails)
            let mut parser = SimpleParser::new(r#"  "My""Table"  "#, Dialect::GoogleSql);
            let result = parser.eat_identifier_part();
            assert_eq!(
                result, None,
                "double quotes are not valid identifier quotes in GoogleSQL"
            );
        }

        {
            // GoogleSQL: backticks are quoted identifiers
            let mut parser = SimpleParser::new(r#"  `MyTable`  "#, Dialect::GoogleSql);
            let result = parser.eat_identifier_part();
            assert_eq!(
                result,
                Some("MyTable".to_string()),
                "should parse backtick quoted identifier and preserve case in GoogleSQL"
            );
        }

        {
            // GoogleSQL: backtick with escape sequence inside
            let mut parser = SimpleParser::new(r#"`My\`Table` remaining"#, Dialect::GoogleSql);
            let result = parser.eat_identifier_part();
            assert_eq!(
                result,
                Some("My`Table".to_string()),
                "should parse backtick identifier and handle backslash escape"
            );
            assert_eq!(
                parser.remaining_sql(),
                " remaining",
                "should advance past closing backtick"
            );
        }

        {
            // GoogleSQL: unterminated backtick identifier
            let mut parser = SimpleParser::new(r#"`unterminated"#, Dialect::GoogleSql);
            let result = parser.eat_identifier_part();
            assert_eq!(
                result, None,
                "unterminated backtick identifier should return None"
            );
        }

        // Test eat_identifier_part (PostgreSQL)
        {
            let mut parser =
                SimpleParser::new("  /* comment */  foo_bar123   ", Dialect::PostgreSql);
            let result = parser.eat_identifier_part();
            assert_eq!(
                result,
                Some("foo_bar123".to_string()),
                "should parse unquoted identifier and lowercase it in PostgreSQL"
            );
        }

        {
            // PostgreSQL: backticks are NOT valid identifier quotes
            let mut parser = SimpleParser::new(r#"`MyTable`"#, Dialect::PostgreSql);
            let result = parser.eat_identifier_part();
            assert_eq!(
                result, None,
                "backticks are not valid identifier quotes in PostgreSQL"
            );
        }

        {
            // PostgreSQL: double quotes are valid identifier quotes
            let mut parser = SimpleParser::new(r#"  "My""Table"  "#, Dialect::PostgreSql);
            let result = parser.eat_identifier_part();
            assert_eq!(
                result,
                Some(r#"My"Table"#.to_string()),
                "should parse double quoted identifier and handle double-quote escapes in PostgreSQL"
            );
        }

        {
            // PostgreSQL: double quotes does NOT support backslash escapes (backslash is literal)
            let mut parser = SimpleParser::new(r#""My\Table""#, Dialect::PostgreSql);
            let result = parser.eat_identifier_part();
            assert_eq!(
                result,
                Some(r#"My\Table"#.to_string()),
                "backslash should be treated literally in PostgreSQL quoted identifiers"
            );
        }

        {
            // PostgreSQL truncation: 63 bytes max for unquoted identifier
            let long_name = "a".repeat(100);
            let mut parser = SimpleParser::new(&long_name, Dialect::PostgreSql);
            let result = parser.eat_identifier_part();
            assert_eq!(
                result,
                Some("a".repeat(63)),
                "unquoted identifier in PostgreSQL should be truncated to 63 characters"
            );
        }

        {
            // PostgreSQL truncation: 63 bytes max for quoted identifier
            let long_name = format!("\"{}\"", "a".repeat(100));
            let mut parser = SimpleParser::new(&long_name, Dialect::PostgreSql);
            let result = parser.eat_identifier_part();
            assert_eq!(
                result,
                Some("a".repeat(63)),
                "quoted identifier in PostgreSQL should be truncated to 63 characters"
            );
        }

        {
            // PostgreSQL truncation: 63 bytes max, UTF-8 char boundary safe (e.g. 'ä' is 2 bytes: c3 a4)
            // 31 'ä's = 62 bytes. 32 'ä's = 64 bytes. Truncating at 63 bytes should result in 62 bytes (31 'ä's).
            let long_name = format!("\"{}\"", "ä".repeat(35));
            let mut parser = SimpleParser::new(&long_name, Dialect::PostgreSql);
            let result = parser.eat_identifier_part();
            assert_eq!(
                result,
                Some("ä".repeat(31)),
                "should truncate PG identifier to 62 bytes to avoid splitting multi-byte UTF-8 characters"
            );
        }

        // Test eat_identifier (unified path parser)
        {
            // GoogleSQL table name with backticks (2 parts)
            let mut parser = SimpleParser::new("`schema_name`.`table_name`", Dialect::GoogleSql);
            let result = parser.eat_identifier();
            assert_eq!(
                result,
                Some("schema_name.table_name".to_string()),
                "should parse schema and table name in GoogleSQL"
            );
        }

        {
            // GoogleSQL table name with backticks (3 parts)
            let mut parser = SimpleParser::new(
                "`catalog_name`.`schema_name`.`table_name`",
                Dialect::GoogleSql,
            );
            let result = parser.eat_identifier();
            assert_eq!(
                result,
                Some("catalog_name.schema_name.table_name".to_string()),
                "should parse catalog, schema, and table name in GoogleSQL"
            );
        }

        {
            // PostgreSQL table name with double quotes
            let mut parser = SimpleParser::new(r#""SchemaName"."TableName""#, Dialect::PostgreSql);
            let result = parser.eat_identifier();
            assert_eq!(
                result,
                Some("SchemaName.TableName".to_string()),
                "should parse schema and table name in PostgreSQL"
            );
        }

        {
            // Table name with spacing/comments around dot (GoogleSQL)
            let mut parser = SimpleParser::new(
                "schema_name  /*comment*/  .  `TableName`",
                Dialect::GoogleSql,
            );
            let result = parser.eat_identifier();
            assert_eq!(
                result,
                Some("schema_name.TableName".to_string()),
                "should parse schema and table name with spacing/comments in GoogleSQL"
            );
        }

        {
            // Schema with trailing dot but no table name (invalid second part) (GoogleSQL)
            let mut parser = SimpleParser::new("schema_name.   ", Dialect::GoogleSql);
            let result = parser.eat_identifier();
            assert_eq!(
                result,
                Some("schema_name".to_string()),
                "should fallback to schema name if second part is missing"
            );
            assert_eq!(
                parser.pos(),
                11,
                "should restore position to before the dot peek (after schema_name)"
            );
        }

        {
            // Single segment identifier (GoogleSQL)
            let mut parser = SimpleParser::new("  `my_identifier`  ", Dialect::GoogleSql);
            let result = parser.eat_identifier();
            assert_eq!(
                result,
                Some("my_identifier".to_string()),
                "should parse a single segment quoted identifier"
            );
        }

        // Test skip_quoted_string
        {
            let mut parser = SimpleParser::new("'string literal' remaining", Dialect::GoogleSql);
            parser.skip_quoted_string(b'\'');
            assert_eq!(
                parser.remaining_sql(),
                " remaining",
                "should skip quoted string and advance past closing quote"
            );
        }

        {
            let mut parser = SimpleParser::new("\"another string\" rest", Dialect::GoogleSql);
            parser.skip_quoted_string(b'"');
            assert_eq!(
                parser.remaining_sql(),
                " rest",
                "should skip double quoted string and advance past closing quote"
            );
        }

        {
            // Unterminated quoted string skip
            let mut parser = SimpleParser::new("'unterminated", Dialect::GoogleSql);
            parser.skip_quoted_string(b'\'');
            assert_eq!(
                parser.remaining_sql(),
                "",
                "should skip to end of input if quote is unterminated"
            );
        }

        // Test eat_boolean_literal
        {
            // Standard query context: only true/false are boolean keywords
            let mut parser = SimpleParser::new("true", Dialect::PostgreSql);
            assert_eq!(parser.eat_boolean_literal(false), Some(true));

            let mut parser = SimpleParser::new("false", Dialect::PostgreSql);
            assert_eq!(parser.eat_boolean_literal(false), Some(false));

            let mut parser = SimpleParser::new("on", Dialect::PostgreSql);
            assert_eq!(parser.eat_boolean_literal(false), None);

            let mut parser = SimpleParser::new("1", Dialect::PostgreSql);
            assert_eq!(parser.eat_boolean_literal(false), None);

            // SET context (PostgreSQL): allows additional unquoted literals
            let mut parser = SimpleParser::new("on", Dialect::PostgreSql);
            assert_eq!(parser.eat_boolean_literal(true), Some(true));

            let mut parser = SimpleParser::new("off", Dialect::PostgreSql);
            assert_eq!(parser.eat_boolean_literal(true), Some(false));

            let mut parser = SimpleParser::new("yes", Dialect::PostgreSql);
            assert_eq!(parser.eat_boolean_literal(true), Some(true));

            let mut parser = SimpleParser::new("no", Dialect::PostgreSql);
            assert_eq!(parser.eat_boolean_literal(true), Some(false));

            let mut parser = SimpleParser::new("1", Dialect::PostgreSql);
            assert_eq!(parser.eat_boolean_literal(true), Some(true));

            let mut parser = SimpleParser::new("0", Dialect::PostgreSql);
            assert_eq!(parser.eat_boolean_literal(true), Some(false));

            let mut parser = SimpleParser::new("t", Dialect::PostgreSql);
            assert_eq!(parser.eat_boolean_literal(true), Some(true));

            let mut parser = SimpleParser::new("f", Dialect::PostgreSql);
            assert_eq!(parser.eat_boolean_literal(true), Some(false));

            // SET context (GoogleSQL): allows fewer PostgreSQL-specific literals, but does have custom ones
            let mut parser = SimpleParser::new("on", Dialect::GoogleSql);
            assert_eq!(parser.eat_boolean_literal(true), Some(true));

            let mut parser = SimpleParser::new("off", Dialect::GoogleSql);
            assert_eq!(parser.eat_boolean_literal(true), Some(false));

            let mut parser = SimpleParser::new("yes", Dialect::GoogleSql);
            assert_eq!(parser.eat_boolean_literal(true), Some(true));

            let mut parser = SimpleParser::new("1", Dialect::GoogleSql);
            assert_eq!(parser.eat_boolean_literal(true), Some(true));

            let mut parser = SimpleParser::new("0", Dialect::GoogleSql);
            assert_eq!(parser.eat_boolean_literal(true), Some(false));

            let mut parser = SimpleParser::new("t", Dialect::GoogleSql);
            assert_eq!(parser.eat_boolean_literal(true), Some(true));

            let mut parser = SimpleParser::new("f", Dialect::GoogleSql);
            assert_eq!(parser.eat_boolean_literal(true), Some(false));
        }

        // Test eat_string_literal and eat_literal_value
        {
            use crate::to_value::ToValue;

            // GoogleSQL standard string escape tests
            let mut parser = SimpleParser::new("'hello\\nworld'", Dialect::GoogleSql);
            assert_eq!(parser.eat_string_literal().unwrap(), "hello\nworld");

            let mut parser = SimpleParser::new("\"double\\tquoted\"", Dialect::GoogleSql);
            assert_eq!(parser.eat_string_literal().unwrap(), "double\tquoted");

            // GoogleSQL double-quote escape check ('' -> ')
            let mut parser = SimpleParser::new("'hello''world'", Dialect::GoogleSql);
            assert_eq!(parser.eat_string_literal().unwrap(), "hello'world");

            // PostgreSQL standard string (backslashes are literal unless prefixed with E)
            let mut parser = SimpleParser::new("'hello\\nworld'", Dialect::PostgreSql);
            assert_eq!(parser.eat_string_literal().unwrap(), "hello\\nworld");

            // PostgreSQL escape string (E'...')
            let mut parser = SimpleParser::new("E'hello\\nworld'", Dialect::PostgreSql);
            assert_eq!(parser.eat_string_literal().unwrap(), "hello\nworld");

            let mut parser = SimpleParser::new("e'hello\\tworld'", Dialect::PostgreSql);
            assert_eq!(parser.eat_string_literal().unwrap(), "hello\tworld");

            // eat_literal_value tests
            // Exponent numbers
            let mut parser = SimpleParser::new("1.2e3", Dialect::GoogleSql);
            assert_eq!(parser.eat_literal_value().unwrap(), 1200.0.to_value());

            let mut parser = SimpleParser::new("3e-5", Dialect::PostgreSql);
            assert_eq!(parser.eat_literal_value().unwrap(), 0.00003.to_value());

            let mut parser = SimpleParser::new("-4.5E+2", Dialect::GoogleSql);
            assert_eq!(parser.eat_literal_value().unwrap(), (-450.0).to_value());

            // Integers
            let mut parser = SimpleParser::new("42", Dialect::GoogleSql);
            assert_eq!(parser.eat_literal_value().unwrap(), 42.to_value());

            // NULL keyword
            let mut parser = SimpleParser::new("NULL", Dialect::GoogleSql);
            let null_val = parser.eat_literal_value().unwrap();
            assert_eq!(null_val.kind(), crate::value::Kind::Null);

            // Booleans
            let mut parser = SimpleParser::new("true", Dialect::GoogleSql);
            assert_eq!(parser.eat_literal_value().unwrap(), true.to_value());

            // Quoted strings inside eat_literal_value
            let mut parser = SimpleParser::new("'some string'", Dialect::GoogleSql);
            assert_eq!(
                parser.eat_literal_value().unwrap(),
                "some string".to_value()
            );
        }
    }
}
