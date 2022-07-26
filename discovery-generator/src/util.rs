// Copyright 2022 Google LLC
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

//! A grab bag of helper functions

use anyhow::Result;
use regex::Regex;
use std::fmt::Write as FmtWrite;

const MAX_COMMENT_LINE_LENGTH: usize = 70;

// Converts snake_case to PascalCase.
pub fn snake_to_pascal(s: &str) -> String {
    let mut value = String::new();
    let mut seen_underscore = true;
    for c in s.chars() {
        if c.eq(&'_') {
            seen_underscore = true;
            continue;
        }
        if seen_underscore {
            value.push_str(&c.to_uppercase().to_string());
            seen_underscore = false;
            continue;
        }
        value.push_str(c.to_string().as_str())
    }
    value
}

// Converts camelCase to snake_case.
pub fn camel_to_snake(s: &str) -> String {
    let mut value = String::new();
    for c in s.chars() {
        if c.is_ascii_uppercase() {
            value.push('_')
        }
        value.push_str(&c.to_string().to_lowercase());
    }
    value
}

/// Uppercase the first character.
pub fn to_title_case(s: &mut String) -> String {
    if let Some(r) = s.get_mut(0..1) {
        r.make_ascii_uppercase();
    }
    s.to_string()
}

/// Remove some invalid character for method names.
pub fn safe_method_name(s: &str) -> String {
    s.to_owned().replace(".", "_")
}

/// Maps a basic discovery type to Rust type.
pub fn basic_struct_type(disco_type: &str) -> String {
    let base = match disco_type {
        "string" => "String",
        "integer" => "i64",
        "boolean" => "bool",
        _ => panic!("unknown type: {}", disco_type),
    };
    base.into()
}

/// Determine if the string is a keyword. See https://doc.rust-lang.org/reference/keywords.html
pub fn is_keyword(s: &str) -> bool {
    let keywords = vec![
        "as", "break", "const", "continue", "crate", "else", "enum", "extern", "false", "fn",
        "for", "if", "impl", "in", "let", "loop", "match", "mod", "move", "mut", "pub", "ref",
        "return", "self", "Self", "static", "struct", "super", "trait", "true", "type", "unsafe",
        "use", "where", "while", "async", "await", "dyn", "abstract", "become", "box", "do",
        "final", "macro", "override", "priv", "typeof", "unsized", "virtual", "yield", "try",
    ];
    keywords.contains(&s)
}

/// Turns a discovery doc description into a rustdoc comment.
pub fn as_comment(prefix: &str, mut comment: String, add_padding: bool) -> Result<String> {
    if comment.is_empty() {
        return Ok(String::new());
    }
    let mut buf = String::new();
    let mut padding = String::new();
    let mut line_length: usize = 70;
    let mut line_num: usize = 0;
    let re_url = Regex::new(r"^\(?http\S+$")?;

    //TODO just work with chars?
    while comment.chars().count() > 0 {
        if add_padding && line_num == 1 {
            padding.push_str("  ");
            line_length = 68;
        }
        let mut line = comment.clone();
        if line.chars().count() < line_length {
            writeln!(
                &mut buf,
                "{}/// {}{}",
                prefix,
                padding,
                comment_replacer(&line, prefix)
            )?;
            break;
        }

        // Don't break URLs.
        // Find the number of bytes for line_length code-points.
        let i = line.chars().take(line_length).collect::<String>().len();
        let mut split_index = if !re_url.is_match(&line[..i]) {
            line = line[..i].into();
            line.rfind(' ')
        } else {
            line.find(' ')
        };
        let new_line_index = line.find('\n');
        if new_line_index.is_some() && (split_index.is_none() || new_line_index < split_index) {
            split_index = new_line_index;
        }
        if let Some(si) = split_index {
            line = line[..si].into();
        }
        writeln!(
            &mut buf,
            "{}/// {}{}",
            prefix,
            padding,
            comment_replacer(&line, prefix)
        )?;
        comment = comment[line.len()..].to_string();
        if split_index.is_some() {
            comment = comment[1..].to_string();
        }
        line_num += 1;
    }
    Ok(buf)
}

fn comment_replacer(comment: &str, prefix: &str) -> String {
    comment
        .replace("\n", &format!("\n{}/// ", prefix))
        .replace("`\"", "\"")
        .replace("\"`", "\"")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn as_comment_long_text() {
        let input = "This is meant to read like some documentation for rustdoc. It should be printed out in a way that can be easily read in code. Making sure not to exceed a line length of 70 chars when possible.";
        let expected = "/// This is meant to read like some documentation for rustdoc. It should
/// be printed out in a way that can be easily read in code. Making sure
/// not to exceed a line length of 70 chars when possible.
";
        let actual = as_comment("", input.to_string(), false).unwrap();
        assert_eq!(expected, &actual);
    }

    #[test]
    fn as_comment_long_link() {
        let input = "This make sure we don't split long links (http://example.com/really/really/really/really/really/really/really/really/really/really/really/long). We want them to show up well in rustdoc.";
        let expected = "/// This make sure we don't split long links
/// (http://example.com/really/really/really/really/really/really/really/really/really/really/really/long).
/// We want them to show up well in rustdoc.
";
        let actual = as_comment("", input.to_string(), false).unwrap();
        assert_eq!(expected, &actual);
    }

    #[test]
    fn as_comment_with_padding() {
        let input = "- var_input: This is meant for pretty printing docs for arguments passed to functions.";
        let expected = "/// - var_input: This is meant for pretty printing docs for arguments
///   passed to functions.
";
        let actual = as_comment("", input.to_string(), true).unwrap();
        assert_eq!(expected, &actual);
    }
}
