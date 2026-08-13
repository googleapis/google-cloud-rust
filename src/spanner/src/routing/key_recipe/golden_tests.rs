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

use super::*;
use crate::model::key_recipe::Part;
use crate::model::key_recipe::part::{NullOrder, Order};
use crate::model::{KeyRecipe, Type, TypeCode};
use crate::value::{ToValue, Value};
use std::fs;
use std::iter::Peekable;
use std::path::Path;

/// Unescapes C-style octal escape sequences (e.g. `\206`, `\310`, `\002`) and standard ASCII escapes
/// from Protobuf `textproto` byte strings.
fn unescape_bytes(s: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(s.len());
    let mut bytes = s.bytes().peekable();

    while let Some(b) = bytes.next() {
        if b != b'\\' {
            out.push(b);
            continue;
        }

        // Try to parse up to 3 octal digits (`\ooo`) which represent raw byte values.
        if let Some(octal_byte) = try_parse_octal_escape(&mut bytes) {
            out.push(octal_byte);
            continue;
        }

        // Otherwise, handle standard ASCII escape sequences (`\n`, `\r`, `\t`, etc.).
        if let Some(next_b) = bytes.next() {
            let escaped = match next_b {
                b'n' => b'\n',
                b'r' => b'\r',
                b't' => b'\t',
                b'\\' => b'\\',
                b'"' => b'"',
                _ => next_b,
            };
            out.push(escaped);
        }
    }

    out
}

/// Helper that attempts to read up to 3 consecutive octal digits from a byte stream.
/// Returns `Some(byte)` if at least one octal digit was consumed, or `None` otherwise.
fn try_parse_octal_escape<I: Iterator<Item = u8>>(bytes: &mut Peekable<I>) -> Option<u8> {
    let mut value = 0u8;
    let mut parsed_digits = 0;

    for _ in 0..3 {
        if let Some(&b) = bytes.peek() {
            if (b'0'..=b'7').contains(&b) {
                let digit = b - b'0';
                value = value.wrapping_mul(8).wrapping_add(digit);
                bytes.next(); // Consume the octal digit byte.
                parsed_digits += 1;
            } else {
                break;
            }
        }
    }

    if parsed_digits > 0 { Some(value) } else { None }
}

struct ParsedTestCase {
    name: String,
    recipe: KeyRecipe,
    tests: Vec<ParsedTest>,
}

struct ParsedTest {
    values: Vec<Value>,
    start: Vec<u8>,
    approximate: bool,
}

/// Simple line-trimming helper that extracts the value after a specified field prefix
/// (e.g., stripping `"name: "` and removing surrounding quotes).
fn extract_value<'a>(line: &'a str, prefix: &str) -> Option<&'a str> {
    line.trim()
        .strip_prefix(prefix)
        .map(|rest| rest.trim_matches(|c| c == ' ' || c == '"'))
}

/// Maps Spanner data type code names in `recipe_test.textproto` to [`TypeCode`].
fn parse_type_code(code_string: &str) -> Option<TypeCode> {
    match code_string {
        "BOOL" => Some(TypeCode::Bool),
        "INT64" => Some(TypeCode::Int64),
        "FLOAT64" => Some(TypeCode::Float64),
        "STRING" => Some(TypeCode::String),
        "BYTES" => Some(TypeCode::Bytes),
        "DATE" => Some(TypeCode::Date),
        "TIMESTAMP" => Some(TypeCode::Timestamp),
        "UUID" => Some(TypeCode::Uuid),
        "ENUM" => Some(TypeCode::Enum),
        _ => None,
    }
}

/// Consumes lines belonging to a `part { ... }` block inside a Spanner key recipe and returns a [`Part`].
///
/// A part either represents a composite tag (`tag: <u32>`) or an individual column definition
/// with an ordering (`ASCENDING` / `DESCENDING`), a null ordering (`NULLS_FIRST`, `NULLS_LAST`, etc.),
/// and a Spanner type code (`BOOL`, `INT64`, `FLOAT64`, `STRING`, `BYTES`, etc.).
fn parse_part_block<'a, I: Iterator<Item = &'a str>>(lines: &mut Peekable<I>) -> Part {
    let mut tag = 0u32;
    let mut order = Order::Ascending;
    let mut null_order = NullOrder::Unspecified;
    let mut type_code = None;
    let mut depth = 1;

    for line in lines.by_ref() {
        let trimmed = line.trim();
        if trimmed.contains('{') {
            depth += 1;
        }
        if trimmed == "}" || trimmed.ends_with('}') {
            depth -= 1;
            if depth == 0 {
                break;
            }
        }

        if let Some(tag_string) = extract_value(trimmed, "tag:") {
            if let Ok(tag_number) = tag_string.parse::<u32>() {
                tag = tag_number;
            }
        } else if let Some(order_string) = extract_value(trimmed, "order:") {
            order = match order_string {
                "DESCENDING" => Order::Descending,
                "ASCENDING" => Order::Ascending,
                _ => order,
            };
        } else if let Some(null_order_string) = extract_value(trimmed, "null_order:") {
            null_order = match null_order_string {
                "NULLS_FIRST" => NullOrder::NullsFirst,
                "NULLS_LAST" => NullOrder::NullsLast,
                "NOT_NULL" => NullOrder::NotNull,
                _ => null_order,
            };
        } else if let Some(code_string) = extract_value(trimmed, "code:") {
            type_code = parse_type_code(code_string);
        }
    }

    let mut part = Part::new();
    if tag != 0 {
        part = part.set_tag(tag);
    } else {
        part = part.set_order(order).set_null_order(null_order);
        if let Some(code) = type_code {
            part = part.set_type(Type::default().set_code(code));
        }
    }
    part
}

/// Consumes lines belonging to a `test { ... }` block inside a test case and returns a [`ParsedTest`]
/// if it represents a simple key evaluation (`key { ... }`).
///
/// This helper filters out more advanced routing test structures (such as `key_range`, `key_set`,
/// and `query_params`) so that we only execute direct conformance tests against `encode_key_from_recipe`.
fn parse_test_block<'a, I: Iterator<Item = &'a str>>(
    lines: &mut Peekable<I>,
) -> Option<ParsedTest> {
    let mut values = Vec::new();
    let mut start = None;
    let mut approximate = false;
    let mut is_simple_key = false;
    let mut depth = 1;

    for line in lines.by_ref() {
        let trimmed = line.trim();
        if trimmed.contains('{') {
            depth += 1;
        }
        if trimmed == "}" || trimmed.ends_with('}') {
            depth -= 1;
            if depth == 0 {
                break;
            }
        }

        if trimmed.starts_with("key {") {
            is_simple_key = true;
        } else if trimmed.starts_with("key_range {")
            || trimmed.starts_with("key_set {")
            || trimmed.starts_with("query_params {")
        {
            is_simple_key = false;
        } else if let Some(start_string) = extract_value(trimmed, "start:") {
            start = Some(unescape_bytes(start_string));
        } else if let Some(boolean_string) = extract_value(trimmed, "bool_value:") {
            values.push((boolean_string == "true").to_value());
        } else if let Some(string_value) = extract_value(trimmed, "string_value:") {
            values.push(string_value.to_value());
        } else if let Some(number_string) = extract_value(trimmed, "number_value:") {
            if let Ok(num) = number_string.parse::<f64>() {
                values.push(num.to_value());
            }
        } else if trimmed == "null_value: NULL_VALUE" {
            values.push(Value::null());
        } else if trimmed == "approximate: true" {
            approximate = true;
        }
    }

    if let (true, Some(start_bytes)) = (is_simple_key, start) {
        Some(ParsedTest {
            values,
            start: start_bytes,
            approximate,
        })
    } else {
        None
    }
}

/// Consumes lines belonging to a top-level `test_case { ... }` block and coordinates parsing of its
/// name, recipe parts, and conformance test executions.
fn parse_test_case_block<'a, I: Iterator<Item = &'a str>>(
    lines: &mut Peekable<I>,
) -> Option<ParsedTestCase> {
    let mut name = String::new();
    let mut parts = Vec::new();
    let mut tests = Vec::new();
    let mut depth = 1;

    while let Some(line) = lines.next() {
        let trimmed = line.trim();
        if !trimmed.starts_with("part {") && !trimmed.starts_with("test {") {
            if trimmed.contains('{') {
                depth += 1;
            }
            if trimmed == "}" || trimmed.ends_with('}') {
                depth -= 1;
                if depth == 0 {
                    break;
                }
            }
        }

        if let Some(n) = extract_value(trimmed, "name:") {
            name = n.to_string();
        } else if trimmed.starts_with("part {") {
            parts.push(parse_part_block(lines));
        } else if trimmed.starts_with("test {")
            && let Some(test) = parse_test_block(lines)
        {
            tests.push(test);
        }
    }

    if !name.is_empty() && !parts.is_empty() {
        Some(ParsedTestCase {
            name,
            recipe: KeyRecipe::new().set_part(parts),
            tests,
        })
    } else {
        None
    }
}

/// Parses a subset of `recipe_test.textproto` sufficient for golden conformance testing.
fn parse_golden_textproto(content: &str) -> Vec<ParsedTestCase> {
    let mut cases = Vec::new();
    let mut lines = content.lines().peekable();

    while let Some(line) = lines.next() {
        if line.trim().starts_with("test_case {")
            && let Some(case) = parse_test_case_block(&mut lines)
        {
            cases.push(case);
        }
    }

    cases
}

#[test]
fn golden_conformance_supported_types() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("routing")
        .join("testdata")
        .join("recipe_test.textproto");

    let textproto = fs::read_to_string(&path)
        .expect("failed to load Spanner golden testdata from recipe_test.textproto");

    let cases = parse_golden_textproto(&textproto);
    assert!(
        !cases.is_empty(),
        "must parse at least one golden test case"
    );

    // Only execute golden conformance tests for data types currently supported by `encode_key_from_recipe`.
    // Binary ssformat encoding for DATE, TIMESTAMP, UUID, and ENUM is not yet supported and will be
    // added in subsequent pull requests.
    let supported_test_prefixes = [
        "DataTypeTest_BOOL",
        "DataTypeTest_INT64",
        "DataTypeTest_FLOAT64",
        "DataTypeTest_STRING",
        "DataTypeTest_BYTES",
    ];

    let mut executed_tests = 0;

    for case in cases {
        if !supported_test_prefixes
            .iter()
            .any(|prefix| case.name.starts_with(prefix))
        {
            continue;
        }

        for (index, test) in case.tests.iter().enumerate() {
            // In Spanner's `recipe_test.textproto`, tests marked `approximate: true` represent
            // cases where an invalid value type was provided (e.g., passing string `"true"` for a
            // `BOOL` column). Spanner's router handles this by falling back to a partial prefix
            // range lookup using only the preceding table/index tags. Because our `encode_key_from_recipe`
            // method strictly verifies types and returns `Err` on type mismatch rather than truncating
            // the key, we skip approximate prefix tests in full-key conformance verification.
            if test.approximate {
                continue;
            }

            let encoded = match encode_key_from_recipe(&case.recipe, &test.values) {
                Ok(bytes) => bytes,
                Err(e) => panic!(
                    "Golden test case {} index {} failed encoding: {}",
                    case.name, index, e
                ),
            };

            assert_eq!(
                &encoded, &test.start,
                "Mismatch in golden test case {} at test index {}: expected {:?}, got {:?}",
                case.name, index, test.start, encoded
            );
            executed_tests += 1;
        }
    }

    assert!(
        executed_tests > 20,
        "Expected to execute over 20 golden tests for supported types, executed {executed_tests}"
    );
}
