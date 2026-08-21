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

//! Shared helper functions for parsing protobuf `textproto` test fixtures.

use std::iter::Peekable;

/// Unescapes C-style octal escape sequences (e.g. `\206`, `\310`, `\002`) and standard ASCII escapes
/// from Protobuf `textproto` byte strings.
pub(crate) fn unescape_bytes(escaped_string: &str) -> Vec<u8> {
    let mut output = Vec::with_capacity(escaped_string.len());
    let mut bytes = escaped_string.bytes().peekable();

    while let Some(byte) = bytes.next() {
        if byte != b'\\' {
            output.push(byte);
            continue;
        }

        // Try to parse up to 3 octal digits (`\ooo`) which represent raw byte values.
        if let Some(octal_byte) = try_parse_octal_escape(&mut bytes) {
            output.push(octal_byte);
            continue;
        }

        // Otherwise, handle standard ASCII escape sequences (`\n`, `\r`, `\t`, etc.).
        if let Some(next_byte) = bytes.next() {
            let escaped = match next_byte {
                b'n' => b'\n',
                b'r' => b'\r',
                b't' => b'\t',
                b'\\' => b'\\',
                b'"' => b'"',
                _ => next_byte,
            };
            output.push(escaped);
        }
    }

    output
}

/// Helper that attempts to read up to 3 consecutive octal digits from a byte stream.
/// Returns `Some(byte)` if at least one octal digit was consumed, or `None` otherwise.
pub(crate) fn try_parse_octal_escape<I: Iterator<Item = u8>>(
    bytes: &mut Peekable<I>,
) -> Option<u8> {
    let mut value = 0u8;
    let mut parsed_digits = 0;

    for _ in 0..3 {
        if let Some(&byte) = bytes.peek() {
            if (b'0'..=b'7').contains(&byte) {
                let digit = byte - b'0';
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

/// Simple line-trimming helper that extracts the value after a specified field prefix
/// (e.g., stripping `"name: "` and removing surrounding quotes).
pub(crate) fn extract_value<'a>(line: &'a str, prefix: &str) -> Option<&'a str> {
    line.trim()
        .strip_prefix(prefix)
        .map(|rest| rest.trim_matches(|character| character == ' ' || character == '"'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unescape_bytes_plain_and_escapes() {
        assert_eq!(unescape_bytes("hello world"), b"hello world");
        assert_eq!(
            unescape_bytes(r#"\n\r\t\\\""#),
            vec![b'\n', b'\r', b'\t', b'\\', b'\"']
        );
        assert_eq!(unescape_bytes(r#"\001\377\07"#), vec![1, 255, 7]);
        assert_eq!(unescape_bytes(r#"\x"#), b"x");
        assert_eq!(unescape_bytes(r#"abc\"#), b"abc");
    }

    #[test]
    fn try_parse_octal_escape_digits() {
        let mut bytes1 = b"5abc".iter().copied().peekable();
        assert_eq!(try_parse_octal_escape(&mut bytes1), Some(5));

        let mut bytes2 = b"77xyz".iter().copied().peekable();
        assert_eq!(try_parse_octal_escape(&mut bytes2), Some(63));

        let mut bytes3 = b"377xyz".iter().copied().peekable();
        assert_eq!(try_parse_octal_escape(&mut bytes3), Some(255));

        let mut non_octal = b"89".iter().copied().peekable();
        assert_eq!(try_parse_octal_escape(&mut non_octal), None);
    }

    #[test]
    fn extract_value_prefixes_and_quotes() {
        assert_eq!(
            extract_value(r#"  name: "my_test"  "#, "name: "),
            Some("my_test")
        );
        assert_eq!(
            extract_value("  group_uid: 42  ", "group_uid: "),
            Some("42")
        );
        assert_eq!(extract_value("other_field: 1", "name: "), None);
    }
}
