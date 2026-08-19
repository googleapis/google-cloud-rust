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

//! `UUID` data type codec for Spanner location-aware routing.

use crate::Result;
use crate::routing::ssformat;
use crate::value::Value;

#[inline]
fn append_bytes_ordered(buffer: &mut Vec<u8>, value: &[u8], decreasing: bool) {
    if decreasing {
        ssformat::append_bytes_decreasing(buffer, value);
    } else {
        ssformat::append_bytes_increasing(buffer, value);
    }
}

#[inline]
fn parse_hex_digits(slice: &[u8], uuid_string: &str) -> Result<u64> {
    let mut value = 0u64;
    for &byte in slice {
        let digit = match byte {
            b'0'..=b'9' => u64::from(byte - b'0'),
            b'a'..=b'f' => u64::from(byte - b'a' + 10),
            b'A'..=b'F' => u64::from(byte - b'A' + 10),
            _ => {
                return Err(crate::error::internal_error(format!(
                    "Invalid UUID string '{uuid_string}': non-hex character"
                )));
            }
        };
        value = (value << 4) | digit;
    }
    Ok(value)
}

/// Parses a Spanner `UUID` string (`"xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"`, with optional `{}` braces)
/// and returns the 16-byte big-endian binary representation `[high: 8 bytes, low: 8 bytes]`.
///
/// Checks length first (36 for standard format, 38 for braced format), validates hyphens at
/// standard indices, and converts hex segments without heap allocations.
pub(crate) fn parse_uuid_bytes(uuid_string: &str) -> Result<[u8; 16]> {
    let uuid_text = match uuid_string.len() {
        36 => uuid_string,
        38 => {
            let bytes = uuid_string.as_bytes();
            if bytes[0] != b'{' || bytes[37] != b'}' {
                return Err(crate::error::internal_error(format!(
                    "Invalid UUID string '{uuid_string}': malformed braces"
                )));
            }
            &uuid_string[1..37]
        }
        _ => {
            return Err(crate::error::internal_error(format!(
                "Invalid UUID string '{uuid_string}': expected 36 or 38 characters"
            )));
        }
    };

    let bytes = uuid_text.as_bytes();
    if bytes[8] != b'-' || bytes[13] != b'-' || bytes[18] != b'-' || bytes[23] != b'-' {
        return Err(crate::error::internal_error(format!(
            "Invalid UUID string '{uuid_string}': malformed hyphens"
        )));
    }

    let high = (parse_hex_digits(&bytes[0..8], uuid_string)? << 32)
        | (parse_hex_digits(&bytes[9..13], uuid_string)? << 16)
        | parse_hex_digits(&bytes[14..18], uuid_string)?;

    let low = (parse_hex_digits(&bytes[19..23], uuid_string)? << 48)
        | parse_hex_digits(&bytes[24..36], uuid_string)?;

    Ok(ssformat::encode_uuid(high, low))
}

/// Encodes a Spanner `UUID` value into the buffer using `ssformat` binary representation.
pub(crate) fn encode_uuid_part(
    buffer: &mut Vec<u8>,
    value: &Value,
    decreasing: bool,
) -> Result<()> {
    let uuid_string = value.try_as_string().ok_or_else(|| {
        crate::error::internal_error("Type mismatch: expected String value for UUID column")
    })?;
    let uuid_bytes = parse_uuid_bytes(uuid_string)?;
    append_bytes_ordered(buffer, &uuid_bytes, decreasing);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::ToValue;

    #[test]
    fn parse_uuid_bytes_standard_format_returns_bytes() {
        let uuid_bytes = parse_uuid_bytes("01234567-89ab-cdef-0123-456789abcdef")
            .expect("failed to parse standard UUID");
        let expected_high = 0x0123_4567_89ab_cdef_u64.to_be_bytes();
        let expected_low = 0x0123_4567_89ab_cdef_u64.to_be_bytes();
        assert_eq!(&uuid_bytes[..8], &expected_high[..], "high bits mismatch");
        assert_eq!(&uuid_bytes[8..16], &expected_low[..], "low bits mismatch");
    }

    #[test]
    fn parse_uuid_bytes_with_braces_returns_bytes() {
        let standard_bytes = parse_uuid_bytes("01234567-89ab-cdef-0123-456789abcdef")
            .expect("failed to parse standard UUID");
        let braced_bytes = parse_uuid_bytes("{01234567-89ab-cdef-0123-456789abcdef}")
            .expect("failed to parse braced UUID");
        assert_eq!(
            standard_bytes, braced_bytes,
            "braced UUID must parse identically to standard UUID"
        );
    }

    #[test]
    fn parse_uuid_bytes_case_insensitive() {
        let lower_bytes = parse_uuid_bytes("01234567-89ab-cdef-0123-456789abcdef")
            .expect("failed to parse lowercase UUID");
        let upper_bytes = parse_uuid_bytes("01234567-89AB-CDEF-0123-456789ABCDEF")
            .expect("failed to parse uppercase UUID");
        assert_eq!(
            lower_bytes, upper_bytes,
            "uppercase and lowercase hex characters must parse identically"
        );
    }

    #[test]
    fn parse_uuid_bytes_spanner_min_max_boundaries() {
        let min_bytes = parse_uuid_bytes("00000000-0000-0000-0000-000000000000")
            .expect("failed to parse minimum UUID");
        assert_eq!(min_bytes, [0u8; 16], "minimum UUID must be all zero bytes");

        let max_bytes = parse_uuid_bytes("ffffffff-ffff-ffff-ffff-ffffffffffff")
            .expect("failed to parse maximum UUID");
        assert_eq!(
            max_bytes, [0xffu8; 16],
            "maximum UUID must be all 0xff bytes"
        );
    }

    #[test]
    fn parse_uuid_bytes_invalid_inputs_return_err() {
        let invalid_cases = [
            "{01234567-89ab-cdef-0123-456789abcdef",
            "01234567-89ab-cdef-0123-456789abcde",
            "01234567-89ab-cdef-0123-456789abcdeff",
            "01234567_89ab_cdef_0123_456789abcdef",
            "-1234567-89ab-cdef-0123-456789abcdef",
            "g1234567-89ab-cdef-0123-456789abcdef",
            "0123456-789a-bcde-f012-3456789abcdef",
            "",
            "{}",
        ];
        for invalid_input in invalid_cases {
            let error = parse_uuid_bytes(invalid_input)
                .expect_err("invalid input should fail UUID parsing");
            assert!(
                error.to_string().contains("Invalid UUID string"),
                "unexpected error message for input '{invalid_input}': {error}"
            );
        }
    }

    #[test]
    fn uuid_encoding_preserves_lexicographical_sort_order() {
        let sorted_uuids = [
            "00000000-0000-0000-0000-000000000000",
            "00000000-0000-0000-0000-000000000001",
            "01234567-89ab-cdef-0123-456789abcdef",
            "ffffffff-ffff-ffff-ffff-ffffffffffff",
        ];
        let mut encoded_buffers = Vec::new();

        for uuid_text in sorted_uuids {
            let value = uuid_text.to_value();
            let mut buffer = Vec::new();
            encode_uuid_part(&mut buffer, &value, false)
                .expect("failed to encode UUID in sort order test");
            encoded_buffers.push(buffer);
        }

        for index in 0..encoded_buffers.len() - 1 {
            assert!(
                encoded_buffers[index] < encoded_buffers[index + 1],
                "UUID encodings must preserve lexicographical sort order: index {index} >= index {}",
                index + 1
            );
        }
    }

    #[test]
    fn encode_uuid_part_ascending_and_descending() {
        let value = "01234567-89ab-cdef-0123-456789abcdef".to_value();
        let mut ascending_buffer = Vec::new();
        encode_uuid_part(&mut ascending_buffer, &value, false)
            .expect("failed to encode ascending UUID part");

        let mut descending_buffer = Vec::new();
        encode_uuid_part(&mut descending_buffer, &value, true)
            .expect("failed to encode descending UUID part");

        assert_ne!(
            ascending_buffer, descending_buffer,
            "ascending and descending UUID encodings must differ"
        );
    }

    #[test]
    fn encode_uuid_part_invalid_value_kind_returns_err() {
        let value = true.to_value();
        let mut buffer = Vec::new();
        let error = encode_uuid_part(&mut buffer, &value, false)
            .expect_err("bool value for UUID column should fail");
        assert!(
            error
                .to_string()
                .contains("Type mismatch: expected String value for UUID column"),
            "unexpected error message: {error}"
        );
    }
}
