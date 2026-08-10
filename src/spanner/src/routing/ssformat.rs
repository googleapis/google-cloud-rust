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

//! Sortable String Format (`ssformat`) encoding utilities for Spanner keys.
//!
//! This module provides functions to encode various Spanner data types into a byte format
//! that preserves lexicographic ordering. The encoding supports both increasing (ascending)
//! and decreasing (descending) sort orders.

#![allow(dead_code)]

use crate::Result;

const IS_KEY: u8 = 0x80;

// Header type constants for unsigned integers (variable length 1-9 bytes)
const TYPE_UINT_1: u8 = 0;
const TYPE_DECREASING_UINT_1: u8 = 40;

// Header type constants for signed integers (variable length 1-8 bytes)
const TYPE_NEG_INT_1: u8 = 16;
const TYPE_POS_INT_1: u8 = 17;
const TYPE_DECREASING_NEG_INT_1: u8 = 48;
const TYPE_DECREASING_POS_INT_1: u8 = 49;

// Strings and bytes
const TYPE_STRING: u8 = 25;
const TYPE_DECREASING_STRING: u8 = 57;

// Nullable markers
const TYPE_NULL_ORDERED_FIRST: u8 = 27;
const TYPE_NULLABLE_NOT_NULL_NULL_ORDERED_FIRST: u8 = 28;
const TYPE_NULLABLE_NOT_NULL_NULL_ORDERED_LAST: u8 = 59;
const TYPE_NULL_ORDERED_LAST: u8 = 60;

// Doubles (variable length 1-8 bytes, encoded as transformed int64)
const TYPE_NEG_DOUBLE_1: u8 = 73;
const TYPE_POS_DOUBLE_1: u8 = 74;
const TYPE_DECREASING_NEG_DOUBLE_1: u8 = 89;
const TYPE_DECREASING_POS_DOUBLE_1: u8 = 90;

// Escape character bytes
const ASCENDING_ZERO_ESCAPE: u8 = 0xf0;
const ASCENDING_FF_ESCAPE: u8 = 0x10;
const SEP: u8 = 0x78; // 'x'

// Composite tag validation constants
const K_OBJECT_EXISTENCE_TAG: i32 = 0x7e;
const K_MAX_FIELD_TAG: i32 = 0xffff;

// Offset to make negative timestamp seconds sort correctly
const TIMESTAMP_SECONDS_OFFSET: i64 = i64::MIN;

/// Mutates `key` in-place to be its prefix successor.
///
/// Finds the rightmost byte not equal to `0xFF`, increments it by 1, and truncates
/// any trailing bytes. Does nothing if `key` is empty or all bytes are `0xFF`.
pub(crate) fn make_prefix_successor_in_place(key: &mut Vec<u8>) {
    for i in (0..key.len()).rev() {
        if key[i] != 0xFF {
            key[i] += 1;
            key.truncate(i + 1);
            return;
        }
    }
}

/// Makes the given key a prefix successor.
///
/// Returns the smallest possible key that is lexicographically larger than `key`
/// and does not have `key` as a prefix.
pub(crate) fn make_prefix_successor(key: &[u8]) -> Vec<u8> {
    let mut result = key.to_vec();
    make_prefix_successor_in_place(&mut result);
    result
}

/// Appends a composite tag to the output buffer.
pub(crate) fn append_composite_tag(out: &mut Vec<u8>, tag: i32) -> Result<()> {
    if tag == K_OBJECT_EXISTENCE_TAG || tag <= 0 || tag > K_MAX_FIELD_TAG {
        return Err(crate::error::internal_error(format!(
            "Invalid tag value: {tag}"
        )));
    }
    if tag < 16 {
        out.push((tag << 1) as u8);
        return Ok(());
    }
    let shifted_tag = tag << 1;
    if shifted_tag < (1 << (5 + 8)) {
        out.push(((1 << 5) | (shifted_tag >> 8)) as u8);
        out.push((shifted_tag & 0xFF) as u8);
        return Ok(());
    }
    out.push(((2 << 5) | (shifted_tag >> 16)) as u8);
    out.push(((shifted_tag >> 8) & 0xFF) as u8);
    out.push((shifted_tag & 0xFF) as u8);
    Ok(())
}

/// Appends the NULL marker for NULL-ordered-first sorting.
pub(crate) fn append_null_ordered_first(out: &mut Vec<u8>) {
    out.push(IS_KEY | TYPE_NULL_ORDERED_FIRST);
    out.push(0);
}

/// Appends the NULL marker for NULL-ordered-last sorting.
pub(crate) fn append_null_ordered_last(out: &mut Vec<u8>) {
    out.push(IS_KEY | TYPE_NULL_ORDERED_LAST);
    out.push(0);
}

/// Appends the NOT NULL marker for NULL-ordered-first sorting.
pub(crate) fn append_not_null_marker_null_ordered_first(out: &mut Vec<u8>) {
    out.push(IS_KEY | TYPE_NULLABLE_NOT_NULL_NULL_ORDERED_FIRST);
}

/// Appends the NOT NULL marker for NULL-ordered-last sorting.
pub(crate) fn append_not_null_marker_null_ordered_last(out: &mut Vec<u8>) {
    out.push(IS_KEY | TYPE_NULLABLE_NOT_NULL_NULL_ORDERED_LAST);
}

/// Appends a boolean value in increasing (ascending) sort order.
pub(crate) fn append_bool_increasing(out: &mut Vec<u8>, value: bool) {
    let encoded = value as u8;
    out.push(IS_KEY | TYPE_UINT_1);
    out.push(encoded << 1);
}

/// Appends a boolean value in decreasing (descending) sort order.
pub(crate) fn append_bool_decreasing(out: &mut Vec<u8>, value: bool) {
    let encoded = value as u8;
    out.push(IS_KEY | TYPE_DECREASING_UINT_1);
    out.push((!encoded & 0x7F) << 1);
}

fn append_int64_internal(out: &mut Vec<u8>, mut val: i64, decreasing: bool, is_double: bool) {
    if decreasing {
        val = !val;
    }

    let mut buf = [0u8; 8];
    buf[7] = ((val & 0x7F) << 1) as u8;
    let temp_val = val >> 7;

    let len = 1 + if temp_val >= 0 {
        (64 - temp_val.leading_zeros()).div_ceil(8)
    } else {
        (64 - temp_val.leading_ones()).div_ceil(8)
    } as usize;

    if len > 1 {
        let temp_bytes = temp_val.to_be_bytes();
        buf[8 - len..7].copy_from_slice(&temp_bytes[9 - len..8]);
    }

    let type_code = match (val >= 0, decreasing, is_double) {
        (true, false, false) => TYPE_POS_INT_1 + len as u8 - 1,
        (true, false, true) => TYPE_POS_DOUBLE_1 + len as u8 - 1,
        (true, true, false) => TYPE_DECREASING_POS_INT_1 + len as u8 - 1,
        (true, true, true) => TYPE_DECREASING_POS_DOUBLE_1 + len as u8 - 1,
        (false, false, false) => TYPE_NEG_INT_1 - len as u8 + 1,
        (false, false, true) => TYPE_NEG_DOUBLE_1 - len as u8 + 1,
        (false, true, false) => TYPE_DECREASING_NEG_INT_1 - len as u8 + 1,
        (false, true, true) => TYPE_DECREASING_NEG_DOUBLE_1 - len as u8 + 1,
    };

    out.push(IS_KEY | type_code);
    out.extend_from_slice(&buf[8 - len..]);
}

/// Appends a signed 64-bit integer in increasing (ascending) sort order.
pub(crate) fn append_int64_increasing(out: &mut Vec<u8>, value: i64) {
    append_int64_internal(out, value, false, false);
}

/// Appends a signed 64-bit integer in decreasing (descending) sort order.
pub(crate) fn append_int64_decreasing(out: &mut Vec<u8>, value: i64) {
    append_int64_internal(out, value, true, false);
}

/// Returns canonical bits for a 64-bit float, normalizing all NaNs to IEEE 754 quiet NaN
/// (`0x7ff8_0000_0000_0000`).
#[inline]
fn canonical_double_bits(value: f64) -> i64 {
    if value.is_nan() {
        return 0x7ff8_0000_0000_0000_i64;
    }
    value.to_bits() as i64
}

/// Appends a 64-bit floating point number in increasing (ascending) sort order.
pub(crate) fn append_double_increasing(out: &mut Vec<u8>, value: f64) {
    let mut enc = canonical_double_bits(value);
    if enc < 0 {
        enc = i64::MIN.wrapping_sub(enc);
    }
    append_int64_internal(out, enc, false, true);
}

/// Appends a 64-bit floating point number in decreasing (descending) sort order.
pub(crate) fn append_double_decreasing(out: &mut Vec<u8>, value: f64) {
    let mut enc = canonical_double_bits(value);
    if enc < 0 {
        enc = i64::MIN.wrapping_sub(enc);
    }
    append_int64_internal(out, enc, true, true);
}

fn append_byte_sequence(out: &mut Vec<u8>, bytes: &[u8], decreasing: bool) {
    out.reserve(bytes.len() + 3);
    out.push(
        IS_KEY
            | if decreasing {
                TYPE_DECREASING_STRING
            } else {
                TYPE_STRING
            },
    );

    for &b in bytes {
        let current_byte = if decreasing { !b } else { b };
        match current_byte {
            0x00 => {
                out.push(0x00);
                out.push(ASCENDING_ZERO_ESCAPE);
            }
            0xFF => {
                out.push(0xFF);
                out.push(ASCENDING_FF_ESCAPE);
            }
            other => out.push(other),
        }
    }
    out.push(if decreasing { 0xFF } else { 0x00 });
    out.push(SEP);
}

/// Appends a string in increasing (ascending) sort order.
pub(crate) fn append_string_increasing(out: &mut Vec<u8>, value: &str) {
    append_byte_sequence(out, value.as_bytes(), false);
}

/// Appends a string in decreasing (descending) sort order.
pub(crate) fn append_string_decreasing(out: &mut Vec<u8>, value: &str) {
    append_byte_sequence(out, value.as_bytes(), true);
}

/// Appends a byte slice in increasing (ascending) sort order.
pub(crate) fn append_bytes_increasing(out: &mut Vec<u8>, value: &[u8]) {
    append_byte_sequence(out, value, false);
}

/// Appends a byte slice in decreasing (descending) sort order.
pub(crate) fn append_bytes_decreasing(out: &mut Vec<u8>, value: &[u8]) {
    append_byte_sequence(out, value, true);
}

/// Encodes a Spanner timestamp into a 12-byte representation.
///
/// Uses 8 bytes for seconds since Unix epoch (with sign offset) and 4 bytes for nanoseconds.
pub(crate) fn encode_timestamp(seconds: i64, nanos: i32) -> [u8; 12] {
    let offset_seconds = seconds.wrapping_add(TIMESTAMP_SECONDS_OFFSET);
    let mut buf = [0u8; 12];
    buf[0..8].copy_from_slice(&offset_seconds.to_be_bytes());
    buf[8..12].copy_from_slice(&nanos.to_be_bytes());
    buf
}

/// Encodes a 128-bit UUID into a 16-byte big-endian representation.
pub(crate) fn encode_uuid(high: u64, low: u64) -> [u8; 16] {
    let mut buf = [0u8; 16];
    buf[0..8].copy_from_slice(&high.to_be_bytes());
    buf[8..16].copy_from_slice(&low.to_be_bytes());
    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    fn build_signed_int_test_values() -> Vec<i64> {
        let mut values = BTreeSet::new();

        for i in -300..=300 {
            values.insert(i);
        }

        for i in 0..63 {
            let power_of_2 = 1i64 << i;
            values.insert(power_of_2);
            values.insert(power_of_2.saturating_sub(1));
            values.insert(power_of_2.saturating_add(1));
            values.insert(-power_of_2);
            values.insert((-power_of_2).saturating_sub(1));
            values.insert((-power_of_2).saturating_add(1));
        }

        values.insert(i64::MIN);
        values.insert(i64::MAX);

        values.into_iter().collect()
    }

    fn build_double_test_values() -> Vec<f64> {
        vec![
            f64::NEG_INFINITY,
            -1000.0,
            -1.0,
            -0.5,
            -0.0,
            0.0,
            0.5,
            1.0,
            1000.0,
            f64::INFINITY,
        ]
    }

    #[test]
    fn make_prefix_successor_cases() {
        assert_eq!(make_prefix_successor(&[]), Vec::<u8>::new());
        assert_eq!(make_prefix_successor(&[0x00]), vec![0x01]);
        assert_eq!(make_prefix_successor(&[0x01]), vec![0x02]);
        assert_eq!(make_prefix_successor(&[0x02, 0x04]), vec![0x02, 0x05]);
        assert_eq!(make_prefix_successor(&[0xFF]), vec![0xFF]);
        assert_eq!(make_prefix_successor(&[0xFF, 0xFF]), vec![0xFF, 0xFF]);
        assert_eq!(make_prefix_successor(&[0x10, 0xFE]), vec![0x10, 0xFF]);
        assert_eq!(make_prefix_successor(&[0x02, 0xFF]), vec![0x03]);
        assert_eq!(make_prefix_successor(&[0x02, 0xFF, 0xFF]), vec![0x03]);
    }

    #[test]
    fn make_prefix_successor_in_place_cases() {
        let mut key = vec![0x02, 0xFF, 0xFF];
        make_prefix_successor_in_place(&mut key);
        assert_eq!(key, vec![0x03]);

        let mut all_ff = vec![0xFF, 0xFF];
        make_prefix_successor_in_place(&mut all_ff);
        assert_eq!(all_ff, vec![0xFF, 0xFF]);
    }

    #[test]
    fn append_composite_tag_valid_tags() {
        let tags = [1, 15, 16, 100, 4095, 4096, 10000, 65535];
        let mut encoded_results = Vec::new();

        for &tag in &tags {
            let mut out = Vec::new();
            append_composite_tag(&mut out, tag).expect("valid tag should encode successfully");
            assert!(!out.is_empty(), "encoded composite tag should not be empty");
            encoded_results.push(out);
        }

        for i in 0..encoded_results.len() - 1 {
            assert!(
                encoded_results[i] < encoded_results[i + 1],
                "smaller composite tag should sort before larger tag: {:?} vs {:?}",
                tags[i],
                tags[i + 1]
            );
        }
    }

    #[test]
    fn append_composite_tag_invalid_tags() {
        let invalid_tags = [0, -1, K_OBJECT_EXISTENCE_TAG, K_MAX_FIELD_TAG + 1];
        for &tag in &invalid_tags {
            let mut out = Vec::new();
            assert!(
                append_composite_tag(&mut out, tag).is_err(),
                "tag {tag} should be rejected as invalid"
            );
        }
    }

    #[test]
    fn append_int64_increasing_preserves_ordering() {
        let values = build_signed_int_test_values();
        for i in 0..values.len() - 1 {
            let v1 = values[i];
            let v2 = values[i + 1];

            let mut out1 = Vec::new();
            let mut out2 = Vec::new();
            append_int64_increasing(&mut out1, v1);
            append_int64_increasing(&mut out2, v2);

            assert!(out1 < out2, "encoded {v1} should be less than encoded {v2}");
            assert_ne!(out1[0] & IS_KEY, 0, "IS_KEY bit must be set for {v1}");
        }
    }

    #[test]
    fn append_int64_decreasing_reverses_ordering() {
        let values = build_signed_int_test_values();
        for i in 0..values.len() - 1 {
            let v1 = values[i];
            let v2 = values[i + 1];

            let mut out1 = Vec::new();
            let mut out2 = Vec::new();
            append_int64_decreasing(&mut out1, v1);
            append_int64_decreasing(&mut out2, v2);

            assert!(
                out1 > out2,
                "decreasing encoded {v1} should be greater than encoded {v2}"
            );
            assert_ne!(out1[0] & IS_KEY, 0, "IS_KEY bit must be set for {v1}");
        }
    }

    #[test]
    fn append_bool_increasing_preserves_ordering() {
        let mut out_false = Vec::new();
        let mut out_true = Vec::new();
        append_bool_increasing(&mut out_false, false);
        append_bool_increasing(&mut out_true, true);

        assert!(
            out_false < out_true,
            "encoded false should be less than encoded true"
        );
        assert_ne!(out_false[0] & IS_KEY, 0, "IS_KEY bit must be set");
        assert_ne!(out_true[0] & IS_KEY, 0, "IS_KEY bit must be set");
    }

    #[test]
    fn append_bool_decreasing_reverses_ordering() {
        let mut out_false = Vec::new();
        let mut out_true = Vec::new();
        append_bool_decreasing(&mut out_false, false);
        append_bool_decreasing(&mut out_true, true);

        assert!(
            out_false > out_true,
            "decreasing encoded false should be greater than encoded true"
        );
        assert_ne!(out_false[0] & IS_KEY, 0, "IS_KEY bit must be set");
        assert_ne!(out_true[0] & IS_KEY, 0, "IS_KEY bit must be set");
    }

    #[test]
    fn append_double_increasing_preserves_ordering() {
        let values = build_double_test_values();
        for i in 0..values.len() - 1 {
            let v1 = values[i];
            let v2 = values[i + 1];

            let mut out1 = Vec::new();
            let mut out2 = Vec::new();
            append_double_increasing(&mut out1, v1);
            append_double_increasing(&mut out2, v2);

            // -0.0 and 0.0 encode identically, so allow equality
            assert!(
                out1 <= out2,
                "encoded double {v1} should be <= encoded {v2}"
            );
            assert_ne!(out1[0] & IS_KEY, 0, "IS_KEY bit must be set for {v1}");
        }
    }

    #[test]
    fn append_double_decreasing_reverses_ordering() {
        let values = build_double_test_values();
        for i in 0..values.len() - 1 {
            let v1 = values[i];
            let v2 = values[i + 1];

            let mut out1 = Vec::new();
            let mut out2 = Vec::new();
            append_double_decreasing(&mut out1, v1);
            append_double_decreasing(&mut out2, v2);

            // -0.0 and 0.0 encode identically, so allow equality
            assert!(
                out1 >= out2,
                "decreasing encoded double {v1} should be >= encoded {v2}"
            );
            assert_ne!(out1[0] & IS_KEY, 0, "IS_KEY bit must be set for {v1}");
        }
    }

    #[test]
    fn nan_normalization_produces_identical_encoding() {
        let nan1 = f64::NAN;
        // A NaN with a custom non-zero payload bit set
        let nan2 = f64::from_bits(0x7ff8_0000_0000_0001);
        assert!(nan2.is_nan());

        let mut out1 = Vec::new();
        let mut out2 = Vec::new();
        append_double_increasing(&mut out1, nan1);
        append_double_increasing(&mut out2, nan2);
        assert_eq!(
            out1, out2,
            "Different NaN payloads must encode identically in increasing order"
        );

        let mut out3 = Vec::new();
        let mut out4 = Vec::new();
        append_double_decreasing(&mut out3, nan1);
        append_double_decreasing(&mut out4, nan2);
        assert_eq!(
            out3, out4,
            "Different NaN payloads must encode identically in decreasing order"
        );
    }

    #[test]
    fn append_string_increasing_preserves_ordering() {
        let strings = [
            "",
            "\x00",
            "\x00\x00",
            "\x00a",
            "a",
            "aa",
            "b",
            "c\x00",
            "c\u{00FF}",
            "\u{00FF}",
            "\u{00FF}\x00",
            "\u{00FF}\u{00FF}",
        ];

        for i in 0..strings.len() - 1 {
            let s1 = strings[i];
            let s2 = strings[i + 1];

            let mut out1 = Vec::new();
            let mut out2 = Vec::new();
            append_string_increasing(&mut out1, s1);
            append_string_increasing(&mut out2, s2);

            assert!(
                out1 < out2,
                "encoded string {s1:?} should be less than {s2:?}"
            );
            assert_ne!(out1[0] & IS_KEY, 0, "IS_KEY bit must be set for {s1:?}");
        }
    }

    #[test]
    fn append_string_decreasing_reverses_ordering() {
        let strings = [
            "",
            "\x00",
            "\x00\x00",
            "\x00a",
            "a",
            "aa",
            "b",
            "c\x00",
            "c\u{00FF}",
            "\u{00FF}",
            "\u{00FF}\x00",
            "\u{00FF}\u{00FF}",
        ];

        for i in 0..strings.len() - 1 {
            let s1 = strings[i];
            let s2 = strings[i + 1];

            let mut out1 = Vec::new();
            let mut out2 = Vec::new();
            append_string_decreasing(&mut out1, s1);
            append_string_decreasing(&mut out2, s2);

            assert!(
                out1 > out2,
                "decreasing encoded string {s1:?} should be greater than {s2:?}"
            );
            assert_ne!(out1[0] & IS_KEY, 0, "IS_KEY bit must be set for {s1:?}");
        }
    }

    #[test]
    fn append_bytes_increasing_preserves_ordering() {
        let byte_slices: &[&[u8]] = &[
            &[],
            &[0x00],
            &[0x00, 0x00],
            &[0x00, 0x01],
            &[0x01],
            &[0x42],
            &[0xFF],
            &[0xFF, 0x00],
            &[0xFF, 0xFF],
        ];

        for i in 0..byte_slices.len() - 1 {
            let b1 = byte_slices[i];
            let b2 = byte_slices[i + 1];

            let mut out1 = Vec::new();
            let mut out2 = Vec::new();
            append_bytes_increasing(&mut out1, b1);
            append_bytes_increasing(&mut out2, b2);

            assert!(
                out1 < out2,
                "encoded bytes {b1:?} should be less than {b2:?}"
            );
            assert_ne!(out1[0] & IS_KEY, 0, "IS_KEY bit must be set for {b1:?}");
        }
    }

    #[test]
    fn append_bytes_decreasing_reverses_ordering() {
        let byte_slices: &[&[u8]] = &[
            &[],
            &[0x00],
            &[0x00, 0x00],
            &[0x00, 0x01],
            &[0x01],
            &[0x42],
            &[0xFF],
            &[0xFF, 0x00],
            &[0xFF, 0xFF],
        ];

        for i in 0..byte_slices.len() - 1 {
            let b1 = byte_slices[i];
            let b2 = byte_slices[i + 1];

            let mut out1 = Vec::new();
            let mut out2 = Vec::new();
            append_bytes_decreasing(&mut out1, b1);
            append_bytes_decreasing(&mut out2, b2);

            assert!(
                out1 > out2,
                "decreasing encoded bytes {b1:?} should be greater than {b2:?}"
            );
            assert_ne!(out1[0] & IS_KEY, 0, "IS_KEY bit must be set for {b1:?}");
        }
    }

    #[test]
    fn nullable_markers_encoding() {
        let mut out = Vec::new();
        append_null_ordered_first(&mut out);
        assert_eq!(out, vec![IS_KEY | TYPE_NULL_ORDERED_FIRST, 0x00]);

        out.clear();
        append_null_ordered_last(&mut out);
        assert_eq!(out, vec![IS_KEY | TYPE_NULL_ORDERED_LAST, 0x00]);

        out.clear();
        append_not_null_marker_null_ordered_first(&mut out);
        assert_eq!(
            out,
            vec![IS_KEY | TYPE_NULLABLE_NOT_NULL_NULL_ORDERED_FIRST]
        );

        out.clear();
        append_not_null_marker_null_ordered_last(&mut out);
        assert_eq!(out, vec![IS_KEY | TYPE_NULLABLE_NOT_NULL_NULL_ORDERED_LAST]);
    }

    #[test]
    fn encode_timestamp_and_uuid() {
        let ts = encode_timestamp(0, 100);
        assert_eq!(ts.len(), 12);

        let uuid = encode_uuid(0x0123456789ABCDEF, 0xFEDCBA9876543210);
        assert_eq!(
            uuid,
            [
                0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF, 0xFE, 0xDC, 0xBA, 0x98, 0x76, 0x54,
                0x32, 0x10
            ]
        );
    }

    #[test]
    fn composite_key_tag_plus_int_preserves_ordering() {
        let tag = 5;
        let values = [i64::MIN, -1, 0, 1, i64::MAX];

        for i in 0..values.len() - 1 {
            let mut out1 = Vec::new();
            let mut out2 = Vec::new();

            append_composite_tag(&mut out1, tag).expect("valid tag");
            append_int64_increasing(&mut out1, values[i]);

            append_composite_tag(&mut out2, tag).expect("valid tag");
            append_int64_increasing(&mut out2, values[i + 1]);

            assert!(
                out1 < out2,
                "Composite key with {} should be less than with {}",
                values[i],
                values[i + 1]
            );
        }
    }

    #[test]
    fn composite_key_different_tags_sort_by_tag() {
        let value = 100i64;

        let mut out1 = Vec::new();
        let mut out2 = Vec::new();

        append_composite_tag(&mut out1, 5).expect("valid tag");
        append_int64_increasing(&mut out1, value);

        append_composite_tag(&mut out2, 10).expect("valid tag");
        append_int64_increasing(&mut out2, value);

        assert!(out1 < out2, "Key with smaller tag should sort first");
    }

    #[test]
    fn composite_key_multiple_key_parts() {
        let mut out1 = Vec::new();
        let mut out2 = Vec::new();

        append_composite_tag(&mut out1, 1).expect("valid tag");
        append_int64_increasing(&mut out1, 100);
        append_string_increasing(&mut out1, "alice");

        append_composite_tag(&mut out2, 1).expect("valid tag");
        append_int64_increasing(&mut out2, 100);
        append_string_increasing(&mut out2, "bob");

        assert!(
            out1 < out2,
            "Keys with same prefix but different strings should order by string"
        );
    }

    #[test]
    fn null_ordered_first_sorts_before_values() {
        let mut null_out = Vec::new();
        let mut value_out = Vec::new();

        append_null_ordered_first(&mut null_out);
        append_not_null_marker_null_ordered_first(&mut value_out);
        append_int64_increasing(&mut value_out, i64::MIN);

        assert!(
            null_out < value_out,
            "Null (ordered first) should sort before any value"
        );
    }

    #[test]
    fn null_ordered_last_sorts_after_values() {
        let mut null_out = Vec::new();
        let mut value_out = Vec::new();

        append_null_ordered_last(&mut null_out);
        append_not_null_marker_null_ordered_last(&mut value_out);
        append_int64_increasing(&mut value_out, i64::MAX);

        assert!(
            null_out > value_out,
            "Null (ordered last) should sort after any value"
        );
    }

    #[test]
    fn encode_timestamp_preserves_ordering() {
        let timestamps = [
            (i64::MIN, 0i32),
            (-100, 500),
            (-1, 999_999_999),
            (0i64, 0i32),
            (0, 1),
            (0, 999_999_999),
            (1, 0),
            (100, 500_000_000),
            (i64::MAX / 2, 0),
            (i64::MAX, 999_999_999),
        ];

        for i in 0..timestamps.len() - 1 {
            let t1 = encode_timestamp(timestamps[i].0, timestamps[i].1);
            let t2 = encode_timestamp(timestamps[i + 1].0, timestamps[i + 1].1);
            assert!(
                t1 < t2,
                "Earlier timestamp should encode smaller: {:?} vs {:?}",
                timestamps[i],
                timestamps[i + 1]
            );
        }
    }

    #[test]
    fn encode_uuid_preserves_ordering() {
        let uuids = [
            (0u64, 0u64),
            (0, 1),
            (0, u64::MAX),
            (1, 0),
            (u64::MAX, u64::MAX),
        ];

        for i in 0..uuids.len() - 1 {
            let u1 = encode_uuid(uuids[i].0, uuids[i].1);
            let u2 = encode_uuid(uuids[i + 1].0, uuids[i + 1].1);
            assert!(
                u1 < u2,
                "UUID ordering should be preserved: {:?} vs {:?}",
                uuids[i],
                uuids[i + 1]
            );
        }
    }

    #[test]
    fn canonical_double_bits_nan_is_ieee_quiet_nan() {
        assert_eq!(canonical_double_bits(f64::NAN), 0x7ff8_0000_0000_0000_i64);
        assert_eq!(canonical_double_bits(-f64::NAN), 0x7ff8_0000_0000_0000_i64);
        assert_eq!(canonical_double_bits(0.0), 0i64);
        assert_eq!(canonical_double_bits(1.0), 0x3FF0_0000_0000_0000_i64);
    }

    #[test]
    fn append_int64_internal_byte_lengths() {
        let cases = [
            (0, 2),
            (127, 2),
            (128, 3),
            (-1, 2),
            (-128, 2),
            (-129, 3),
            (i64::MAX, 9),
            (i64::MIN, 9),
        ];

        for (val, expected_len) in cases {
            let mut out = Vec::new();
            append_int64_increasing(&mut out, val);
            assert_eq!(
                out.len(),
                expected_len,
                "val {val} should encode to {expected_len} bytes"
            );
        }
    }
}
