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

//! Temporal data type codecs (`DATE` and `TIMESTAMP`) for Spanner location-aware routing.

use crate::Result;
use crate::routing::ssformat;
use crate::value::Value;
use time::{Date, OffsetDateTime, format_description::well_known::Rfc3339};

/// The Julian day number of the Unix epoch (`1970-01-01`).
const UNIX_EPOCH_JULIAN_DAY: i32 = 2440588;

#[inline]
fn append_int64_ordered(buffer: &mut Vec<u8>, value: i64, decreasing: bool) {
    if decreasing {
        ssformat::append_int64_decreasing(buffer, value);
    } else {
        ssformat::append_int64_increasing(buffer, value);
    }
}

#[inline]
fn append_bytes_ordered(buffer: &mut Vec<u8>, value: &[u8], decreasing: bool) {
    if decreasing {
        ssformat::append_bytes_decreasing(buffer, value);
    } else {
        ssformat::append_bytes_increasing(buffer, value);
    }
}

/// Parses an ISO 8601 `"YYYY-MM-DD"` date string and returns the number of days since
/// Unix epoch (`1970-01-01`).
///
/// Supports the full Spanner `DATE` range from `0001-01-01` to `9999-12-31`.
/// Because Spanner dates represent a calendar date without a time or timezone,
/// this evaluation is strictly timezone-independent.
pub(crate) fn parse_date_days(date_string: &str) -> Result<i64> {
    let date = Date::parse(date_string, crate::value::SPANNER_DATE_FORMAT).map_err(|e| {
        crate::error::internal_error(format!("Failed to parse DATE string '{date_string}': {e}"))
    })?;
    let days_since_epoch = date.to_julian_day() - UNIX_EPOCH_JULIAN_DAY;
    Ok(i64::from(days_since_epoch))
}

/// Parses an RFC 3339 `"YYYY-MM-DDTHH:MM:SS[.fffffffff][Z|±HH:MM]"` timestamp string and returns the
/// 12-byte big-endian binary representation `[offset_seconds: 8 bytes, nanos: 4 bytes]`.
///
/// Supports the full Spanner `TIMESTAMP` range from `0001-01-01T00:00:00Z` to
/// `9999-12-31T23:59:59.999999999Z`, including valid non-UTC RFC 3339 timezone offsets
/// normalized to UTC epoch seconds.
pub(crate) fn parse_timestamp_bytes(timestamp_string: &str) -> Result<[u8; 12]> {
    let datetime = OffsetDateTime::parse(timestamp_string, &Rfc3339).map_err(|e| {
        crate::error::internal_error(format!(
            "Failed to parse TIMESTAMP string '{timestamp_string}': {e}"
        ))
    })?;

    Ok(ssformat::encode_timestamp(
        datetime.unix_timestamp(),
        datetime.nanosecond() as i32,
    ))
}

/// Encodes a Spanner `DATE` value into the buffer using `ssformat` binary representation.
pub(crate) fn encode_date_part(
    buffer: &mut Vec<u8>,
    value: &Value,
    decreasing: bool,
) -> Result<()> {
    let date_string = value.try_as_string().ok_or_else(|| {
        crate::error::internal_error(
            "Type mismatch: expected ISO 8601 String value for DATE column",
        )
    })?;
    let days_since_epoch = parse_date_days(date_string)?;
    append_int64_ordered(buffer, days_since_epoch, decreasing);
    Ok(())
}

/// Encodes a Spanner `TIMESTAMP` value into the buffer using `ssformat` binary representation.
pub(crate) fn encode_timestamp_part(
    buffer: &mut Vec<u8>,
    value: &Value,
    decreasing: bool,
) -> Result<()> {
    let timestamp_string = value.try_as_string().ok_or_else(|| {
        crate::error::internal_error(
            "Type mismatch: expected RFC 3339 String value for TIMESTAMP column",
        )
    })?;
    let timestamp_bytes = parse_timestamp_bytes(timestamp_string)?;
    append_bytes_ordered(buffer, &timestamp_bytes, decreasing);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::ToValue;

    #[test]
    fn parse_date_days_unix_epoch_returns_zero() {
        let days =
            parse_date_days("1970-01-01").expect("failed to parse Unix epoch date 1970-01-01");
        assert_eq!(days, 0, "1970-01-01 must be 0 days since epoch");
    }

    #[test]
    fn parse_date_days_spanner_min_max_boundaries() {
        let min_days =
            parse_date_days("0001-01-01").expect("failed to parse minimum Spanner date 0001-01-01");
        assert_eq!(min_days, -719162, "0001-01-01 days since epoch mismatch");

        let max_days =
            parse_date_days("9999-12-31").expect("failed to parse maximum Spanner date 9999-12-31");
        assert_eq!(max_days, 2932896, "9999-12-31 days since epoch mismatch");
    }

    #[test]
    fn parse_date_days_leap_year_century_rules() {
        let leap_year_century =
            parse_date_days("2000-02-29").expect("year 2000 is a valid leap year");
        assert_eq!(
            leap_year_century, 11016,
            "2000-02-29 days since epoch mismatch"
        );

        let invalid_century_leap_year = parse_date_days("1900-02-29");
        assert!(
            invalid_century_leap_year.is_err(),
            "1900-02-29 is not a leap year and must fail to parse"
        );
    }

    #[test]
    fn parse_date_days_positive_and_negative_epochs() {
        let positive_days = parse_date_days("1970-01-02").expect("failed to parse date 1970-01-02");
        assert_eq!(
            positive_days, 1,
            "1970-01-02 must be 1 day after Unix epoch"
        );

        let negative_days = parse_date_days("1969-12-31").expect("failed to parse date 1969-12-31");
        assert_eq!(
            negative_days, -1,
            "1969-12-31 must be 1 day before Unix epoch"
        );

        let future_days =
            parse_date_days("2026-08-05").expect("failed to parse future date 2026-08-05");
        assert_eq!(
            future_days, 20670,
            "2026-08-05 must match expected Julian day difference"
        );
    }

    #[test]
    fn parse_date_days_invalid_inputs_return_err() {
        let invalid_cases = [
            "not_a_date",
            "2026-02-30",
            "2026-13-01",
            "2026-8-5",
            "",
            "2026-08-05T00:00:00Z",
        ];
        for invalid_input in invalid_cases {
            let error =
                parse_date_days(invalid_input).expect_err("invalid input should fail DATE parsing");
            assert!(
                error.to_string().contains("Failed to parse DATE string"),
                "unexpected error message for input '{invalid_input}': {error}"
            );
        }
    }

    #[test]
    fn date_encoding_preserves_chronological_sort_order() {
        let chronological_dates = [
            "0001-01-01",
            "1969-12-31",
            "1970-01-01",
            "2026-08-05",
            "9999-12-31",
        ];
        let mut encoded_buffers = Vec::new();

        for date_text in chronological_dates {
            let value = date_text.to_value();
            let mut buffer = Vec::new();
            encode_date_part(&mut buffer, &value, false)
                .expect("failed to encode date in chronological sort test");
            encoded_buffers.push(buffer);
        }

        for index in 0..encoded_buffers.len() - 1 {
            assert!(
                encoded_buffers[index] < encoded_buffers[index + 1],
                "date encodings must preserve lexicographical chronological sort order: index {index} >= index {}",
                index + 1
            );
        }
    }

    #[test]
    fn parse_timestamp_bytes_unix_epoch() {
        let encoded_bytes = parse_timestamp_bytes("1970-01-01T00:00:00Z")
            .expect("failed to parse Unix epoch timestamp");
        let expected_bytes: [u8; 12] = [0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(
            encoded_bytes, expected_bytes,
            "Unix epoch 0 seconds should encode with sign bit flipped to 0x80"
        );
    }

    #[test]
    fn parse_timestamp_bytes_spanner_min_max_boundaries() {
        let min_timestamp = parse_timestamp_bytes("0001-01-01T00:00:00Z")
            .expect("failed to parse minimum Spanner timestamp");
        assert!(
            min_timestamp[0] < 0x80,
            "min timestamp must sort before epoch 0x80"
        );

        let max_timestamp = parse_timestamp_bytes("9999-12-31T23:59:59.999999999Z")
            .expect("failed to parse maximum Spanner timestamp");
        assert!(
            max_timestamp[0] >= 0x80,
            "max timestamp must sort after epoch 0x80"
        );
    }

    #[test]
    fn parse_timestamp_bytes_with_nanos() {
        let encoded_bytes = parse_timestamp_bytes("1970-01-01T00:00:01.123456789Z")
            .expect("failed to parse timestamp with nanoseconds");
        let expected_seconds = (1i64 ^ (1i64 << 63)).to_be_bytes();
        let expected_nanos = 123456789u32.to_be_bytes();

        assert_eq!(
            &encoded_bytes[..8],
            &expected_seconds[..],
            "seconds part mismatch"
        );
        assert_eq!(
            &encoded_bytes[8..12],
            &expected_nanos[..],
            "nanoseconds part mismatch"
        );
    }

    #[test]
    fn parse_timestamp_bytes_before_epoch() {
        let encoded_bytes = parse_timestamp_bytes("1969-12-31T23:59:59Z")
            .expect("failed to parse pre-epoch timestamp");
        assert_eq!(
            encoded_bytes[0], 0x7F,
            "pre-epoch timestamp high byte must be 0x7F so it sorts before 0x80"
        );
    }

    #[test]
    fn parse_timestamp_bytes_invalid_inputs_return_err() {
        let invalid_cases = [
            "2026-08-05",
            "1970-01-01T24:00:00Z",
            "not_a_timestamp",
            "",
            "1970-01-01T00:00:00",
        ];
        for invalid_input in invalid_cases {
            let error = parse_timestamp_bytes(invalid_input)
                .expect_err("invalid input should fail TIMESTAMP parsing");
            assert!(
                error
                    .to_string()
                    .contains("Failed to parse TIMESTAMP string"),
                "unexpected error message for input '{invalid_input}': {error}"
            );
        }
    }

    #[test]
    fn timestamp_encoding_preserves_chronological_sort_order() {
        let chronological_timestamps = [
            "0001-01-01T00:00:00Z",
            "1969-12-31T23:59:59Z",
            "1970-01-01T00:00:00Z",
            "1970-01-01T00:00:00.000000001Z",
            "1970-01-01T00:00:00.999999999Z",
            "2026-08-05T18:00:00Z",
            "9999-12-31T23:59:59.999999999Z",
        ];
        let mut encoded_buffers = Vec::new();

        for timestamp_text in chronological_timestamps {
            let value = timestamp_text.to_value();
            let mut buffer = Vec::new();
            encode_timestamp_part(&mut buffer, &value, false)
                .expect("failed to encode timestamp in chronological sort test");
            encoded_buffers.push(buffer);
        }

        for index in 0..encoded_buffers.len() - 1 {
            assert!(
                encoded_buffers[index] < encoded_buffers[index + 1],
                "timestamp encodings must preserve lexicographical chronological sort order: index {index} >= index {}",
                index + 1
            );
        }
    }

    #[test]
    fn encode_date_part_ascending_and_descending() {
        let value = "1970-01-02".to_value();
        let mut ascending_buffer = Vec::new();
        encode_date_part(&mut ascending_buffer, &value, false)
            .expect("failed to encode ascending date part");

        let mut descending_buffer = Vec::new();
        encode_date_part(&mut descending_buffer, &value, true)
            .expect("failed to encode descending date part");

        assert_ne!(
            ascending_buffer, descending_buffer,
            "ascending and descending encodings must differ"
        );
    }

    #[test]
    fn encode_date_part_invalid_value_kind_returns_err() {
        let value = true.to_value();
        let mut buffer = Vec::new();
        let error = encode_date_part(&mut buffer, &value, false)
            .expect_err("bool value for DATE column should fail");
        assert!(
            error
                .to_string()
                .contains("Type mismatch: expected ISO 8601 String value for DATE column"),
            "unexpected error message: {error}"
        );
    }

    #[test]
    fn encode_timestamp_part_ascending_and_descending() {
        let value = "1970-01-01T00:00:01Z".to_value();
        let mut ascending_buffer = Vec::new();
        encode_timestamp_part(&mut ascending_buffer, &value, false)
            .expect("failed to encode ascending timestamp part");

        let mut descending_buffer = Vec::new();
        encode_timestamp_part(&mut descending_buffer, &value, true)
            .expect("failed to encode descending timestamp part");

        assert_ne!(
            ascending_buffer, descending_buffer,
            "ascending and descending encodings must differ"
        );
    }

    #[test]
    fn encode_timestamp_part_invalid_value_kind_returns_err() {
        let value = true.to_value();
        let mut buffer = Vec::new();
        let error = encode_timestamp_part(&mut buffer, &value, false)
            .expect_err("bool value for TIMESTAMP column should fail");
        assert!(
            error
                .to_string()
                .contains("Type mismatch: expected RFC 3339 String value for TIMESTAMP column"),
            "unexpected error message: {error}"
        );
    }

    #[test]
    fn parse_timestamp_bytes_non_utc_timezone_offsets() {
        let utc_bytes = parse_timestamp_bytes("1970-01-01T00:00:00Z")
            .expect("failed to parse UTC epoch timestamp");
        let positive_offset = parse_timestamp_bytes("1970-01-01T02:00:00+02:00")
            .expect("failed to parse +02:00 offset timestamp");
        let negative_offset = parse_timestamp_bytes("1969-12-31T16:00:00-08:00")
            .expect("failed to parse -08:00 offset timestamp");
        let nepal_offset = parse_timestamp_bytes("1970-01-01T05:45:00+05:45")
            .expect("failed to parse +05:45 offset timestamp");

        assert_eq!(
            positive_offset, utc_bytes,
            "timestamp with +02:00 offset representing epoch must encode identically to UTC epoch"
        );
        assert_eq!(
            negative_offset, utc_bytes,
            "timestamp with -08:00 offset representing epoch must encode identically to UTC epoch"
        );
        assert_eq!(
            nepal_offset, utc_bytes,
            "timestamp with +05:45 offset representing epoch must encode identically to UTC epoch"
        );
    }

    #[test]
    fn parse_timestamp_bytes_leap_second_handling() {
        // In the Rust `time` crate, RFC 3339 leap seconds (`:60`) are accepted and clamped
        // to the final nanosecond of `:59` (`:59.999999999`), aligning with smeared leap second ordering.
        let leap_second_bytes = parse_timestamp_bytes("1998-12-31T23:59:60Z")
            .expect("RFC 3339 leap second should be parsed");
        let end_of_second_bytes = parse_timestamp_bytes("1998-12-31T23:59:59.999999999Z")
            .expect("end of second should be parsed");
        assert_eq!(
            leap_second_bytes, end_of_second_bytes,
            "leap second (:60) must map to the final nanosecond of :59"
        );
    }

    #[test]
    fn parse_timestamp_bytes_leap_year_century_rules() {
        let leap_year_bytes = parse_timestamp_bytes("2000-02-29T12:00:00Z")
            .expect("year 2000 is a valid leap year timestamp");
        assert!(
            leap_year_bytes[0] >= 0x80,
            "2000-02-29 timestamp must sort after epoch"
        );

        let invalid_century_leap_year = parse_timestamp_bytes("1900-02-29T12:00:00Z");
        assert!(
            invalid_century_leap_year.is_err(),
            "1900-02-29 is not a leap year and must fail to parse as timestamp"
        );
    }

    #[test]
    fn parse_timestamp_bytes_varying_subsecond_precision() {
        let half_second_1_digit =
            parse_timestamp_bytes("1970-01-01T00:00:00.5Z").expect("failed to parse .5Z timestamp");
        let half_second_3_digits = parse_timestamp_bytes("1970-01-01T00:00:00.500Z")
            .expect("failed to parse .500Z timestamp");
        let half_second_9_digits = parse_timestamp_bytes("1970-01-01T00:00:00.500000000Z")
            .expect("failed to parse .500000000Z timestamp");

        assert_eq!(
            half_second_1_digit, half_second_3_digits,
            "1 digit and 3 digits representing 500ms must match"
        );
        assert_eq!(
            half_second_1_digit, half_second_9_digits,
            "1 digit and 9 digits representing 500ms must match"
        );
        let expected_nanos = 500_000_000u32.to_be_bytes();
        assert_eq!(
            &half_second_1_digit[8..12],
            &expected_nanos[..],
            "nanoseconds must equal 500_000_000"
        );
    }
}
