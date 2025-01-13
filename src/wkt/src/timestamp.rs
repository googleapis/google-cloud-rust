// Copyright 2024 Google LLC
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

/// Well-known point in time representation for Google APIs.
///
/// A Timestamp represents a point in time independent of any time zone or local
/// calendar, encoded as a count of seconds and fractions of seconds at
/// nanosecond resolution. The count is relative to an epoch at UTC midnight on
/// January 1, 1970, in the proleptic Gregorian calendar which extends the
/// Gregorian calendar backwards to year one.
///
/// All minutes are 60 seconds long. Leap seconds are "smeared" so that no leap
/// second table is needed for interpretation, using a [24-hour linear
/// smear](https://developers.google.com/time/smear).
///
/// The range is from 0001-01-01T00:00:00Z to 9999-12-31T23:59:59.999999999Z. By
/// restricting to that range, we ensure that we can convert to and from [RFC
/// 3339](https://www.ietf.org/rfc/rfc3339.txt) date strings.
///
/// # JSON Mapping
///
/// In JSON format, the Timestamp type is encoded as a string in the
/// [RFC 3339](https://www.ietf.org/rfc/rfc3339.txt) format. That is, the
/// format is "{year}-{month}-{day}T{hour}:{min}:{sec}[.{frac_sec}]Z"
/// where {year} is always expressed using four digits while {month}, {day},
/// {hour}, {min}, and {sec} are zero-padded to two digits each. The fractional
/// seconds, which can go up to 9 digits (i.e. up to 1 nanosecond resolution),
/// are optional. The "Z" suffix indicates the timezone ("UTC"); the timezone
/// is required.
///
/// For example, "2017-01-15T01:30:15.01Z" encodes 15.01 seconds past
/// 01:30 UTC on January 15, 2017.
///
#[derive(Clone, Debug, Default, PartialEq, PartialOrd)]
#[non_exhaustive]
pub struct Timestamp {
    /// Represents seconds of UTC time since Unix epoch
    /// 1970-01-01T00:00:00Z. Must be from 0001-01-01T00:00:00Z to
    /// 9999-12-31T23:59:59Z inclusive.
    seconds: i64,

    /// Non-negative fractions of a second at nanosecond resolution. Negative
    /// second values with fractions must still have non-negative nanos values
    /// that count forward in time. Must be from 0 to 999,999,999
    /// inclusive.
    nanos: i32,
}

/// Represent failures in converting or creating [Timestamp] instances.
#[derive(thiserror::Error, Debug, PartialEq)]
pub enum TimestampError {
    /// One of the components (seconds and/or nanoseconds) was out of range.
    #[error("seconds and/or nanoseconds out of range")]
    OutOfRange(),

    #[error("cannot serialize timestamp: {0}")]
    Serialize(String),

    #[error("cannot deserialize timestamp: {0}")]
    Deserialize(String),
}

type Error = TimestampError;

impl Timestamp {
    const NS: i32 = 1_000_000_000;

    // Obtained via: `date +%s --date='0001-01-01T00:00:00Z'`
    /// The minimum value for the `seconds` component. Corresponds to '0001-01-01T00:00:00Z'.
    pub const MIN_SECONDS: i64 = -62135596800;

    // Obtained via: `date +%s --date='9999-12-31T23:59:59Z'`
    /// The maximum value for the `seconds` component. Corresponds to '9999-12-31T23:59:59Z'.
    pub const MAX_SECONDS: i64 = 253402300799;

    /// The minimum value for the `nanos` component.
    pub const MIN_NANOS: i32 = 0;

    /// The maximum value for the `nanos` component.
    pub const MAX_NANOS: i32 = Self::NS - 1;

    /// Creates a new [Timestamp] from the seconds and nanoseconds.
    ///
    /// If either value is out of range it returns an error.
    ///
    /// # Arguments
    ///
    /// * `seconds` - the seconds on the timestamp.
    /// * `nanos` - the nanoseconds on the timestamp.
    pub fn new(seconds: i64, nanos: i32) -> std::result::Result<Self, Error> {
        if !(Self::MIN_SECONDS..=Self::MAX_SECONDS).contains(&seconds) {
            return Err(Error::OutOfRange());
        }
        if !(Self::MIN_NANOS..=Self::MAX_NANOS).contains(&nanos) {
            return Err(Error::OutOfRange());
        }
        Ok(Self { seconds, nanos })
    }

    /// Create a normalized, clamped [Timestamp].
    ///
    /// Timestamps must be between 0001-01-01T00:00:00Z and
    /// 9999-12-31T23:59:59.999999999Z, and the nanoseconds component must
    /// always be in the range [0, 999_999_999]. This function creates a
    /// new [Timestamp] instance clamped to those ranges.
    ///
    /// The function effectively adds the nanoseconds part (with carry) to the
    /// seconds part, with saturation.
    ///
    /// # Arguments
    ///
    /// * `seconds` - the seconds on the timestamp.
    /// * `nanos` - the nanoseconds added to the seconds.
    pub fn clamp(seconds: i64, nanos: i32) -> Self {
        let (seconds, nanos) = match nanos.cmp(&0_i32) {
            std::cmp::Ordering::Equal => (seconds, nanos),
            std::cmp::Ordering::Greater => (
                seconds.saturating_add((nanos / Self::NS) as i64),
                nanos % Self::NS,
            ),
            std::cmp::Ordering::Less => (
                seconds.saturating_sub(1 - (nanos / Self::NS) as i64),
                Self::NS + nanos % Self::NS,
            ),
        };
        if seconds < Self::MIN_SECONDS {
            return Self {
                seconds: Self::MIN_SECONDS,
                nanos: 0,
            };
        } else if seconds > Self::MAX_SECONDS {
            return Self {
                seconds: Self::MAX_SECONDS,
                nanos: 0,
            };
        }
        Self { seconds, nanos }
    }

    /// Represents seconds of UTC time since Unix epoch (1970-01-01T00:00:00Z).
    ///
    /// Must be from 0001-01-01T00:00:00Z to 9999-12-31T23:59:59Z inclusive.
    pub fn seconds(&self) -> i64 {
        self.seconds
    }

    /// Non-negative fractions of a second at nanosecond resolution.
    ///
    /// Negative second values (before the Unix epoch) with fractions must still
    /// have non-negative nanos values that count forward in time. Must be from
    /// 0 to 999,999,999 inclusive.
    pub fn nanos(&self) -> i32 {
        self.nanos
    }
}

impl crate::message::Message for Timestamp {
    fn typename() -> &'static str {
        "type.googleapis.com/google.protobuf.Timestamp"
    }
}

use time::format_description::well_known::Rfc3339;
const NS: i128 = 1_000_000_000;

/// Implement [`serde`](::serde) serialization for timestamps.
impl serde::ser::Serialize for Timestamp {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        use serde::ser::Error as _;
        String::try_from(self)
            .map_err(S::Error::custom)?
            .serialize(serializer)
    }
}

struct TimestampVisitor;

impl serde::de::Visitor<'_> for TimestampVisitor {
    type Value = Timestamp;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a string with a timestamp in RFC 3339 format")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Timestamp::try_from(value).map_err(E::custom)
    }
}

/// Implement [`serde`](::serde) deserialization for timestamps.
impl<'de> serde::de::Deserialize<'de> for Timestamp {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(TimestampVisitor)
    }
}

/// Convert from [time::OffsetDateTime] to [Timestamp].
///
/// This conversion may fail if the [time::OffsetDateTime] value is out of range.
#[cfg(feature = "time")]
impl TryFrom<time::OffsetDateTime> for Timestamp {
    type Error = TimestampError;

    fn try_from(value: time::OffsetDateTime) -> std::result::Result<Self, Self::Error> {
        use time::convert::{Nanosecond, Second};

        let seconds = value.unix_timestamp();
        let nanos = (value.unix_timestamp_nanos()
            - seconds as i128 * Nanosecond::per(Second) as i128) as i32;
        Self::new(seconds, nanos)
    }
}

/// Convert from [Timestamp] to [OffsetDateTime][time::OffsetDateTime]
///
/// This conversion may fail if the [Timestamp] value is out of range.
#[cfg(feature = "time")]
impl TryFrom<Timestamp> for time::OffsetDateTime {
    type Error = time::error::ComponentRange;
    fn try_from(value: Timestamp) -> std::result::Result<Self, Self::Error> {
        let ts = time::OffsetDateTime::from_unix_timestamp(value.seconds())?;
        Ok(ts + time::Duration::nanoseconds(value.nanos() as i64))
    }
}

/// Converts a [Timestamp] to its [String] representation.
impl TryFrom<&Timestamp> for String {
    type Error = TimestampError;
    fn try_from(timestamp: &Timestamp) -> std::result::Result<Self, Self::Error> {
        let ts = time::OffsetDateTime::from_unix_timestamp_nanos(
            timestamp.seconds as i128 * NS + timestamp.nanos as i128,
        )
        .map_err(|e| TimestampError::Serialize(format!("{e}")))?;
        ts.format(&Rfc3339)
            .map_err(|e| TimestampError::Serialize(format!("{e}")))
    }
}

/// Converts the [String] representation of a timestamp to [Timestamp].
impl TryFrom<&str> for Timestamp {
    type Error = TimestampError;
    fn try_from(value: &str) -> std::result::Result<Self, Self::Error> {
        let odt = time::OffsetDateTime::parse(value, &Rfc3339)
            .map_err(|e| TimestampError::Deserialize(format!("{e}")))?;
        let nanos_since_epoch = odt.unix_timestamp_nanos();
        let seconds = (nanos_since_epoch / NS) as i64;
        let nanos = (nanos_since_epoch % NS) as i32;
        Timestamp::new(seconds, nanos)
    }
}

/// Converts from [chrono::DateTime] to [Timestamp].
///
/// This conversion may fail if the [chrono::DateTime] value is out of range.
#[cfg(feature = "chrono")]
impl TryFrom<chrono::DateTime<chrono::Utc>> for Timestamp {
    type Error = TimestampError;

    fn try_from(value: chrono::DateTime<chrono::Utc>) -> std::result::Result<Self, Self::Error> {
        assert!(value.timestamp_subsec_nanos() <= (i32::MAX as u32));
        Timestamp::new(value.timestamp(), value.timestamp_subsec_nanos() as i32)
    }
}

/// Converts from [Timestamp] to [chrono::DateTime].
#[cfg(feature = "chrono")]
impl TryFrom<Timestamp> for chrono::DateTime<chrono::Utc> {
    type Error = TimestampError;
    fn try_from(value: Timestamp) -> std::result::Result<Self, Self::Error> {
        let ts = chrono::DateTime::from_timestamp(value.seconds, 0).unwrap();
        Ok(ts + chrono::Duration::nanoseconds(value.nanos as i64))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::json;
    use test_case::test_case;
    type Result = std::result::Result<(), Box<dyn std::error::Error>>;

    // Verify the epoch converts as expected.
    #[test]
    fn unix_epoch() -> Result {
        let proto = Timestamp::default();
        let json = serde_json::to_value(&proto)?;
        let expected = json!("1970-01-01T00:00:00Z");
        assert_eq!(json, expected);
        let roundtrip = serde_json::from_value::<Timestamp>(json)?;
        assert_eq!(proto, roundtrip);
        Ok(())
    }

    fn get_seconds(input: &str) -> i64 {
        let odt = time::OffsetDateTime::parse(input, &Rfc3339);
        let odt = odt.unwrap();
        odt.unix_timestamp()
    }

    fn get_min_seconds() -> i64 {
        self::get_seconds("0001-01-01T00:00:00Z")
    }

    fn get_max_seconds() -> i64 {
        self::get_seconds("9999-12-31T23:59:59Z")
    }

    #[test_case(get_min_seconds() - 1, 0; "seconds below range")]
    #[test_case(get_max_seconds() + 1, 0; "seconds above range")]
    #[test_case(0, -1; "nanos below range")]
    #[test_case(0, 1_000_000_000; "nanos above range")]
    fn new_out_of_range(seconds: i64, nanos: i32) -> Result {
        let t = Timestamp::new(seconds, nanos);
        assert_eq!(t, Err(Error::OutOfRange()));
        Ok(())
    }

    #[test_case(0, 0, 0, 0; "zero")]
    #[test_case(0, 1_234_567_890, 1, 234_567_890; "nanos overflow")]
    #[test_case(0, -1_400_000_000, -2, 600_000_000; "nanos underflow")]
    #[test_case(self::get_max_seconds() + 1, 0, get_max_seconds(), 0; "seconds over range")]
    #[test_case(self::get_min_seconds() - 1, 0, get_min_seconds(), 0; "seconds below range")]
    #[test_case(self::get_max_seconds() - 1, 2_000_000_001, get_max_seconds(), 0; "nanos overflow range")]
    #[test_case(self::get_min_seconds() + 1, -1_500_000_000, get_min_seconds(), 0; "nanos underflow range")]
    fn clamp(seconds: i64, nanos: i32, want_seconds: i64, want_nanos: i32) {
        let got = Timestamp::clamp(seconds, nanos);
        let want = Timestamp {
            seconds: want_seconds,
            nanos: want_nanos,
        };
        assert_eq!(got, want);
    }

    // Verify timestamps can roundtrip from string -> struct -> string without loss.
    #[test_case("0001-01-01T00:00:00Z")]
    #[test_case("9999-12-31T23:59:59.999999999Z")]
    #[test_case("2024-10-19T12:34:56.789Z")]
    #[test_case("2024-10-19T12:34:56.789123456Z")]
    fn roundtrip(input: &str) -> Result {
        let json = serde_json::Value::String(input.to_string());
        let timestamp = serde_json::from_value::<Timestamp>(json)?;
        let roundtrip = serde_json::to_string(&timestamp)?;
        assert_eq!(
            format!("\"{input}\""),
            roundtrip,
            "mismatched value for input={input}"
        );
        Ok(())
    }

    #[test_case("0000-01-01T00:00:00Z"; "below range")]
    #[test_case("10000-01-01T00:00:00Z"; "above range")]
    fn deserialize_out_of_range(input: &str) -> Result {
        let value = serde_json::to_value(input)?;
        let got = serde_json::from_value::<Timestamp>(value);
        assert!(got.is_err());
        Ok(())
    }

    #[test]
    fn deserialize_unexpected_input_type() -> Result {
        let got = serde_json::from_value::<Timestamp>(serde_json::json!({}));
        assert!(got.is_err());
        let msg = format!("{got:?}");
        assert!(msg.contains("RFC 3339"), "message={}", msg);
        Ok(())
    }
}
