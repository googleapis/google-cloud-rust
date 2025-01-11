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

///
/// Well-known duration representation for Google APIs.
///
/// A Duration represents a signed, fixed-length span of time represented
/// as a count of seconds and fractions of seconds at nanosecond
/// resolution. It is independent of any calendar and concepts like "day"
/// or "month". It is related to [Timestamp](crate::Timestamp) in that the
/// difference between two Timestamp values is a Duration and it can be added
/// or subtracted from a Timestamp. Range is approximately +-10,000 years.
///
/// # JSON Mapping
///
/// In JSON format, the Duration type is encoded as a string rather than an
/// object, where the string ends in the suffix "s" (indicating seconds) and
/// is preceded by the number of seconds, with nanoseconds expressed as
/// fractional seconds. For example, 3 seconds with 0 nanoseconds should be
/// encoded in JSON format as "3s", while 3 seconds and 1 nanosecond should
/// be expressed in JSON format as "3.000000001s", and 3 seconds and 1
/// microsecond should be expressed in JSON format as "3.000001s".
#[derive(Clone, Debug, Default, PartialEq, PartialOrd)]
#[non_exhaustive]
pub struct Duration {
    /// Signed seconds of the span of time.
    ///
    /// Must be from -315,576,000,000 to +315,576,000,000 inclusive. Note: these
    /// bounds are computed from:
    ///     60 sec/min * 60 min/hr * 24 hr/day * 365.25 days/year * 10000 years
    seconds: i64,

    /// Signed fractions of a second at nanosecond resolution of the span
    /// of time.
    ///
    /// Durations less than one second are represented with a 0 `seconds` field
    /// and a positive or negative `nanos` field. For durations
    /// of one second or more, a non-zero value for the `nanos` field must be
    /// of the same sign as the `seconds` field. Must be from -999,999,999
    /// to +999,999,999 inclusive.
    nanos: i32,
}

/// Represent failures in converting or creating [Duration] instances.
#[derive(thiserror::Error, Debug, PartialEq)]
pub enum DurationError {
    /// One of the components (seconds and/or nanoseconds) was out of range.
    #[error("seconds and/or nanoseconds out of range")]
    OutOfRange(),

    /// The sign of the seconds component does not match the sign of the nanoseconds component.
    #[error("if seconds and nanoseconds are not zero, they must have the same sign")]
    MismatchedSigns(),

    /// Cannot serialize the duration.
    #[error("cannot serialize the duration")]
    Serializate(),

    /// Cannot deserialize the duration.
    #[error("cannot deserialize the duration: {0:?}")]
    Deserialize(String),
}

type Error = DurationError;

impl Duration {
    const NS: i32 = 1_000_000_000;

    /// The maximum value for the `seconds` component, approximately 10,000 years.
    pub const MAX_SECONDS: i64 = 315_576_000_000;

    /// The minimum value for the `seconds` component, approximately -10,000 years.
    pub const MIN_SECONDS: i64 = -Self::MAX_SECONDS;

    /// The maximum value for the `nanos` component.
    pub const MAX_NANOS: i32 = Self::NS - 1;

    /// The minimum value for the `nanos` component.
    pub const MIN_NANOS: i32 = -Self::MAX_NANOS;

    /// Creates a [Duration] from the seconds and nanoseconds component.
    ///
    /// This function validates the `seconds` and `nanos` components and returns
    /// an error if either are out of range or their signs do not match.
    /// Consider using [clamp()][Duration::clamp] to add nanoseconds to seconds
    /// with carry.
    ///
    /// # Arguments
    ///
    /// * `seconds` - the seconds in the interval.
    /// * `nanos` - the nanoseconds *added* to the interval.
    pub fn new(seconds: i64, nanos: i32) -> Result<Self, Error> {
        if !(Self::MIN_SECONDS..=Self::MAX_SECONDS).contains(&seconds) {
            return Err(Error::OutOfRange());
        }
        if !(Self::MIN_NANOS..=Self::MAX_NANOS).contains(&nanos) {
            return Err(Error::OutOfRange());
        }
        if (seconds != 0 && nanos != 0) && ((seconds < 0) != (nanos < 0)) {
            return Err(Error::MismatchedSigns());
        }
        Ok(Self { seconds, nanos })
    }

    /// Create a normalized, clamped [Duration].
    ///
    /// Durations must be in the [-10_000, +10_000] year range, the nanoseconds
    /// field must be in the [-999_999_999, +999_999_999] range, and the seconds
    /// and nanosecond fields must have the same sign. This function creates a
    /// new [Duration] instance clamped to those ranges.
    ///
    /// The function effectively adds the nanoseconds part (with carry) to the
    /// seconds part, with saturation.
    ///
    /// # Arguments
    ///
    /// * `seconds` - the seconds in the interval.
    /// * `nanos` - the nanoseconds *added* to the interval.
    pub fn clamp(seconds: i64, nanos: i32) -> Self {
        let mut seconds = seconds;
        seconds = seconds.saturating_add((nanos / Self::NS) as i64);
        let mut nanos = nanos % Self::NS;
        if seconds > 0 && nanos < 0 {
            seconds = seconds.saturating_sub(1);
            nanos += Self::NS;
        } else if seconds < 0 && nanos > 0 {
            seconds = seconds.saturating_add(1);
            nanos = -(Self::NS - nanos);
        }
        if seconds > Self::MAX_SECONDS {
            return Self {
                seconds: Self::MAX_SECONDS,
                nanos: 0,
            };
        }
        if seconds < Self::MIN_SECONDS {
            return Self {
                seconds: Self::MIN_SECONDS,
                nanos: 0,
            };
        }
        Self { seconds, nanos }
    }

    /// Returns the seconds part of the duration.
    pub fn seconds(&self) -> i64 {
        self.seconds
    }

    /// Returns the sub-second part of the duration.
    pub fn nanos(&self) -> i32 {
        self.nanos
    }
}

impl crate::message::Message for Duration {
    fn typename() -> &'static str {
        "type.googleapis.com/google.protobuf.Duration"
    }
}

/// Converts a [Duration] to its [String] representation.
impl std::convert::From<&Duration> for String {
    fn from(duration: &Duration) -> String {
        let sign = if duration.seconds < 0 || duration.nanos < 0 {
            "-"
        } else {
            ""
        };
        if duration.nanos == 0 {
            return format!("{sign}{}s", duration.seconds.abs());
        }
        if duration.seconds == 0 {
            let ns = format!("{:09}", duration.nanos.abs());
            return format!("{sign}0.{}s", ns.trim_end_matches('0'));
        }
        format!(
            "{sign}{}.{:09}s",
            duration.seconds.abs(),
            duration.nanos.abs()
        )
    }
}

/// Converts the [String] representation of a duration to [Duration].
impl std::convert::TryFrom<&str> for Duration {
    type Error = DurationError;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if !value.ends_with('s') {
            return Err(DurationError::Deserialize("missing trailing 's'".into()));
        }
        let digits = &value[..(value.len() - 1)];
        let (sign, digits) = if let Some(stripped) = digits.strip_prefix('-') {
            (-1, stripped)
        } else {
            (1, &digits[0..])
        };
        let mut split = digits.splitn(2, '.');
        let (seconds, nanos) = (split.next(), split.next());
        let seconds = seconds
            .map(str::parse::<i64>)
            .transpose()
            .map_err(|e| DurationError::Deserialize(format!("{e}")))?
            .unwrap_or(0);
        let nanos = nanos
            .map(|s| {
                let pad = "000000000";
                format!("{s}{}", &pad[s.len()..])
            })
            .map(|s| s.parse::<i32>())
            .transpose()
            .map_err(|e| DurationError::Deserialize(format!("{e}")))?
            .unwrap_or(0);

        Duration::new(sign * seconds, sign as i32 * nanos)
    }
}

/// Convert from [std::time::Duration] to [Duration].
impl std::convert::TryFrom<std::time::Duration> for Duration {
    type Error = DurationError;

    fn try_from(value: std::time::Duration) -> Result<Self, Self::Error> {
        if value.as_secs() > (i64::MAX as u64) {
            return Err(Error::OutOfRange());
        }
        assert!(value.subsec_nanos() <= (i32::MAX as u32));
        Self::new(value.as_secs() as i64, value.subsec_nanos() as i32)
    }
}

/// Convert from [Duration] to [std::time::Duration].
impl std::convert::TryFrom<Duration> for std::time::Duration {
    type Error = DurationError;

    fn try_from(value: Duration) -> Result<Self, Self::Error> {
        if value.seconds < 0 {
            return Err(Error::OutOfRange());
        }
        if value.nanos < 0 {
            return Err(Error::OutOfRange());
        }
        Ok(Self::new(value.seconds as u64, value.nanos as u32))
    }
}

/// Convert from [time::Duration] to [Duration].
///
/// This conversion may fail if the [time::Duration] value is out of range.
#[cfg(feature = "time")]
impl std::convert::TryFrom<time::Duration> for Duration {
    type Error = DurationError;

    fn try_from(value: time::Duration) -> Result<Self, Self::Error> {
        Self::new(value.whole_seconds(), value.subsec_nanoseconds())
    }
}

/// Convert from [Duration] to [time::Duration].
///
/// This conversion is always safe because the range for [Duration] is
/// guaranteed to fit into the destination type.
#[cfg(feature = "time")]
impl std::convert::From<Duration> for time::Duration {
    fn from(value: Duration) -> Self {
        Self::new(value.seconds(), value.nanos())
    }
}

/// Converts from [chrono::Duration] to [Duration].
#[cfg(feature = "chrono")]
impl std::convert::TryFrom<chrono::Duration> for Duration {
    type Error = DurationError;

    fn try_from(value: chrono::Duration) -> Result<Self, Self::Error> {
        Self::new(value.num_seconds(), value.subsec_nanos())
    }
}

/// Converts from [Duration] to [chrono::Duration].
#[cfg(feature = "chrono")]
impl std::convert::From<Duration> for chrono::Duration {
    fn from(value: Duration) -> Self {
        Self::seconds(value.seconds) + Self::nanoseconds(value.nanos as i64)
    }
}

/// Implement [`serde`](::serde) serialization for [Duration].
impl serde::ser::Serialize for Duration {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        let formatted = String::from(self);
        formatted.serialize(serializer)
    }
}

struct DurationVisitor;

impl serde::de::Visitor<'_> for DurationVisitor {
    type Value = Duration;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a string with a duration in Google format ([sign]{seconds}.{nanos}s)")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let d = Duration::try_from(value).map_err(E::custom)?;
        Ok(d)
    }
}

/// Implement [`serde`](::serde) deserialization for [`Duration`].
impl<'de> serde::de::Deserialize<'de> for Duration {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(DurationVisitor)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::json;
    use test_case::test_case;
    type Result = std::result::Result<(), Box<dyn std::error::Error>>;

    // Verify 0 converts as expected.
    #[test]
    fn zero() -> Result {
        let proto = Duration {
            seconds: 0,
            nanos: 0,
        };
        let json = serde_json::to_value(&proto)?;
        let expected = json!(r#"0s"#);
        assert_eq!(json, expected);
        let roundtrip = serde_json::from_value::<Duration>(json)?;
        assert_eq!(proto, roundtrip);
        Ok(())
    }

    // Google assumes all minutes have 60 seconds. Leap seconds are handled via
    // smearing.
    const SECONDS_IN_DAY: i64 = 24 * 60 * 60;
    // For the purposes of this Duration type, Google ignores the subtleties of
    // leap years on multiples of 100 and 400.
    const SECONDS_IN_YEAR: i64 = 365 * SECONDS_IN_DAY + SECONDS_IN_DAY / 4;

    #[test_case(10_000 * SECONDS_IN_YEAR , 0 ; "exactly 10,000 years")]
    #[test_case(- 10_000 * SECONDS_IN_YEAR , 0 ; "exactly negative 10,000 years")]
    #[test_case(10_000 * SECONDS_IN_YEAR , 999_999_999 ; "exactly 10,000 years and 999,999,999 nanos")]
    #[test_case(- 10_000 * SECONDS_IN_YEAR , -999_999_999 ; "exactly negative 10,000 years and 999,999,999 nanos")]
    #[test_case(0, 999_999_999 ; "exactly 999,999,999 nanos")]
    #[test_case(0 , -999_999_999 ; "exactly negative 999,999,999 nanos")]
    fn edge_of_range(seconds: i64, nanos: i32) -> Result {
        let d = Duration::new(seconds, nanos)?;
        assert_eq!(seconds, d.seconds());
        assert_eq!(nanos, d.nanos());
        Ok(())
    }

    #[test_case(10_000 * SECONDS_IN_YEAR + 1, 0 ; "more seconds than in 10,000 years")]
    #[test_case(- 10_000 * SECONDS_IN_YEAR - 1, 0 ; "more negative seconds than in -10,000 years")]
    #[test_case(0, 1_000_000_000 ; "too many positive nanoseconds")]
    #[test_case(0, -1_000_000_000 ; "too many negative nanoseconds")]
    fn out_of_range(seconds: i64, nanos: i32) -> Result {
        let d = Duration::new(seconds, nanos);
        assert_eq!(d, Err(Error::OutOfRange()));
        Ok(())
    }

    #[test_case(1 , -1 ; "mismatched sign case 1")]
    #[test_case(-1 , 1 ; "mismatched sign case 2")]
    fn mismatched_sign(seconds: i64, nanos: i32) -> Result {
        let d = Duration::new(seconds, nanos);
        assert_eq!(d, Err(Error::MismatchedSigns()));
        Ok(())
    }

    #[test_case(20_000 * SECONDS_IN_YEAR, 0, 10_000 * SECONDS_IN_YEAR, 0 ; "too many positive seconds")]
    #[test_case(-20_000 * SECONDS_IN_YEAR, 0, -10_000 * SECONDS_IN_YEAR, 0 ; "too many negative seconds")]
    #[test_case(10_000 * SECONDS_IN_YEAR - 1, 1_999_999_999, 10_000 * SECONDS_IN_YEAR, 999_999_999 ; "upper edge of range")]
    #[test_case(-10_000 * SECONDS_IN_YEAR + 1, -1_999_999_999, -10_000 * SECONDS_IN_YEAR, -999_999_999 ; "lower edge of range")]
    #[test_case(10_000 * SECONDS_IN_YEAR - 1 , 2 * 1_000_000_000_i32, 10_000 * SECONDS_IN_YEAR, 0 ; "nanos push over 10,000 years")]
    #[test_case(-10_000 * SECONDS_IN_YEAR + 1, -2 * 1_000_000_000_i32, -10_000 * SECONDS_IN_YEAR, 0 ; "one push under -10,000 years")]
    #[test_case(0, 0, 0, 0 ; "all inputs are zero")]
    #[test_case(1, 0, 1, 0 ; "positive seconds and zero nanos")]
    #[test_case(1, 200_000, 1, 200_000 ; "positive seconds and nanos")]
    #[test_case(-1, 0, -1, 0; "negative seconds and zero nanos")]
    #[test_case(-1, -500_000_000, -1, -500_000_000; "negative seconds and nanos")]
    #[test_case(2, -400_000_000, 1, 600_000_000; "positive seconds and negative nanos")]
    #[test_case(-2, 400_000_000, -1, -600_000_000; "negative seconds and positive nanos")]
    fn clamp(seconds: i64, nanos: i32, want_seconds: i64, want_nanos: i32) -> Result {
        let got = Duration::clamp(seconds, nanos);
        let want = Duration {
            seconds: want_seconds,
            nanos: want_nanos,
        };
        assert_eq!(want, got);
        Ok(())
    }

    // Verify durations can roundtrip from string -> struct -> string without loss.
    #[test_case(0, 0, "0s" ; "zero")]
    #[test_case(0, 2, "0.000000002s" ; "2ns")]
    #[test_case(0, 200_000_000, "0.2s" ; "200ms")]
    #[test_case(12, 0, "12s"; "round positive seconds")]
    #[test_case(12, 123, "12.000000123s"; "positive seconds and nanos")]
    #[test_case(12, 123_000, "12.000123000s"; "positive seconds and micros")]
    #[test_case(12, 123_000_000, "12.123000000s"; "positive seconds and millis")]
    #[test_case(12, 123_456_789, "12.123456789s"; "positive seconds and full nanos")]
    #[test_case(-12, -0, "-12s"; "round negative seconds")]
    #[test_case(-12, -123, "-12.000000123s"; "negative seconds and nanos")]
    #[test_case(-12, -123_000, "-12.000123000s"; "negative seconds and micros")]
    #[test_case(-12, -123_000_000, "-12.123000000s"; "negative seconds and millis")]
    #[test_case(-12, -123_456_789, "-12.123456789s"; "negative seconds and full nanos")]
    #[test_case(-10_000 * SECONDS_IN_YEAR, -999_999_999, "-315576000000.999999999s"; "range edge start")]
    #[test_case(10_000 * SECONDS_IN_YEAR, 999_999_999, "315576000000.999999999s"; "range edge end")]
    fn roundtrip(seconds: i64, nanos: i32, want: &str) -> Result {
        let input = Duration::new(seconds, nanos)?;
        let got = serde_json::to_value(&input)?
            .as_str()
            .map(str::to_string)
            .ok_or("cannot convert value to string")?;
        assert_eq!(want, got);

        let rt = serde_json::from_value::<Duration>(serde_json::Value::String(got))?;
        assert_eq!(input, rt);
        Ok(())
    }

    #[test_case("-315576000001s"; "range edge start")]
    #[test_case("315576000001s"; "range edge end")]
    fn deserialize_out_of_range(input: &str) -> Result {
        let value = serde_json::to_value(input)?;
        let got = serde_json::from_value::<Duration>(value);
        assert!(got.is_err());
        Ok(())
    }

    #[test_case(time::Duration::default(), Duration::default() ; "default")]
    #[test_case(time::Duration::new(0, 0), Duration::new(0, 0).unwrap() ; "zero")]
    #[test_case(time::Duration::new(10_000 * SECONDS_IN_YEAR , 0), Duration::new(10_000 * SECONDS_IN_YEAR, 0).unwrap() ; "exactly 10,000 years")]
    #[test_case(time::Duration::new(-10_000 * SECONDS_IN_YEAR , 0), Duration::new(-10_000 * SECONDS_IN_YEAR, 0).unwrap() ; "exactly negative 10,000 years")]
    fn from_time_in_range(value: time::Duration, want: Duration) -> Result {
        let got = Duration::try_from(value)?;
        assert_eq!(got, want);
        Ok(())
    }

    #[test_case(time::Duration::new(10_001 * SECONDS_IN_YEAR, 0) ; "above the range")]
    #[test_case(time::Duration::new(-10_001 * SECONDS_IN_YEAR, 0) ; "below the range")]
    fn from_time_out_of_range(value: time::Duration) {
        let got = Duration::try_from(value);
        assert_eq!(got, Err(DurationError::OutOfRange()));
    }

    #[test_case(Duration::default(), time::Duration::default() ; "default")]
    #[test_case(Duration::new(0, 0).unwrap(), time::Duration::new(0, 0) ; "zero")]
    #[test_case(Duration::new(10_000 * SECONDS_IN_YEAR , 0).unwrap(), time::Duration::new(10_000 * SECONDS_IN_YEAR, 0) ; "exactly 10,000 years")]
    #[test_case(Duration::new(-10_000 * SECONDS_IN_YEAR , 0).unwrap(), time::Duration::new(-10_000 * SECONDS_IN_YEAR, 0) ; "exactly negative 10,000 years")]
    fn to_time_in_range(value: Duration, want: time::Duration) -> Result {
        let got = time::Duration::from(value);
        assert_eq!(got, want);
        Ok(())
    }

    #[test_case("" ; "empty")]
    #[test_case("1.0" ; "missing final s")]
    #[test_case("1.2.3.4s" ; "too many periods")]
    #[test_case("aaas" ; "not a number")]
    #[test_case("aaaa.0s" ; "seconds are not a number [aaa]")]
    #[test_case("1a.0s" ; "seconds are not a number [1a]")]
    #[test_case("1.aaas" ; "nanos are not a number [aaa]")]
    #[test_case("1.0as" ; "nanos are not a number [0a]")]
    fn parse_detect_bad_input(input: &str) -> Result {
        let got = Duration::try_from(input);
        assert!(got.is_err());
        let err = got.err().unwrap();
        match err {
            DurationError::Deserialize(_) => {
                assert!(true)
            }
            _ => assert!(false, "unexpected error {err:?}"),
        };
        Ok(())
    }

    #[test]
    fn deserialize_unexpected_input_type() -> Result {
        let got = serde_json::from_value::<Duration>(serde_json::json!({}));
        assert!(got.is_err());
        let msg = format!("{got:?}");
        assert!(msg.contains("duration in Google format"), "message={}", msg);
        Ok(())
    }

    #[test_case(chrono::Duration::default(), Duration::default() ; "default")]
    #[test_case(chrono::Duration::new(0, 0).unwrap(), Duration::new(0, 0).unwrap() ; "zero")]
    #[test_case(chrono::Duration::new(10_000 * SECONDS_IN_YEAR, 0).unwrap(), Duration::new(10_000 * SECONDS_IN_YEAR, 0).unwrap() ; "exactly 10,000 years")]
    #[test_case(chrono::Duration::new(-10_000 * SECONDS_IN_YEAR, 0).unwrap(), Duration::new(-10_000 * SECONDS_IN_YEAR, 0).unwrap() ; "exactly negative 10,000 years")]
    fn from_chrono_time_in_range(value: chrono::Duration, want: Duration) -> Result {
        let got = Duration::try_from(value)?;
        assert_eq!(got, want);
        Ok(())
    }
    #[test_case(Duration::default(), chrono::Duration::default() ; "default")]
    #[test_case(Duration::new(0, 0).unwrap(), chrono::Duration::new(0, 0).unwrap() ; "zero")]
    #[test_case(Duration::new(10_000 * SECONDS_IN_YEAR , 0).unwrap(), chrono::Duration::new(10_000 * SECONDS_IN_YEAR, 0).unwrap() ; "exactly 10,000 years")]
    #[test_case(Duration::new(-10_000 * SECONDS_IN_YEAR , 0).unwrap(), chrono::Duration::new(-10_000 * SECONDS_IN_YEAR, 0).unwrap() ; "exactly negative 10,000 years")]
    fn to_chrono_time_in_range(value: Duration, want: chrono::Duration) -> Result {
        let got = chrono::Duration::from(value);
        assert_eq!(got, want);
        Ok(())
    }

    #[test_case(chrono::Duration::new(10_001 * SECONDS_IN_YEAR, 0).unwrap() ; "above the range")]
    #[test_case(chrono::Duration::new(-10_001 * SECONDS_IN_YEAR, 0).unwrap() ; "below the range")]
    fn from_chrono_time_out_of_range(value: chrono::Duration) {
        let got = Duration::try_from(value);
        assert_eq!(got, Err(DurationError::OutOfRange()));
    }
}
