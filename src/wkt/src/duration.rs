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

/// Well-known duration representation for Google APIs.
///
/// # Examples
/// ```
/// # use google_cloud_wkt::{Duration, DurationError};
/// let d = Duration::try_from("12.34s")?;
/// assert_eq!(d.seconds(), 12);
/// assert_eq!(d.nanos(), 340_000_000);
/// assert_eq!(d, Duration::new(12, 340_000_000)?);
/// assert_eq!(d, Duration::clamp(12, 340_000_000));
///
/// # Ok::<(), DurationError>(())
/// ```
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
#[derive(Clone, Copy, Debug, Default, PartialEq, PartialOrd)]
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
///
/// # Examples
/// ```
/// # use google_cloud_wkt::{Duration, DurationError};
/// let duration = Duration::new(Duration::MAX_SECONDS + 2, 0);
/// assert!(matches!(duration, Err(DurationError::OutOfRange)));
///
/// let duration = Duration::new(0, 1_500_000_000);
/// assert!(matches!(duration, Err(DurationError::OutOfRange)));
///
/// let duration = Duration::new(120, -500_000_000);
/// assert!(matches!(duration, Err(DurationError::MismatchedSigns)));
///
/// let ts = Duration::try_from("invalid");
/// assert!(matches!(ts, Err(DurationError::Deserialize(_))));
/// ```
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum DurationError {
    /// One of the components (seconds and/or nanoseconds) was out of range.
    #[error("seconds and/or nanoseconds out of range")]
    OutOfRange,

    /// The sign of the seconds component does not match the sign of the nanoseconds component.
    #[error("if seconds and nanoseconds are not zero, they must have the same sign")]
    MismatchedSigns,

    /// Cannot deserialize the duration.
    #[error("cannot deserialize the duration: {0}")]
    Deserialize(#[source] BoxedError),
}

type BoxedError = Box<dyn std::error::Error + Send + Sync>;
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
    /// # Examples
    /// ```
    /// # use google_cloud_wkt::{Duration, DurationError};
    /// let d = Duration::new(12, 340_000_000)?;
    /// assert_eq!(String::from(d), "12.34s");
    ///
    /// let d = Duration::new(-12, -340_000_000)?;
    /// assert_eq!(String::from(d), "-12.34s");
    /// # Ok::<(), DurationError>(())
    /// ```
    ///
    /// # Examples: invalid inputs
    /// ```
    /// # use google_cloud_wkt::{Duration, DurationError};
    /// let d = Duration::new(12, 2_000_000_000);
    /// assert!(matches!(d, Err(DurationError::OutOfRange)));
    ///
    /// let d = Duration::new(-12, 340_000_000);
    /// assert!(matches!(d, Err(DurationError::MismatchedSigns)));
    /// # Ok::<(), DurationError>(())
    /// ```
    ///
    /// This function validates the `seconds` and `nanos` components and returns
    /// an error if either are out of range or their signs do not match.
    /// Consider using [clamp()][Duration::clamp] to add nanoseconds to seconds
    /// with carry.
    ///
    /// # Parameters
    ///
    /// * `seconds` - the seconds in the interval.
    /// * `nanos` - the nanoseconds *added* to the interval.
    pub fn new(seconds: i64, nanos: i32) -> Result<Self, Error> {
        if !(Self::MIN_SECONDS..=Self::MAX_SECONDS).contains(&seconds) {
            return Err(Error::OutOfRange);
        }
        if !(Self::MIN_NANOS..=Self::MAX_NANOS).contains(&nanos) {
            return Err(Error::OutOfRange);
        }
        if (seconds != 0 && nanos != 0) && ((seconds < 0) != (nanos < 0)) {
            return Err(Error::MismatchedSigns);
        }
        Ok(Self { seconds, nanos })
    }

    /// Create a normalized, clamped [Duration].
    ///
    /// # Examples
    /// ```
    /// # use google_cloud_wkt::{Duration, DurationError};
    /// let d = Duration::clamp(12, 340_000_000);
    /// assert_eq!(String::from(d), "12.34s");
    /// let d = Duration::clamp(10, 2_000_000_000);
    /// assert_eq!(String::from(d), "12s");
    /// # Ok::<(), DurationError>(())
    /// ```
    ///
    /// Durations must be in the [-10_000, +10_000] year range, the nanoseconds
    /// field must be in the [-999_999_999, +999_999_999] range, and the seconds
    /// and nanosecond fields must have the same sign. This function creates a
    /// new [Duration] instance clamped to those ranges.
    ///
    /// The function effectively adds the nanoseconds part (with carry) to the
    /// seconds part, with saturation.
    ///
    /// # Parameters
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
    ///
    /// # Example
    /// ```
    /// # use google_cloud_wkt::Duration;
    /// let d = Duration::clamp(12, 34);
    /// assert_eq!(d.seconds(), 12);
    /// ```
    pub fn seconds(&self) -> i64 {
        self.seconds
    }

    /// Returns the sub-second part of the duration.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_wkt::Duration;
    /// let d = Duration::clamp(12, 34);
    /// assert_eq!(d.nanos(), 34);
    /// ```
    pub fn nanos(&self) -> i32 {
        self.nanos
    }
}

impl crate::message::Message for Duration {
    fn typename() -> &'static str {
        "type.googleapis.com/google.protobuf.Duration"
    }

    #[allow(private_interfaces)]
    fn serializer() -> impl crate::message::MessageSerializer<Self> {
        crate::message::ValueSerializer::<Self>::new()
    }
}

/// Converts a [Duration] to its [String] representation.
///
/// # Example
/// ```
/// # use google_cloud_wkt::Duration;
/// let d = Duration::clamp(12, 340_000_000);
/// assert_eq!(String::from(d), "12.34s");
/// ```
impl From<Duration> for String {
    fn from(duration: Duration) -> String {
        let sign = if duration.seconds < 0 || duration.nanos < 0 {
            "-"
        } else {
            ""
        };
        if duration.nanos == 0 {
            return format!("{sign}{}s", duration.seconds.abs());
        }
        let ns = format!("{:09}", duration.nanos.abs());
        format!(
            "{sign}{}.{}s",
            duration.seconds.abs(),
            ns.trim_end_matches('0')
        )
    }
}

/// Converts the string representation of a duration to [Duration].
///
/// # Example
/// ```
/// # use google_cloud_wkt::{Duration, DurationError};
/// let d = Duration::try_from("12.34s")?;
/// assert_eq!(d.seconds(), 12);
/// assert_eq!(d.nanos(), 340_000_000);
/// # Ok::<(), DurationError>(())
/// ```
impl TryFrom<&str> for Duration {
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
            .map_err(|e| DurationError::Deserialize(e.into()))?
            .unwrap_or(0);
        let nanos = nanos
            .map(|s| {
                let pad = "000000000";
                format!("{s}{}", &pad[s.len()..])
            })
            .map(|s| s.parse::<i32>())
            .transpose()
            .map_err(|e| DurationError::Deserialize(e.into()))?
            .unwrap_or(0);

        Duration::new(sign * seconds, sign as i32 * nanos)
    }
}

/// Converts the string representation of a duration to [Duration].
///
/// # Example
/// ```
/// # use google_cloud_wkt::{Duration, DurationError};
/// let s = "12.34s".to_string();
/// let d = Duration::try_from(&s)?;
/// assert_eq!(d.seconds(), 12);
/// assert_eq!(d.nanos(), 340_000_000);
/// # Ok::<(), DurationError>(())
/// ```
impl TryFrom<&String> for Duration {
    type Error = DurationError;
    fn try_from(value: &String) -> Result<Self, Self::Error> {
        Duration::try_from(value.as_str())
    }
}

/// Convert from [std::time::Duration] to [Duration].
///
/// # Example
/// ```
/// # use google_cloud_wkt::{Duration, DurationError};
/// let d = Duration::try_from(std::time::Duration::from_secs(123))?;
/// assert_eq!(d.seconds(), 123);
/// assert_eq!(d.nanos(), 0);
/// # Ok::<(), DurationError>(())
/// ```
impl TryFrom<std::time::Duration> for Duration {
    type Error = DurationError;

    fn try_from(value: std::time::Duration) -> Result<Self, Self::Error> {
        if value.as_secs() > (i64::MAX as u64) {
            return Err(Error::OutOfRange);
        }
        assert!(value.as_secs() <= (i64::MAX as u64));
        assert!(value.subsec_nanos() <= (i32::MAX as u32));
        Self::new(value.as_secs() as i64, value.subsec_nanos() as i32)
    }
}

/// Convert from [Duration] to [std::time::Duration].
///
/// Returns an error if `value` is negative, as `std::time::Duration` cannot
/// represent negative durations.
///
/// # Example
/// ```
/// # use google_cloud_wkt::{Duration, DurationError};
/// let d = Duration::new(12, 340_000_000)?;
/// let duration = std::time::Duration::try_from(d)?;
/// assert_eq!(duration.as_secs(), 12);
/// assert_eq!(duration.subsec_nanos(), 340_000_000);
/// # Ok::<(), DurationError>(())
/// ```
impl TryFrom<Duration> for std::time::Duration {
    type Error = DurationError;

    fn try_from(value: Duration) -> Result<Self, Self::Error> {
        if value.seconds < 0 {
            return Err(Error::OutOfRange);
        }
        if value.nanos < 0 {
            return Err(Error::OutOfRange);
        }
        Ok(Self::new(value.seconds as u64, value.nanos as u32))
    }
}

/// Convert from [time::Duration] to [Duration].
///
/// This conversion may fail if the [time::Duration] value is out of range.
///
/// # Example
/// ```
/// # use google_cloud_wkt::{Duration, DurationError};
/// let d = Duration::try_from(time::Duration::new(12, 340_000_000))?;
/// assert_eq!(d.seconds(), 12);
/// assert_eq!(d.nanos(), 340_000_000);
/// # Ok::<(), DurationError>(())
/// ```
#[cfg(feature = "time")]
#[cfg_attr(docsrs, doc(cfg(feature = "time")))]
impl TryFrom<time::Duration> for Duration {
    type Error = DurationError;

    fn try_from(value: time::Duration) -> Result<Self, Self::Error> {
        Self::new(value.whole_seconds(), value.subsec_nanoseconds())
    }
}

/// Convert from [Duration] to [time::Duration].
///
/// This conversion is always safe because the range for [Duration] is
/// guaranteed to fit into the destination type.
///
/// # Example
/// ```
/// # use google_cloud_wkt::{Duration, DurationError};
/// let d = time::Duration::from(Duration::clamp(12, 340_000_000));
/// assert_eq!(d.whole_seconds(), 12);
/// assert_eq!(d.subsec_nanoseconds(), 340_000_000);
/// # Ok::<(), DurationError>(())
/// ```
#[cfg(feature = "time")]
#[cfg_attr(docsrs, doc(cfg(feature = "time")))]
impl From<Duration> for time::Duration {
    fn from(value: Duration) -> Self {
        Self::new(value.seconds(), value.nanos())
    }
}

/// Converts from [chrono::Duration] to [Duration].
///
/// The conversion may fail if the input value is out of range.
///
/// # Example
/// ```
/// # use google_cloud_wkt::{Duration, DurationError};
/// let d = Duration::try_from(chrono::Duration::new(12, 340_000_000).unwrap())?;
/// assert_eq!(d.seconds(), 12);
/// assert_eq!(d.nanos(), 340_000_000);
/// # Ok::<(), DurationError>(())
/// ```
#[cfg(feature = "chrono")]
#[cfg_attr(docsrs, doc(cfg(feature = "chrono")))]
impl TryFrom<chrono::Duration> for Duration {
    type Error = DurationError;

    fn try_from(value: chrono::Duration) -> Result<Self, Self::Error> {
        Self::new(value.num_seconds(), value.subsec_nanos())
    }
}

/// Converts from [Duration] to [chrono::Duration].
///
/// # Example
/// ```
/// # use google_cloud_wkt::{Duration, DurationError};
/// let d = chrono::Duration::from(Duration::clamp(12, 340_000_000));
/// assert_eq!(d.num_seconds(), 12);
/// assert_eq!(d.subsec_nanos(), 340_000_000);
/// # Ok::<(), DurationError>(())
/// ```
#[cfg(feature = "chrono")]
#[cfg_attr(docsrs, doc(cfg(feature = "chrono")))]
impl From<Duration> for chrono::Duration {
    fn from(value: Duration) -> Self {
        Self::seconds(value.seconds) + Self::nanoseconds(value.nanos as i64)
    }
}

/// Implement [`serde`](::serde) serialization for [Duration].
#[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
impl serde::ser::Serialize for Duration {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        let formatted = String::from(*self);
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
#[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
impl<'de> serde::de::Deserialize<'de> for Duration {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(DurationVisitor)
    }
}

#[cfg(test)]
mod tests {
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
        let json = serde_json::to_value(proto)?;
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
    #[test_case(10_000 * SECONDS_IN_YEAR , 999_999_999 ; "exactly 10,000 years and 999,999,999 nanos"
	)]
    #[test_case(- 10_000 * SECONDS_IN_YEAR , -999_999_999 ; "exactly negative 10,000 years and 999,999,999 nanos"
	)]
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
        assert!(matches!(d, Err(Error::OutOfRange)), "{d:?}");
        Ok(())
    }

    #[test_case(1 , -1 ; "mismatched sign case 1")]
    #[test_case(-1 , 1 ; "mismatched sign case 2")]
    fn mismatched_sign(seconds: i64, nanos: i32) -> Result {
        let d = Duration::new(seconds, nanos);
        assert!(matches!(d, Err(Error::MismatchedSigns)), "{d:?}");
        Ok(())
    }

    #[test_case(20_000 * SECONDS_IN_YEAR, 0, 10_000 * SECONDS_IN_YEAR, 0 ; "too many positive seconds"
	)]
    #[test_case(-20_000 * SECONDS_IN_YEAR, 0, -10_000 * SECONDS_IN_YEAR, 0 ; "too many negative seconds"
	)]
    #[test_case(10_000 * SECONDS_IN_YEAR - 1, 1_999_999_999, 10_000 * SECONDS_IN_YEAR, 999_999_999 ; "upper edge of range"
	)]
    #[test_case(-10_000 * SECONDS_IN_YEAR + 1, -1_999_999_999, -10_000 * SECONDS_IN_YEAR, -999_999_999 ; "lower edge of range"
	)]
    #[test_case(10_000 * SECONDS_IN_YEAR - 1 , 2 * 1_000_000_000_i32, 10_000 * SECONDS_IN_YEAR, 0 ; "nanos push over 10,000 years"
	)]
    #[test_case(-10_000 * SECONDS_IN_YEAR + 1, -2 * 1_000_000_000_i32, -10_000 * SECONDS_IN_YEAR, 0 ; "one push under -10,000 years"
	)]
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
    #[test_case(12, 123_000, "12.000123s"; "positive seconds and micros")]
    #[test_case(12, 123_000_000, "12.123s"; "positive seconds and millis")]
    #[test_case(12, 123_456_789, "12.123456789s"; "positive seconds and full nanos")]
    #[test_case(-12, -0, "-12s"; "round negative seconds")]
    #[test_case(-12, -123, "-12.000000123s"; "negative seconds and nanos")]
    #[test_case(-12, -123_000, "-12.000123s"; "negative seconds and micros")]
    #[test_case(-12, -123_000_000, "-12.123s"; "negative seconds and millis")]
    #[test_case(-12, -123_456_789, "-12.123456789s"; "negative seconds and full nanos")]
    #[test_case(-10_000 * SECONDS_IN_YEAR, -999_999_999, "-315576000000.999999999s"; "range edge start"
	)]
    #[test_case(10_000 * SECONDS_IN_YEAR, 999_999_999, "315576000000.999999999s"; "range edge end")]
    fn roundtrip(seconds: i64, nanos: i32, want: &str) -> Result {
        let input = Duration::new(seconds, nanos)?;
        let got = serde_json::to_value(input)?
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
    #[test_case(time::Duration::new(10_000 * SECONDS_IN_YEAR , 0), Duration::new(10_000 * SECONDS_IN_YEAR, 0).unwrap() ; "exactly 10,000 years"
	)]
    #[test_case(time::Duration::new(-10_000 * SECONDS_IN_YEAR , 0), Duration::new(-10_000 * SECONDS_IN_YEAR, 0).unwrap() ; "exactly negative 10,000 years"
	)]
    fn from_time_in_range(value: time::Duration, want: Duration) -> Result {
        let got = Duration::try_from(value)?;
        assert_eq!(got, want);
        Ok(())
    }

    #[test_case(time::Duration::new(10_001 * SECONDS_IN_YEAR, 0) ; "above the range")]
    #[test_case(time::Duration::new(-10_001 * SECONDS_IN_YEAR, 0) ; "below the range")]
    fn from_time_out_of_range(value: time::Duration) {
        let got = Duration::try_from(value);
        assert!(matches!(got, Err(DurationError::OutOfRange)), "{got:?}");
    }

    #[test_case(Duration::default(), time::Duration::default() ; "default")]
    #[test_case(Duration::new(0, 0).unwrap(), time::Duration::new(0, 0) ; "zero")]
    #[test_case(Duration::new(10_000 * SECONDS_IN_YEAR , 0).unwrap(), time::Duration::new(10_000 * SECONDS_IN_YEAR, 0) ; "exactly 10,000 years"
	)]
    #[test_case(Duration::new(-10_000 * SECONDS_IN_YEAR , 0).unwrap(), time::Duration::new(-10_000 * SECONDS_IN_YEAR, 0) ; "exactly negative 10,000 years"
	)]
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
        assert!(
            matches!(err, DurationError::Deserialize(_)),
            "unexpected error {err:?}"
        );
        Ok(())
    }

    #[test]
    fn deserialize_unexpected_input_type() -> Result {
        let got = serde_json::from_value::<Duration>(serde_json::json!({}));
        assert!(got.is_err());
        let msg = format!("{got:?}");
        assert!(msg.contains("duration in Google format"), "message={msg}");
        Ok(())
    }

    #[test_case(std::time::Duration::new(0, 0), Duration::clamp(0, 0))]
    #[test_case(
        std::time::Duration::new(0, 400_000_000),
        Duration::clamp(0, 400_000_000)
    )]
    #[test_case(
        std::time::Duration::new(1, 400_000_000),
        Duration::clamp(1, 400_000_000)
    )]
    #[test_case(std::time::Duration::new(10_000 * SECONDS_IN_YEAR as u64, 999_999_999), Duration::clamp(10_000 * SECONDS_IN_YEAR, 999_999_999))]
    fn from_std_time_in_range(input: std::time::Duration, want: Duration) {
        let got = Duration::try_from(input).unwrap();
        assert_eq!(got, want);
    }

    #[test]
    fn convert_from_string() -> Result {
        let input = "12.750s".to_string();
        let a = Duration::try_from(input.as_str())?;
        let b = Duration::try_from(&input)?;
        assert_eq!(a, b);
        Ok(())
    }

    #[test_case(std::time::Duration::new(i64::MAX as u64, 0))]
    #[test_case(std::time::Duration::new(i64::MAX as u64 + 10, 0))]
    fn from_std_time_out_of_range(input: std::time::Duration) {
        let got = Duration::try_from(input);
        assert!(got.is_err(), "{got:?}");
    }

    #[test_case(chrono::Duration::default(), Duration::default() ; "default")]
    #[test_case(chrono::Duration::new(0, 0).unwrap(), Duration::new(0, 0).unwrap() ; "zero")]
    #[test_case(chrono::Duration::new(10_000 * SECONDS_IN_YEAR, 0).unwrap(), Duration::new(10_000 * SECONDS_IN_YEAR, 0).unwrap() ; "exactly 10,000 years"
	)]
    #[test_case(chrono::Duration::new(-10_000 * SECONDS_IN_YEAR, 0).unwrap(), Duration::new(-10_000 * SECONDS_IN_YEAR, 0).unwrap() ; "exactly negative 10,000 years"
	)]
    fn from_chrono_time_in_range(value: chrono::Duration, want: Duration) -> Result {
        let got = Duration::try_from(value)?;
        assert_eq!(got, want);
        Ok(())
    }

    #[test_case(Duration::default(), chrono::Duration::default() ; "default")]
    #[test_case(Duration::new(0, 0).unwrap(), chrono::Duration::new(0, 0).unwrap() ; "zero")]
    #[test_case(Duration::new(0, 500_000).unwrap(), chrono::Duration::new(0, 500_000).unwrap() ; "500us")]
    #[test_case(Duration::new(1, 400_000_000).unwrap(), chrono::Duration::new(1, 400_000_000).unwrap() ; "1.4s")]
    #[test_case(Duration::new(0, -400_000_000).unwrap(), chrono::Duration::new(-1, 600_000_000).unwrap() ; "minus 0.4s")]
    #[test_case(Duration::new(-1, -400_000_000).unwrap(), chrono::Duration::new(-2, 600_000_000).unwrap() ; "minus 1.4s")]
    #[test_case(Duration::new(10_000 * SECONDS_IN_YEAR , 0).unwrap(), chrono::Duration::new(10_000 * SECONDS_IN_YEAR, 0).unwrap() ; "exactly 10,000 years"
	)]
    #[test_case(Duration::new(-10_000 * SECONDS_IN_YEAR , 0).unwrap(), chrono::Duration::new(-10_000 * SECONDS_IN_YEAR, 0).unwrap() ; "exactly negative 10,000 years"
	)]
    fn to_chrono_time_in_range(value: Duration, want: chrono::Duration) -> Result {
        let got = chrono::Duration::from(value);
        assert_eq!(got, want);
        Ok(())
    }

    #[test_case(chrono::Duration::new(10_001 * SECONDS_IN_YEAR, 0).unwrap() ; "above the range")]
    #[test_case(chrono::Duration::new(-10_001 * SECONDS_IN_YEAR, 0).unwrap() ; "below the range")]
    fn from_chrono_time_out_of_range(value: chrono::Duration) {
        let got = Duration::try_from(value);
        assert!(matches!(got, Err(DurationError::OutOfRange)), "{got:?}");
    }
}
