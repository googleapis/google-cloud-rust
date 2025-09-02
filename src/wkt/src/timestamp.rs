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
/// # Examples
/// ```
/// # use google_cloud_wkt::{Timestamp, TimestampError};
/// let ts = Timestamp::try_from("2025-05-16T09:46:12.500Z")?;
/// assert_eq!(ts.seconds(), 1747388772);
/// assert_eq!(ts.nanos(), 500_000_000);
///
/// assert_eq!(ts, Timestamp::new(1747388772, 500_000_000)?);
/// assert_eq!(ts, Timestamp::clamp(1747388772, 500_000_000));
/// # Ok::<(), TimestampError>(())
/// ```
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
#[derive(Clone, Copy, Debug, Default, PartialEq, PartialOrd)]
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
///
/// Examples
/// ```
/// # use google_cloud_wkt::{Timestamp, TimestampError};
/// let ts = Timestamp::new(Timestamp::MAX_SECONDS + 2, 0);
/// assert!(matches!(ts, Err(TimestampError::OutOfRange)));
///
/// let ts = Timestamp::new(0, 1_500_000_000);
/// assert!(matches!(ts, Err(TimestampError::OutOfRange)));
///
/// let ts = Timestamp::try_from("invalid");
/// assert!(matches!(ts, Err(TimestampError::Deserialize(_))));
/// ```
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum TimestampError {
    /// One of the components (seconds and/or nanoseconds) was out of range.
    #[error("seconds and/or nanoseconds out of range")]
    OutOfRange,

    /// There was a problem deserializing a timestamp.
    #[error("cannot deserialize timestamp, source={0}")]
    Deserialize(#[source] BoxedError),
}

type BoxedError = Box<dyn std::error::Error + Send + Sync>;
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
    /// # Examples
    /// ```
    /// # use google_cloud_wkt::{Timestamp, TimestampError};
    /// let ts = Timestamp::new(1747388772, 0)?;
    /// assert_eq!(String::from(ts), "2025-05-16T09:46:12Z");
    ///
    /// let ts = Timestamp::new(1747388772, 2_000_000_000);
    /// assert!(matches!(ts, Err(TimestampError::OutOfRange)));
    /// # Ok::<(), TimestampError>(())
    /// ```
    ///
    /// # Parameters
    ///
    /// * `seconds` - the seconds on the timestamp.
    /// * `nanos` - the nanoseconds on the timestamp.
    pub fn new(seconds: i64, nanos: i32) -> Result<Self, Error> {
        if !(Self::MIN_SECONDS..=Self::MAX_SECONDS).contains(&seconds) {
            return Err(Error::OutOfRange);
        }
        if !(Self::MIN_NANOS..=Self::MAX_NANOS).contains(&nanos) {
            return Err(Error::OutOfRange);
        }
        Ok(Self { seconds, nanos })
    }

    /// Create a normalized, clamped [Timestamp].
    ///
    /// # Examples
    /// ```
    /// # use google_cloud_wkt::{Timestamp, TimestampError};
    /// let ts = Timestamp::clamp(1747388772, 0);
    /// assert_eq!(String::from(ts), "2025-05-16T09:46:12Z");
    ///
    /// let ts = Timestamp::clamp(1747388772, 2_000_000_000);
    /// // extra nanoseconds are carried as seconds
    /// assert_eq!(String::from(ts), "2025-05-16T09:46:14Z");
    /// # Ok::<(), TimestampError>(())
    /// ```
    ///
    /// Timestamps must be between 0001-01-01T00:00:00Z and
    /// 9999-12-31T23:59:59.999999999Z, and the nanoseconds component must
    /// always be in the range [0, 999_999_999]. This function creates a
    /// new [Timestamp] instance clamped to those ranges.
    ///
    /// The function effectively adds the nanoseconds part (with carry) to the
    /// seconds part, with saturation.
    ///
    /// # Parameters
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
    ///
    /// # Examples
    /// ```
    /// # use google_cloud_wkt::{Timestamp, TimestampError};
    /// let ts = Timestamp::new(120, 500_000_000)?;
    /// assert_eq!(ts.seconds(), 120);
    /// # Ok::<(), TimestampError>(())
    /// ```
    pub fn seconds(&self) -> i64 {
        self.seconds
    }

    /// Non-negative fractions of a second at nanosecond resolution.
    ///
    /// Negative second values (before the Unix epoch) with fractions must still
    /// have non-negative nanos values that count forward in time. Must be from
    /// 0 to 999,999,999 inclusive.
    ///
    /// # Examples
    /// ```
    /// # use google_cloud_wkt::{Timestamp, TimestampError};
    /// let ts = Timestamp::new(120, 500_000_000)?;
    /// assert_eq!(ts.nanos(), 500_000_000);
    /// # Ok::<(), TimestampError>(())
    /// ```
    pub fn nanos(&self) -> i32 {
        self.nanos
    }
}

impl crate::message::Message for Timestamp {
    fn typename() -> &'static str {
        "type.googleapis.com/google.protobuf.Timestamp"
    }

    #[allow(private_interfaces)]
    fn serializer() -> impl crate::message::MessageSerializer<Self> {
        crate::message::ValueSerializer::<Self>::new()
    }
}

const NS: i128 = 1_000_000_000;

/// Implement [`serde`](::serde) serialization for timestamps.
#[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
impl serde::ser::Serialize for Timestamp {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        String::from(*self).serialize(serializer)
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
#[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
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
///
/// # Examples
/// ```
/// # use google_cloud_wkt::Timestamp;
/// use time::{macros::datetime, OffsetDateTime};
/// let dt = datetime!(2025-05-16 09:46:12 UTC);
/// let ts = Timestamp::try_from(dt)?;
/// assert_eq!(String::from(ts), "2025-05-16T09:46:12Z");
/// # Ok::<(), anyhow::Error>(())
/// ```
#[cfg(feature = "time")]
#[cfg_attr(docsrs, doc(cfg(feature = "time")))]
impl TryFrom<time::OffsetDateTime> for Timestamp {
    type Error = TimestampError;

    fn try_from(value: time::OffsetDateTime) -> Result<Self, Self::Error> {
        const SCALE: i128 = 1_000_000_000_i128;
        let seconds = value.unix_timestamp();
        let nanos = (value.unix_timestamp_nanos() - seconds as i128 * SCALE) as i32;
        Self::new(seconds, nanos)
    }
}

/// Convert from [Timestamp] to [OffsetDateTime][time::OffsetDateTime]
///
/// This conversion may fail if the [Timestamp] value is out of range.
///
/// # Examples
/// ```
/// # use google_cloud_wkt::Timestamp;
/// use time::{macros::datetime, OffsetDateTime};
/// let ts = Timestamp::try_from("2025-05-16T09:46:12Z")?;
/// let dt = OffsetDateTime::try_from(ts)?;
/// assert_eq!(dt, datetime!(2025-05-16 09:46:12 UTC));
/// # Ok::<(), anyhow::Error>(())
/// ```
#[cfg(feature = "time")]
#[cfg_attr(docsrs, doc(cfg(feature = "time")))]
impl TryFrom<Timestamp> for time::OffsetDateTime {
    type Error = time::error::ComponentRange;
    fn try_from(value: Timestamp) -> Result<Self, Self::Error> {
        let ts = time::OffsetDateTime::from_unix_timestamp(value.seconds())?;
        Ok(ts + time::Duration::nanoseconds(value.nanos() as i64))
    }
}

const EXPECT_OFFSET_DATE_TIME_CONVERTS: &str = concat!(
    "converting Timestamp to time::OffsetDateTime should always succeed. ",
    "The Timestamp values are always in range. ",
    "If this is not the case, please file a bug at https://github.com/googleapis/google-cloud-rust/issues"
);
const EXPECT_TIMESTAMP_FORMAT_SUCCEEDS: &str = concat!(
    "formatting a Timestamp using RFC-3339 should always succeed. ",
    "The Timestamp values are always in range, and we use a well-known constant for the format specifier. ",
    "If this is not the case, please file a bug at https://github.com/googleapis/google-cloud-rust/issues"
);
use time::format_description::well_known::Rfc3339;

/// Converts a [Timestamp] to its [String] representation.
///
/// # Example
/// ```
/// # use google_cloud_wkt::{Timestamp, TimestampError};
/// let ts = Timestamp::new(1747388772, 0)?;
/// assert_eq!(String::from(ts), "2025-05-16T09:46:12Z");
/// # Ok::<(), anyhow::Error>(())
/// ```
impl From<Timestamp> for String {
    fn from(timestamp: Timestamp) -> Self {
        let ts = time::OffsetDateTime::from_unix_timestamp_nanos(
            timestamp.seconds as i128 * NS + timestamp.nanos as i128,
        )
        .expect(EXPECT_OFFSET_DATE_TIME_CONVERTS);
        ts.format(&Rfc3339).expect(EXPECT_TIMESTAMP_FORMAT_SUCCEEDS)
    }
}

/// Converts the string representation of a timestamp to [Timestamp].
///
/// # Example
/// ```
/// # use google_cloud_wkt::{Timestamp, TimestampError};
/// let ts = Timestamp::try_from("2025-05-16T09:46:12.500Z")?;
/// assert_eq!(ts.seconds(), 1747388772);
/// assert_eq!(ts.nanos(), 500_000_000);
/// # Ok::<(), anyhow::Error>(())
/// ```
impl TryFrom<&str> for Timestamp {
    type Error = TimestampError;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let odt = time::OffsetDateTime::parse(value, &Rfc3339)
            .map_err(|e| TimestampError::Deserialize(e.into()))?;
        let nanos_since_epoch = odt.unix_timestamp_nanos();
        let seconds = (nanos_since_epoch / NS) as i64;
        let nanos = (nanos_since_epoch % NS) as i32;
        if nanos < 0 {
            return Timestamp::new(seconds - 1, Self::NS + nanos);
        }
        Timestamp::new(seconds, nanos)
    }
}

/// Converts the string representation of a timestamp to [Timestamp].
///
/// # Example
/// ```
/// # use google_cloud_wkt::{Timestamp, TimestampError};
/// let s = "2025-05-16T09:46:12.500Z".to_string();
/// let ts = Timestamp::try_from(&s)?;
/// assert_eq!(ts.seconds(), 1747388772);
/// assert_eq!(ts.nanos(), 500_000_000);
/// # Ok::<(), anyhow::Error>(())
/// ```
impl TryFrom<&String> for Timestamp {
    type Error = TimestampError;
    fn try_from(value: &String) -> Result<Self, Self::Error> {
        Timestamp::try_from(value.as_str())
    }
}

/// Converts from [chrono::DateTime] to [Timestamp].
///
/// This conversion may fail if the [chrono::DateTime] value is out of range.
///
/// # Example
/// ```
/// # use google_cloud_wkt::{Timestamp, TimestampError};
/// use chrono::{DateTime, TimeZone, Utc};
/// let date : DateTime<Utc> = Utc.with_ymd_and_hms(2025, 5, 16, 10, 15, 00).unwrap();
/// let ts = Timestamp::try_from(date)?;
/// assert_eq!(String::from(ts), "2025-05-16T10:15:00Z");
/// # Ok::<(), anyhow::Error>(())
/// ```
#[cfg(feature = "chrono")]
#[cfg_attr(docsrs, doc(cfg(feature = "chrono")))]
impl TryFrom<chrono::DateTime<chrono::Utc>> for Timestamp {
    type Error = TimestampError;

    fn try_from(value: chrono::DateTime<chrono::Utc>) -> Result<Self, Self::Error> {
        assert!(value.timestamp_subsec_nanos() <= (i32::MAX as u32));
        Timestamp::new(value.timestamp(), value.timestamp_subsec_nanos() as i32)
    }
}

/// Converts from [Timestamp] to [chrono::DateTime].
///
/// # Example
/// ```
/// # use google_cloud_wkt::{Timestamp, TimestampError};
/// use chrono::{DateTime, TimeZone, Utc};
/// let ts = Timestamp::try_from("2025-05-16T10:15:00Z")?;
/// let date = DateTime::try_from(ts)?;
/// # Ok::<(), anyhow::Error>(())
/// ```
#[cfg(feature = "chrono")]
#[cfg_attr(docsrs, doc(cfg(feature = "chrono")))]
impl TryFrom<Timestamp> for chrono::DateTime<chrono::Utc> {
    type Error = TimestampError;
    fn try_from(value: Timestamp) -> Result<Self, Self::Error> {
        let ts = chrono::DateTime::from_timestamp(value.seconds, 0).unwrap();
        Ok(ts + chrono::Duration::nanoseconds(value.nanos as i64))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use test_case::test_case;
    type Result = std::result::Result<(), Box<dyn std::error::Error>>;

    // Verify the epoch converts as expected.
    #[test]
    fn unix_epoch() -> Result {
        let proto = Timestamp::default();
        let json = serde_json::to_value(proto)?;
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
        assert!(matches!(t, Err(Error::OutOfRange)), "{t:?}");
        Ok(())
    }

    #[test_case(0, 0, 0, 0; "zero")]
    #[test_case(0, 1_234_567_890, 1, 234_567_890; "nanos overflow")]
    #[test_case(0, 2_100_000_123, 2, 100_000_123; "nanos overflow x2")]
    #[test_case(0, -1_400_000_000, -2, 600_000_000; "nanos underflow")]
    #[test_case(0, -2_100_000_000, -3, 900_000_000; "nanos underflow x2")]
    #[test_case(self::get_max_seconds() + 1, 0, get_max_seconds(), 0; "seconds over range")]
    #[test_case(self::get_min_seconds() - 1, 0, get_min_seconds(), 0; "seconds below range")]
    #[test_case(self::get_max_seconds() - 1, 2_000_000_001, get_max_seconds(), 0; "nanos overflow range"
	)]
    #[test_case(self::get_min_seconds() + 1, -1_500_000_000, get_min_seconds(), 0; "nanos underflow range"
	)]
    fn clamp(seconds: i64, nanos: i32, want_seconds: i64, want_nanos: i32) {
        let got = Timestamp::clamp(seconds, nanos);
        let want = Timestamp {
            seconds: want_seconds,
            nanos: want_nanos,
        };
        assert_eq!(got, want);
    }

    // Verify timestamps can roundtrip from string -> struct -> string without loss.
    #[test_case("0001-01-01T00:00:00.123456789Z")]
    #[test_case("0001-01-01T00:00:00.123456Z")]
    #[test_case("0001-01-01T00:00:00.123Z")]
    #[test_case("0001-01-01T00:00:00Z")]
    #[test_case("1960-01-01T00:00:00.123456789Z")]
    #[test_case("1960-01-01T00:00:00.123456Z")]
    #[test_case("1960-01-01T00:00:00.123Z")]
    #[test_case("1960-01-01T00:00:00Z")]
    #[test_case("1970-01-01T00:00:00.123456789Z")]
    #[test_case("1970-01-01T00:00:00.123456Z")]
    #[test_case("1970-01-01T00:00:00.123Z")]
    #[test_case("1970-01-01T00:00:00Z")]
    #[test_case("9999-12-31T23:59:59.999999999Z")]
    #[test_case("9999-12-31T23:59:59.123456789Z")]
    #[test_case("9999-12-31T23:59:59.123456Z")]
    #[test_case("9999-12-31T23:59:59.123Z")]
    #[test_case("2024-10-19T12:34:56Z")]
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

    // Verify timestamps work for some well know times, including fractional
    // seconds.
    #[test_case(
        "0001-01-01T00:00:00.123456789Z",
        Timestamp::clamp(Timestamp::MIN_SECONDS, 123_456_789)
    )]
    #[test_case(
        "0001-01-01T00:00:00.123456Z",
        Timestamp::clamp(Timestamp::MIN_SECONDS, 123_456_000)
    )]
    #[test_case(
        "0001-01-01T00:00:00.123Z",
        Timestamp::clamp(Timestamp::MIN_SECONDS, 123_000_000)
    )]
    #[test_case("0001-01-01T00:00:00Z", Timestamp::clamp(Timestamp::MIN_SECONDS, 0))]
    #[test_case("1970-01-01T00:00:00.123456789Z", Timestamp::clamp(0, 123_456_789))]
    #[test_case("1970-01-01T00:00:00.123456Z", Timestamp::clamp(0, 123_456_000))]
    #[test_case("1970-01-01T00:00:00.123Z", Timestamp::clamp(0, 123_000_000))]
    #[test_case("1970-01-01T00:00:00Z", Timestamp::clamp(0, 0))]
    #[test_case(
        "9999-12-31T23:59:59.123456789Z",
        Timestamp::clamp(Timestamp::MAX_SECONDS, 123_456_789)
    )]
    #[test_case(
        "9999-12-31T23:59:59.123456Z",
        Timestamp::clamp(Timestamp::MAX_SECONDS, 123_456_000)
    )]
    #[test_case(
        "9999-12-31T23:59:59.123Z",
        Timestamp::clamp(Timestamp::MAX_SECONDS, 123_000_000)
    )]
    #[test_case("9999-12-31T23:59:59Z", Timestamp::clamp(Timestamp::MAX_SECONDS, 0))]
    fn well_known(input: &str, want: Timestamp) -> Result {
        let json = serde_json::Value::String(input.to_string());
        let got = serde_json::from_value::<Timestamp>(json)?;
        assert_eq!(want, got);
        Ok(())
    }

    #[test_case("1970-01-01T00:00:00Z", Timestamp::clamp(0, 0); "zulu offset")]
    #[test_case("1970-01-01T00:00:00+02:00", Timestamp::clamp(-2 * 60 * 60, 0); "2h positive")]
    #[test_case("1970-01-01T00:00:00+02:45", Timestamp::clamp(-2 * 60 * 60 - 45 * 60, 0); "2h45m positive"
	)]
    #[test_case("1970-01-01T00:00:00-02:00", Timestamp::clamp(2 * 60 * 60, 0); "2h negative")]
    #[test_case("1970-01-01T00:00:00-02:45", Timestamp::clamp(2 * 60 * 60 + 45 * 60, 0); "2h45m negative"
	)]
    fn deserialize_offsets(input: &str, want: Timestamp) -> Result {
        let json = serde_json::Value::String(input.to_string());
        let got = serde_json::from_value::<Timestamp>(json)?;
        assert_eq!(want, got);
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
        assert!(msg.contains("RFC 3339"), "message={msg}");
        Ok(())
    }

    #[serde_with::skip_serializing_none]
    #[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]
    #[serde(rename_all = "camelCase")]
    struct Helper {
        pub create_time: Option<Timestamp>,
    }

    #[test]
    fn access() {
        let ts = Timestamp::default();
        assert_eq!(ts.nanos(), 0);
        assert_eq!(ts.seconds(), 0);
    }

    #[test]
    fn serialize_in_struct() -> Result {
        let input = Helper {
            ..Default::default()
        };
        let json = serde_json::to_value(input)?;
        assert_eq!(json, json!({}));

        let input = Helper {
            create_time: Some(Timestamp::new(12, 345_678_900)?),
        };

        let json = serde_json::to_value(input)?;
        assert_eq!(
            json,
            json!({ "createTime": "1970-01-01T00:00:12.3456789Z" })
        );
        Ok(())
    }

    #[test]
    fn deserialize_in_struct() -> Result {
        let input = json!({});
        let want = Helper {
            ..Default::default()
        };
        let got = serde_json::from_value::<Helper>(input)?;
        assert_eq!(want, got);

        let input = json!({ "createTime": "1970-01-01T00:00:12.3456789Z" });
        let want = Helper {
            create_time: Some(Timestamp::new(12, 345678900)?),
        };
        let got = serde_json::from_value::<Helper>(input)?;
        assert_eq!(want, got);
        Ok(())
    }

    #[test]
    fn compare() -> Result {
        let ts0 = Timestamp::default();
        let ts1 = Timestamp::new(1, 100)?;
        let ts2 = Timestamp::new(1, 200)?;
        let ts3 = Timestamp::new(2, 0)?;
        assert_eq!(ts0.partial_cmp(&ts0), Some(std::cmp::Ordering::Equal));
        assert_eq!(ts0.partial_cmp(&ts1), Some(std::cmp::Ordering::Less));
        assert_eq!(ts2.partial_cmp(&ts3), Some(std::cmp::Ordering::Less));
        Ok(())
    }

    #[test]
    fn convert_from_string() -> Result {
        let input = "2025-05-16T18:00:00Z".to_string();
        let a = Timestamp::try_from(input.as_str())?;
        let b = Timestamp::try_from(&input)?;
        assert_eq!(a, b);
        Ok(())
    }

    #[test]
    fn convert_from_time() -> Result {
        let ts = time::OffsetDateTime::from_unix_timestamp(123)?
            + time::Duration::nanoseconds(456789012);
        let got = Timestamp::try_from(ts)?;
        let want = Timestamp::new(123, 456789012)?;
        assert_eq!(got, want);
        Ok(())
    }

    #[test]
    fn convert_to_time() -> Result {
        let ts = Timestamp::new(123, 456789012)?;
        let got = time::OffsetDateTime::try_from(ts)?;
        let want = time::OffsetDateTime::from_unix_timestamp(123)?
            + time::Duration::nanoseconds(456789012);
        assert_eq!(got, want);
        Ok(())
    }

    #[test]
    fn convert_from_chrono_time() -> Result {
        let ts = chrono::DateTime::from_timestamp(123, 456789012).unwrap();
        let got = Timestamp::try_from(ts)?;
        let want = Timestamp::new(123, 456789012)?;
        assert_eq!(got, want);
        Ok(())
    }

    #[test]
    fn convert_to_chrono_time() -> Result {
        let ts = Timestamp::new(123, 456789012)?;
        let got = chrono::DateTime::try_from(ts)?;
        let want = chrono::DateTime::from_timestamp(123, 456789012).unwrap();
        assert_eq!(got, want);
        Ok(())
    }
}
