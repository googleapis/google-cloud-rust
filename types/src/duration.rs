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
#[derive(Clone, Debug, Default, PartialEq)]
#[non_exhaustive]
pub struct Duration {
    /// Signed seconds of the span of time.
    ///
    /// Must be from -315,576,000,000 to +315,576,000,000 inclusive. Note: these
    /// bounds are computed from:
    ///     60 sec/min * 60 min/hr * 24 hr/day * 365.25 days/year * 10000 years
    pub seconds: i64,

    /// Signed fractions of a second at nanosecond resolution of the span
    /// of time.
    ///
    /// Durations less than one second are represented with a 0 `seconds` field
    /// and a positive or negative `nanos` field. For durations
    /// of one second or more, a non-zero value for the `nanos` field must be
    /// of the same sign as the `seconds` field. Must be from -999,999,999
    /// to +999,999,999 inclusive.
    pub nanos: i32,
}

const NS: i32 = 1_000_000_000;

impl Duration {
    /// Create a normalized Duration.
    ///
    /// Durations must have the same sign in the seconds and nanos field. This
    /// function creates a normalized value.
    ///
    /// `seconds` - the seconds in the interval.
    /// `nanos` - the nanoseconds *added* to the interval.
    pub fn new(seconds: i64, nanos: i32) -> Self {
        Self::saturated_add(Self::from_seconds(seconds), Self::from_nanos(nanos))
    }

    /// Creates a new [Duration] from the specified number of nanoseconds.
    pub fn from_nanos(nanos: i32) -> Self {
        Self {
            seconds: (nanos / NS) as i64,
            nanos: nanos % NS,
        }
    }

    /// Creates a new [Duration] from the specified number of seconds.
    pub fn from_seconds(seconds: i64) -> Self {
        Self { seconds, nanos: 0 }
    }

    fn saturated_add(a: Duration, b: Duration) -> Self {
        let mut seconds = a.seconds + b.seconds;
        let mut nanos = a.nanos + b.nanos;

        seconds += (nanos / NS) as i64;
        nanos %= NS;
        if seconds > 0 && nanos < 0 {
            return Self {
                seconds: seconds - 1,
                nanos: NS + nanos,
            };
        }
        if seconds < 0 && nanos > 0 {
            return Self {
                seconds: seconds + 1,
                nanos: -(NS - nanos),
            };
        }
        Self { seconds, nanos }
    }
}

/// Implement [`serde`](::serde) serialization for Duration.
impl serde::ser::Serialize for Duration {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        let sign = if self.seconds < 0 || self.nanos < 0 {
            "-"
        } else {
            ""
        };
        let formatted = if self.nanos == 0 {
            format!("{sign}{}s", self.seconds.abs())
        } else if self.seconds == 0 {
            format!("{sign}0.{}", self.nanos.abs())
        } else {
            format!("{sign}{}.{:09}s", self.seconds.abs(), self.nanos.abs())
        };

        formatted.serialize(serializer)
    }
}

struct DurationVisitor;

impl<'de> serde::de::Visitor<'de> for DurationVisitor {
    type Value = Duration;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a string with a duration in Google format ([sign]{seconds}.{nanos}s)")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        use serde::de::Error;

        if !value.ends_with('s') {
            return Err(serde::de::Error::custom("cannot find trailing 's'"));
        }
        let digits = &value[..(value.len() - 1)];
        let (sign, digits) = if let Some(trailing) = digits.strip_prefix('-') {
            (-1, trailing)
        } else {
            (1, digits)
        };
        let mut split = digits.splitn(2, '.');
        let (s, n) = (split.next(), split.next());
        let seconds = s
            .map(str::parse::<i64>)
            .transpose()
            .map_err(Error::custom)?;
        let nanos = n
            .map(str::parse::<i32>)
            .transpose()
            .map_err(Error::custom)?;

        Ok(Duration {
            seconds: sign * seconds.unwrap_or(0),
            nanos: sign as i32 * nanos.unwrap_or(0),
        })
    }
}

/// Implement [`serde`](::serde) deserialization for timestamps.
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

    // Verify 0 converts as expected.
    #[test]
    fn zero() {
        let proto = Duration {
            seconds: 0,
            nanos: 0,
        };
        let json = serde_json::to_value(&proto).unwrap();
        let expected = json!(r#"0s"#);
        assert_eq!(json, expected);
        let roundtrip = serde_json::from_value::<Duration>(json).unwrap();
        assert_eq!(proto, roundtrip);
    }

    // Verify 0 converts as expected.
    #[test_case(0, 0, 0, 0 ; "all inputs are zero")]
    #[test_case(1, 0, 1, 0 ; "positive seconds and zero nanos")]
    #[test_case(1, 200_000, 1, 200_000 ; "positive seconds and nanos")]
    #[test_case(-1, 0, -1, 0; "negative seconds and zero nanos")]
    #[test_case(-1, -500_000_000, -1, -500_000_000; "negative seconds and nanos")]
    #[test_case(2, -400_000_000, 1, 600_000_000; "positive seconds and negative nanos")]
    #[test_case(-2, 400_000_000, -1, -600_000_000; "negative seconds and positive nanos")]
    fn normalize(seconds: i64, nanos: i32, want_seconds: i64, want_nanos: i32) {
        let got = Duration::new(seconds, nanos);
        let want = Duration {
            seconds: want_seconds,
            nanos: want_nanos,
        };
        assert_eq!(want, got);
    }

    #[test_case(0, 0, 0; "zero nanos")]
    #[test_case(1_000_000_000, 1, 0; "one second")]
    #[test_case(1_200_000_000, 1, 200_000_000; "one second and 200ms")]
    #[test_case(-1_200_000_000, -1, -200_000_000; "minus one second and 200ms")]
    fn from_nanos(nanos: i32, want_seconds: i64, want_nanos: i32) {
        let got = Duration::from_nanos(nanos);
        let want = Duration {
            seconds: want_seconds,
            nanos: want_nanos,
        };
        assert_eq!(want, got);
    }

    #[test_case(0, 0, "0s" ; "zero")]
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
    fn serialize(seconds: i64, nanos: i32, want: &str) {
        let got = serde_json::to_value(Duration { seconds, nanos })
            .unwrap()
            .as_str()
            .map(str::to_string)
            .unwrap();
        assert_eq!(want, got);
    }

    // Verify durations can roundtrip from string -> struct -> string without loss.
    #[test]
    fn roundtrip() {
        let inputs = vec!["-315576000000.999999999s", "315576000000.999999999s"];

        for input in inputs {
            let json = serde_json::Value::String(input.to_string());
            let timestamp = serde_json::from_value::<Duration>(json).unwrap();
            let roundtrip = serde_json::to_string(&timestamp).unwrap();
            assert_eq!(
                format!("\"{input}\""),
                roundtrip,
                "mismatched value for input={input}"
            );
        }
    }

    #[serde_with::skip_serializing_none]
    #[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]
    #[serde(rename_all = "camelCase")]
    #[non_exhaustive]
    pub struct Helper {
        pub time_to_live: Option<Duration>,
    }

    #[test]
    fn serialize_in_struct() {
        let input = Helper {
            ..Default::default()
        };
        let json = serde_json::to_value(input).unwrap();
        assert_eq!(json, json!({}));

        let input = Helper {
            time_to_live: Some(Duration {
                seconds: 12,
                nanos: 345678900,
            }),
            ..Default::default()
        };

        let json = serde_json::to_value(input).unwrap();
        assert_eq!(json, json!({ "timeToLive": "12.345678900s" }));
    }

    #[test]
    fn deserialize_in_struct() {
        let input = json!({});
        let want = Helper {
            ..Default::default()
        };
        let got = serde_json::from_value::<Helper>(input).unwrap();
        assert_eq!(want, got);

        let input = json!({ "timeToLive": "12.345678900s" });
        let want = Helper {
            time_to_live: Some(Duration {
                seconds: 12,
                nanos: 345678900,
            }),
            ..Default::default()
        };
        let got = serde_json::from_value::<Helper>(input).unwrap();
        assert_eq!(want, got);
    }
}
