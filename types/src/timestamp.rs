// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

///
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
#[derive(Clone, Debug, Default, PartialEq)]
#[non_exhaustive]
pub struct Timestamp {
    /// Represents seconds of UTC time since Unix epoch
    /// 1970-01-01T00:00:00Z. Must be from 0001-01-01T00:00:00Z to
    /// 9999-12-31T23:59:59Z inclusive.
    pub seconds: i64,

    /// Non-negative fractions of a second at nanosecond resolution. Negative
    /// second values with fractions must still have non-negative nanos values
    /// that count forward in time. Must be from 0 to 999,999,999
    /// inclusive.
    pub nanos: i32,
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
        let ts = time::OffsetDateTime::from_unix_timestamp_nanos(
            self.seconds as i128 * NS + self.nanos as i128,
        )
        .map_err(S::Error::custom)?;
        ts.format(&Rfc3339)
            .map_err(S::Error::custom)?
            .serialize(serializer)
    }
}

struct TimestampVisitor;

impl<'de> serde::de::Visitor<'de> for TimestampVisitor {
    type Value = Timestamp;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a string with a timestamp in RFC 3339 format")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let odt = time::OffsetDateTime::parse(&value, &Rfc3339).map_err(E::custom)?;
        let nanos_since_epoch = odt.unix_timestamp_nanos();
        let seconds = (nanos_since_epoch / NS) as i64;
        let nanos = (nanos_since_epoch % NS) as i32;
        Ok(Self::Value {
            seconds: seconds,
            nanos: nanos,
        })
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

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::json;

    // Verify the epoch converts as expected.
    #[test]
    fn unix_epoch() {
        let proto = Timestamp {
            seconds: 0,
            nanos: 0,
        };
        let json = serde_json::to_value(&proto).unwrap();
        let expected = json!(r#"1970-01-01T00:00:00Z"#);
        assert_eq!(json, expected);
        let roundtrip = serde_json::from_value::<Timestamp>(json).unwrap();
        assert_eq!(proto, roundtrip);
    }

    // Verify timestamps can roundtrip from string -> struct -> string without loss.
    #[test]
    fn roundtrip() {
        let inputs = vec![
            "0001-01-01T00:00:00Z",
            "9999-12-31T23:59:59.999999999Z",
            "2024-10-19T12:34:56.789Z",
            "2024-10-19T12:34:56.789123456Z",
        ];

        for input in inputs {
            let json = serde_json::Value::String(input.to_string());
            let timestamp = serde_json::from_value::<Timestamp>(json).unwrap();
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
        pub create_time: Option<Timestamp>,
    }

    #[test]
    fn serialize_in_struct() {
        let input = Helper {
            ..Default::default()
        };
        let json = serde_json::to_value(input).unwrap();
        assert_eq!(json, json!({}));

        let input = Helper {
            create_time: Some(Timestamp {
                seconds: 12,
                nanos: 345678000,
            }),
            ..Default::default()
        };

        let json = serde_json::to_value(input).unwrap();
        assert_eq!(json, json!({ "createTime": "1970-01-01T00:00:12.345678Z" }));
    }

    #[test]
    fn deserialize_in_struct() {
        let input = json!({});
        let want = Helper {
            ..Default::default()
        };
        let got = serde_json::from_value::<Helper>(input).unwrap();
        assert_eq!(want, got);

        let input = json!({ "createTime": "1970-01-01T00:00:12.345678Z" });
        let want = Helper {
            create_time: Some(Timestamp {
                seconds: 12,
                nanos: 345678000,
            }),
            ..Default::default()
        };
        let got = serde_json::from_value::<Helper>(input).unwrap();
        assert_eq!(want, got);
    }
}
