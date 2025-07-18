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

use google_cloud_wkt::{Duration, DurationError};
use serde_json::json;
type Result = std::result::Result<(), Box<dyn std::error::Error>>;

#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Helper {
    pub time_to_live: Option<Duration>,
}

#[test]
fn access() {
    let d = Duration::default();
    assert_eq!(d.nanos(), 0);
    assert_eq!(d.seconds(), 0);
}

#[test]
fn serialize_in_struct() -> Result {
    let input = Helper::default();
    let json = serde_json::to_value(input)?;
    assert_eq!(json, json!({}));

    let input = Helper {
        time_to_live: Some(Duration::clamp(12, 345678900)),
    };

    let json = serde_json::to_value(input)?;
    assert_eq!(json, json!({ "timeToLive": "12.3456789s" }));
    Ok(())
}

#[test]
fn deserialize_in_struct() -> Result {
    let input = json!({});
    let want = Helper::default();
    let got = serde_json::from_value::<Helper>(input)?;
    assert_eq!(want, got);

    let input = json!({ "timeToLive": "12.3456789s" });
    let want = Helper {
        time_to_live: Some(Duration::clamp(12, 345678900)),
    };
    let got = serde_json::from_value::<Helper>(input)?;
    assert_eq!(want, got);
    Ok(())
}

#[test]
fn compare() {
    let ts0 = Duration::default();
    let ts1 = Duration::clamp(1, 100);
    let ts2 = Duration::clamp(1, 200);
    let ts3 = Duration::clamp(2, 0);
    assert_eq!(ts0.partial_cmp(&ts0), Some(std::cmp::Ordering::Equal));
    assert_eq!(ts0.partial_cmp(&ts1), Some(std::cmp::Ordering::Less));
    assert_eq!(ts2.partial_cmp(&ts3), Some(std::cmp::Ordering::Less));
}

#[test]
fn from_std_time_duration() -> Result {
    let std_d = std::time::Duration::new(123, 456789012);
    let got = Duration::try_from(std_d)?;
    let want = Duration::new(123, 456789012)?;
    assert_eq!(got, want);

    let std_d = std::time::Duration::new(i64::MAX as u64 + 2, 0);
    let got = Duration::try_from(std_d);
    assert!(matches!(got, Err(DurationError::OutOfRange)), "{got:?}");

    Ok(())
}

#[test]
fn std_from_duration() -> Result {
    let dur = Duration::new(123, 456789012)?;
    let got = std::time::Duration::try_from(dur)?;
    let want = std::time::Duration::new(123, 456789012);
    assert_eq!(got, want);

    let dur = Duration::new(-10, 0)?;
    let got = std::time::Duration::try_from(dur);
    assert!(matches!(got, Err(DurationError::OutOfRange)), "{got:?}");

    let dur = Duration::new(0, -10)?;
    let got = std::time::Duration::try_from(dur);
    assert!(matches!(got, Err(DurationError::OutOfRange)), "{got:?}");

    Ok(())
}
