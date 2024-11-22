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

use gcp_sdk_wkt::Timestamp;
use serde_json::json;
type Result = std::result::Result<(), Box<dyn std::error::Error>>;

#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Helper {
    pub create_time: Option<Timestamp>,
}

#[test]
fn access() {
    let ts = Timestamp::default();
    assert_eq!(ts.nanos, 0);
    assert_eq!(ts.seconds, 0);
}

#[test]
fn serialize_in_struct() -> Result {
    let input = Helper {
        ..Default::default()
    };
    let json = serde_json::to_value(input)?;
    assert_eq!(json, json!({}));

    let input = Helper {
        create_time: Some(Timestamp::default().set_seconds(12).set_nanos(345_678_900)),
        ..Default::default()
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
        create_time: Some(Timestamp::default().set_seconds(12).set_nanos(345678900)),
        ..Default::default()
    };
    let got = serde_json::from_value::<Helper>(input)?;
    assert_eq!(want, got);
    Ok(())
}

#[test]
fn compare() {
    let ts0 = Timestamp::default();
    let ts1 = Timestamp::default().set_seconds(1).set_nanos(100);
    let ts2 = Timestamp::default().set_seconds(1).set_nanos(200);
    let ts3 = Timestamp::default().set_seconds(2);
    assert_eq!(ts0.partial_cmp(&ts0), Some(std::cmp::Ordering::Equal));
    assert_eq!(ts0.partial_cmp(&ts1), Some(std::cmp::Ordering::Less));
    assert_eq!(ts2.partial_cmp(&ts3), Some(std::cmp::Ordering::Less));
}
