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

use gcp_sdk_wkt::Any;
use gcp_sdk_wkt::Duration;

type Result = std::result::Result<(), Box<dyn std::error::Error>>;

#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TestOnly {
    pub parent: String,
    pub filter: Option<String>,
}

impl gcp_sdk_wkt::message::Message for TestOnly {
    fn typename() -> &'static str {
        "type.googleapis.com/wkt.test.TEstOnly"
    }
}

#[test]
fn roundtrip_generic() -> Result {
    let input = TestOnly {
        parent: "parent".to_string(),
        ..Default::default()
    };
    let any = Any::try_from(&input)?;
    let json = serde_json::to_value(any)?;
    let any = serde_json::from_value::<Any>(json)?;
    let output = any.try_into_message::<TestOnly>()?;
    assert_eq!(input, output);
    Ok(())
}

#[test]
fn roundtrip_duration() -> Result {
    let input = Duration::new(12, 3456)?;
    let any = Any::try_from(&input)?;
    let json = serde_json::to_value(any)?;
    let any = serde_json::from_value::<Any>(json)?;
    let output = any.try_into_message::<Duration>()?;
    assert_eq!(input, output);
    Ok(())
}
