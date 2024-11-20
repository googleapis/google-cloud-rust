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

use gcp_sdk_wkt::FieldMask;
use serde_json::json;
type Result = std::result::Result<(), Box<dyn std::error::Error>>;

#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct Helper {
    pub mask: Option<FieldMask>,
}

#[test]
fn access() {
    let fm = FieldMask::default();
    assert_eq!(fm.paths, Vec::<String>::new());
}

#[test]
fn serialize_in_struct() -> Result {
    let input = Helper {
        ..Default::default()
    };
    let json = serde_json::to_value(input)?;
    assert_eq!(json, json!({}));

    let input = Helper {
        mask: Some(FieldMask::default().set_paths(["f1", "f2", "f3"].map(str::to_string).to_vec())),
        ..Default::default()
    };

    let json = serde_json::to_value(input)?;
    assert_eq!(json, json!({ "mask": {"paths": "f1,f2,f3"} }));
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

    let input = json!({ "mask": {"paths": "field1,field2,field3" }});
    let want = Helper {
        mask: Some(
            FieldMask::default()
                .set_paths(["field1", "field2", "field3"].map(str::to_string).to_vec()),
        ),
        ..Default::default()
    };
    let got = serde_json::from_value::<Helper>(input)?;
    assert_eq!(want, got);
    Ok(())
}
