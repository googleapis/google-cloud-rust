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

use gcp_sdk_wkt::*;
use serde_json::json;
type Result = std::result::Result<(), Box<dyn std::error::Error>>;

#[serde_with::serde_as]
#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct Helper {
    pub field_double: Option<DoubleValue>,
    pub field_float: Option<FloatValue>,
    #[serde_as(as = "Option<serde_with::DisplayFromStr>")]
    pub field_int64: Option<Int64Value>,
    #[serde_as(as = "Option<serde_with::DisplayFromStr>")]
    pub field_uint64: Option<UInt64Value>,
    pub field_int32: Option<Int32Value>,
    pub field_uint32: Option<UInt32Value>,
    pub field_bool: Option<BoolValue>,
    pub field_string: Option<StringValue>,
    #[serde_as(as = "Option<serde_with::base64::Base64>")]
    pub field_bytes: Option<BytesValue>,
}

#[test]
fn serialize_in_struct() -> Result {
    let input = Helper {
        field_double: Some(42.0_f64),
        field_float: Some(42.0_f32),
        field_int64: Some(42),
        field_uint64: Some(42),
        field_int32: Some(42),
        field_uint32: Some(42),
        field_bool: Some(true),
        field_string: Some("zebras are more fun than foxes".to_string()),
        field_bytes: Some(bytes::Bytes::from_static(
            "but zebras are vexing".as_bytes(),
        )),
    };
    let json = serde_json::to_value(&input)?;
    let want = json!({
        "fieldDouble": 42_f64,
        "fieldFloat": 42_f32,
        "fieldInt64": "42",
        "fieldUint64": "42",
        "fieldInt32": 42,
        "fieldUint32": 42,
        "fieldBool": true,
        "fieldString": "zebras are more fun than foxes",
        "fieldBytes": "YnV0IHplYnJhcyBhcmUgdmV4aW5n",
    });
    assert_eq!(json, want);

    let roundtrip = serde_json::from_value::<Helper>(json)?;
    assert_eq!(input, roundtrip);
    Ok(())
}
