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

use google_cloud_wkt as wkt;
use serde_json::json;
type Result = std::result::Result<(), Box<dyn std::error::Error>>;

#[serde_with::serde_as]
#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct Helper {
    #[serde_as(as = "Option<wkt::internal::F64>")]
    pub field_double: Option<wkt::DoubleValue>,
    #[serde_as(as = "Option<wkt::internal::F64>")]
    pub field_double_neg_inf: Option<wkt::DoubleValue>,
    #[serde_as(as = "Option<wkt::internal::F32>")]
    pub field_float: Option<wkt::FloatValue>,
    #[serde_as(as = "Option<wkt::internal::F32>")]
    pub field_float_inf: Option<wkt::FloatValue>,
    #[serde_as(as = "Option<serde_with::DisplayFromStr>")]
    pub field_int64: Option<wkt::Int64Value>,
    #[serde_as(as = "Option<serde_with::DisplayFromStr>")]
    pub field_uint64: Option<wkt::UInt64Value>,
    pub field_int32: Option<wkt::Int32Value>,
    pub field_uint32: Option<wkt::UInt32Value>,
    pub field_bool: Option<wkt::BoolValue>,
    pub field_string: Option<wkt::StringValue>,
    #[serde_as(as = "Option<serde_with::base64::Base64>")]
    pub field_bytes: Option<wkt::BytesValue>,
}

#[serde_with::serde_as]
#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct Repeated {
    #[serde_as(as = "Vec<wkt::internal::F64>")]
    pub field_double: Vec<wkt::DoubleValue>,
    #[serde_as(as = "Vec<wkt::internal::F32>")]
    pub field_float: Vec<wkt::FloatValue>,
    #[serde_as(as = "Vec<serde_with::DisplayFromStr>")]
    pub field_int64: Vec<wkt::Int64Value>,
    #[serde_as(as = "Vec<serde_with::DisplayFromStr>")]
    pub field_uint64: Vec<wkt::UInt64Value>,
    pub field_int32: Vec<wkt::Int32Value>,
    pub field_uint32: Vec<wkt::UInt32Value>,
    pub field_bool: Vec<wkt::BoolValue>,
    pub field_string: Vec<wkt::StringValue>,
    #[serde_as(as = "Vec<serde_with::base64::Base64>")]
    pub field_bytes: Vec<wkt::BytesValue>,
}

#[test]
fn serialize_in_struct() -> Result {
    let input = Helper {
        field_double: Some(42.0_f64),
        field_double_neg_inf: Some(f64::NEG_INFINITY),
        field_float: Some(42.0_f32),
        field_float_inf: Some(f32::INFINITY),
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
        "fieldDoubleNegInf": "-Infinity",
        "fieldFloat":  42_f32,
        "fieldFloatInf": "Infinity",
        "fieldInt64":  "42",
        "fieldUint64": "42",
        "fieldInt32":  42,
        "fieldUint32": 42,
        "fieldBool":   true,
        "fieldString": "zebras are more fun than foxes",
        "fieldBytes":  "YnV0IHplYnJhcyBhcmUgdmV4aW5n",
    });
    assert_eq!(json, want);

    let roundtrip = serde_json::from_value::<Helper>(json)?;
    assert_eq!(input, roundtrip);
    Ok(())
}

#[test]
fn serialize_in_repeated() -> Result {
    let input = Repeated {
        field_double: vec![42.0_f64, f64::NEG_INFINITY],
        field_float: vec![42.0_f32, f32::INFINITY],
        field_int64: vec![42_i64],
        field_uint64: vec![42_u64],
        field_int32: vec![42_i32],
        field_uint32: vec![42_u32],
        field_bool: vec![true],
        field_string: vec!["zebras are more fun than foxes".to_string()],
        field_bytes: vec![bytes::Bytes::from_static(
            "but zebras are vexing".as_bytes(),
        )],
    };
    let json = serde_json::to_value(&input)?;
    let want = json!({
        "fieldDouble":  [42_f64, "-Infinity"],
        "fieldFloat":   [42_f32, "Infinity"],
        "fieldInt64":   ["42"],
        "fieldUint64":  ["42"],
        "fieldInt32":   [42],
        "fieldUint32":  [42],
        "fieldBool":    [true],
        "fieldString":  ["zebras are more fun than foxes"],
        "fieldBytes":   ["YnV0IHplYnJhcyBhcmUgdmV4aW5n"],
    });
    assert_eq!(json, want);

    let roundtrip = serde_json::from_value::<Repeated>(json)?;
    assert_eq!(input, roundtrip);
    Ok(())
}
