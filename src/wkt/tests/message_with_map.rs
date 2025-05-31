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

#[cfg(test)]
mod test {
    use serde_json::json;
    use std::collections::HashMap;
    type Result = std::result::Result<(), Box<dyn std::error::Error>>;

    #[serde_with::serde_as]
    #[serde_with::skip_serializing_none]
    #[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct MessageWithMap {
        #[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]
        pub map: HashMap<String, String>,

        #[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]
        #[serde_as(as = "HashMap<_, google_cloud_wkt::internal::I64>")]
        pub map_i64: HashMap<String, i64>,

        #[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]
        pub map_i64_key: HashMap<i64, String>,

        #[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]
        #[serde_as(as = "HashMap<_, serde_with::base64::Base64>")]
        pub map_bytes: HashMap<String, bytes::Bytes>,
    }

    #[test]
    fn test_empty() -> Result {
        let msg = MessageWithMap {
            map: HashMap::new(),
            ..Default::default()
        };
        let got = serde_json::to_value(&msg)?;
        let want = json!({});
        assert_eq!(want, got);

        let roundtrip = serde_json::from_value::<MessageWithMap>(got)?;
        assert_eq!(msg, roundtrip);
        Ok(())
    }

    #[test]
    fn test_non_empty() -> Result {
        let msg = MessageWithMap {
            map: HashMap::from(
                [("k1", "v1"), ("k2", "v2")].map(|(k, v)| (k.to_string(), v.to_string())),
            ),
            ..Default::default()
        };
        let got = serde_json::to_value(&msg)?;
        let want = json!({"map": { "k1": "v1", "k2": "v2" }});
        assert_eq!(want, got);

        let roundtrip = serde_json::from_value::<MessageWithMap>(got)?;
        assert_eq!(msg, roundtrip);
        Ok(())
    }

    // 1 << 60 is too large to be represented as a JSON number, those are
    // always IEEE 754 double precision floating point numbers, which only
    // has about 52 bits of mantissa.
    const TEST_I64: i64 = 1_i64 << 60;

    #[test]
    fn test_i64() -> Result {
        let msg = MessageWithMap {
            map_i64: HashMap::from([("k1".to_string(), TEST_I64)]),
            ..Default::default()
        };
        let got = serde_json::to_value(&msg)?;
        let want = json!({"mapI64": { "k1": format!("{TEST_I64}") }});
        assert_eq!(want, got);

        let roundtrip = serde_json::from_value::<MessageWithMap>(got)?;
        assert_eq!(msg, roundtrip);
        Ok(())
    }

    #[test]
    fn test_i64_key() -> Result {
        let msg = MessageWithMap {
            map_i64_key: HashMap::from([(TEST_I64, "v1".to_string())]),
            ..Default::default()
        };
        let got = serde_json::to_value(&msg)?;
        let want = json!({"mapI64Key": { format!("{TEST_I64}"): "v1" }});
        assert_eq!(want, got);

        let roundtrip = serde_json::from_value::<MessageWithMap>(got)?;
        assert_eq!(msg, roundtrip);
        Ok(())
    }

    #[test]
    fn test_bytes() -> Result {
        let b = bytes::Bytes::from("the quick brown fox jumps over the laze dog");
        let msg = MessageWithMap {
            map_bytes: HashMap::from([("k1".to_string(), b)]),
            ..Default::default()
        };
        let got = serde_json::to_value(&msg)?;
        let want = json!({"mapBytes": { "k1": "dGhlIHF1aWNrIGJyb3duIGZveCBqdW1wcyBvdmVyIHRoZSBsYXplIGRvZw==" }});
        assert_eq!(want, got);

        let roundtrip = serde_json::from_value::<MessageWithMap>(got)?;
        assert_eq!(msg, roundtrip);
        Ok(())
    }
}
