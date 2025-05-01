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
    type Result = std::result::Result<(), Box<dyn std::error::Error>>;

    #[serde_with::serde_as]
    #[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]
    #[serde(default, rename_all = "camelCase")]
    pub struct MessageWithI64 {
        #[serde(skip_serializing_if = "google_cloud_wkt::internal::is_default")]
        #[serde_as(as = "serde_with::DisplayFromStr")]
        pub singular: i64,
        #[serde(skip_serializing_if = "Option::is_none")]
        #[serde_as(as = "Option<serde_with::DisplayFromStr>")]
        pub optional: Option<i64>,
        #[serde(skip_serializing_if = "Vec::is_empty")]
        #[serde_as(as = "Vec<serde_with::DisplayFromStr>")]
        pub repeated: Vec<i64>,
    }

    // 1 << 60 is too large to be represented as a JSON number, those are
    // always IEEE 754 double precision floating point numbers, which only
    // has about 52 bits of mantissa.
    const TEST_VALUE: i64 = 1_i64 << 60;

    #[test]
    fn test_singular() -> Result {
        let msg = MessageWithI64 {
            singular: TEST_VALUE,
            ..Default::default()
        };
        let got = serde_json::to_value(&msg)?;
        let want = json!({"singular": format!("{TEST_VALUE}")});
        assert_eq!(want, got);

        let roundtrip = serde_json::from_value::<MessageWithI64>(got)?;
        assert_eq!(msg, roundtrip);
        Ok(())
    }

    #[test]
    fn test_optional() -> Result {
        let msg = MessageWithI64 {
            optional: Some(TEST_VALUE),
            ..Default::default()
        };
        let got = serde_json::to_value(&msg)?;
        let want = json!({"optional": format!("{TEST_VALUE}")});
        assert_eq!(want, got);

        let roundtrip = serde_json::from_value::<MessageWithI64>(got)?;
        assert_eq!(msg, roundtrip);
        Ok(())
    }

    #[test]
    fn test_repeated() -> Result {
        let msg = MessageWithI64 {
            repeated: vec![TEST_VALUE],
            ..Default::default()
        };
        let got = serde_json::to_value(&msg)?;
        let want = json!({"repeated": [format!("{TEST_VALUE}")]});
        assert_eq!(want, got);

        let roundtrip = serde_json::from_value::<MessageWithI64>(got)?;
        assert_eq!(msg, roundtrip);
        Ok(())
    }
}
