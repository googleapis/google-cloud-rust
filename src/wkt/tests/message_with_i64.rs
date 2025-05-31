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
    use serde_json::{Value, json};
    use std::collections::HashMap;
    use test_case::test_case;

    type Result = anyhow::Result<()>;

    #[serde_with::serde_as]
    #[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]
    #[serde(default, rename_all = "camelCase")]
    pub struct MessageWithI64 {
        #[serde(skip_serializing_if = "google_cloud_wkt::internal::is_default")]
        #[serde_as(as = "google_cloud_wkt::internal::I64")]
        pub singular: i64,

        #[serde(skip_serializing_if = "Option::is_none")]
        #[serde_as(as = "Option<google_cloud_wkt::internal::I64>")]
        pub optional: Option<i64>,

        #[serde(skip_serializing_if = "Vec::is_empty")]
        #[serde_as(as = "Vec<google_cloud_wkt::internal::I64>")]
        pub repeated: Vec<i64>,

        #[serde(skip_serializing_if = "HashMap::is_empty")]
        #[serde_as(as = "HashMap<_, google_cloud_wkt::internal::I64>")]
        pub map_value: HashMap<String, i64>,

        #[serde(skip_serializing_if = "HashMap::is_empty")]
        #[serde_as(as = "HashMap<google_cloud_wkt::internal::I64, _>")]
        pub map_key: HashMap<i64, String>,

        #[serde(skip_serializing_if = "HashMap::is_empty")]
        #[serde_as(
            as = "HashMap<google_cloud_wkt::internal::I64, google_cloud_wkt::internal::I64>"
        )]
        pub map_key_value: HashMap<i64, i64>,
    }

    // 1 << 60 is too large to be represented as a JSON number, those are
    // always IEEE 754 double precision floating point numbers, which only
    // has about 52 bits of mantissa.
    const TEST_VALUE: i64 = 1_i64 << 60;

    #[test_case("123", 123)]
    #[test_case(456, 456)]
    #[test_case("-789", -789)]
    #[test_case(TEST_VALUE, TEST_VALUE)]
    #[test_case(format!("{TEST_VALUE}"), TEST_VALUE)]

    fn test_singular<T>(input: T, want: i64) -> Result
    where
        T: serde::ser::Serialize,
    {
        let value = json!({"singular": input});
        let got = serde_json::from_value::<MessageWithI64>(value)?;
        let output = json!({"singular": want.to_string()});
        assert_eq!(
            got,
            MessageWithI64 {
                singular: want,
                ..Default::default()
            }
        );
        let trip = serde_json::to_value(&got)?;
        assert_eq!(trip, output);
        Ok(())
    }

    #[test_case(json!({"singular": 0}))]
    #[test_case(json!({"singular": "0"}); "string zero")]
    #[test_case(json!({}))]
    fn test_singular_default(input: Value) -> Result {
        let want = MessageWithI64 {
            singular: 0,
            ..Default::default()
        };
        let got = serde_json::from_value::<MessageWithI64>(input)?;
        assert_eq!(got, want);
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, json!({}));
        Ok(())
    }

    #[test_case("123", 123)]
    #[test_case(456, 456)]
    #[test_case("-789", -789)]
    #[test_case(0, 0)]
    #[test_case("0", 0; "string zero")]
    fn test_optional<T>(input: T, want: i64) -> Result
    where
        T: serde::ser::Serialize,
    {
        let value = json!({"optional": input});
        let got = serde_json::from_value::<MessageWithI64>(value)?;
        let output = json!({"optional": want.to_string()});
        assert_eq!(
            got,
            MessageWithI64 {
                optional: Some(want),
                ..Default::default()
            }
        );
        let trip = serde_json::to_value(&got)?;
        assert_eq!(trip, output);
        Ok(())
    }

    #[test_case(json!({}))]
    fn test_optional_none(input: Value) -> Result {
        let want = MessageWithI64 {
            optional: None,
            ..Default::default()
        };
        let got = serde_json::from_value::<MessageWithI64>(input)?;
        assert_eq!(got, want);
        Ok(())
    }

    #[test_case(0, 0)]
    #[test_case("0", 0; "zero as string")]
    #[test_case("123", 123)]
    #[test_case(456, 456)]
    #[test_case("-789", -789)]
    fn test_repeated<T>(input: T, want: i64) -> Result
    where
        T: serde::ser::Serialize,
    {
        let value = json!({"repeated": [input]});
        let got = serde_json::from_value::<MessageWithI64>(value)?;
        let output = json!({"repeated": [want.to_string()]});
        assert_eq!(
            got,
            MessageWithI64 {
                repeated: vec![want],
                ..Default::default()
            }
        );
        let trip = serde_json::to_value(&got)?;
        assert_eq!(trip, output);
        Ok(())
    }

    #[test_case(json!({"repeated": []}))]
    #[test_case(json!({}))]
    fn test_repeated_default(input: Value) -> Result {
        let want = MessageWithI64 {
            repeated: vec![],
            ..Default::default()
        };
        let got = serde_json::from_value::<MessageWithI64>(input)?;
        assert_eq!(got, want);
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, json!({}));
        Ok(())
    }

    #[test_case(0, 0)]
    #[test_case("0", 0; "zero string")]
    #[test_case("123", 123)]
    #[test_case(456, 456)]
    #[test_case("-789", -789)]
    fn test_map_value<T>(input: T, want: i64) -> Result
    where
        T: serde::ser::Serialize,
    {
        let value = json!({"mapValue": {"test": input}});
        let got = serde_json::from_value::<MessageWithI64>(value)?;
        let output = json!({"mapValue": {"test": want.to_string()}});
        assert_eq!(
            got,
            MessageWithI64 {
                map_value: HashMap::from([("test".to_string(), want)]),
                ..Default::default()
            }
        );
        let trip = serde_json::to_value(&got)?;
        assert_eq!(trip, output);
        Ok(())
    }

    #[test_case(json!({"mapValue": {}}))]
    #[test_case(json!({}))]
    fn test_map_value_default(input: Value) -> Result {
        let want = MessageWithI64::default();
        let got = serde_json::from_value::<MessageWithI64>(input)?;
        assert_eq!(got, want);
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, json!({}));
        Ok(())
    }

    #[test_case("0", 0)]
    #[test_case("123", 123)]
    #[test_case("-789", -789)]
    fn test_map_key<T>(input: T, want: i64) -> Result
    where
        T: Into<String>,
    {
        let value = json!({"mapKey": {input: "test"}});
        let got = serde_json::from_value::<MessageWithI64>(value)?;
        let output = json!({"mapKey": {want.to_string(): "test"}});
        assert_eq!(
            got,
            MessageWithI64 {
                map_key: HashMap::from([(want, "test".to_string())]),
                ..Default::default()
            }
        );
        let trip = serde_json::to_value(&got)?;
        assert_eq!(trip, output);
        Ok(())
    }

    #[test_case(json!({"mapKey": {}}))]
    #[test_case(json!({}))]
    fn test_map_key_default(input: Value) -> Result {
        let want = MessageWithI64::default();
        let got = serde_json::from_value::<MessageWithI64>(input)?;
        assert_eq!(got, want);
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, json!({}));
        Ok(())
    }

    #[test_case("0", "0", 0, 0; "string zero")]
    #[test_case("0", 0, 0, 0)]
    #[test_case("123", 234, 123, 234)]
    #[test_case("123", "345", 123, 345)]
    #[test_case("-789", 456, -789, 456)]
    #[test_case("-789", "567", -789, 567)]
    fn test_map_key_value<K, V>(key: K, value: V, want_key: i64, want_value: i64) -> Result
    where
        K: Into<String>,
        V: serde::Serialize,
    {
        let value = json!({"mapKeyValue": {key: value}});
        let got = serde_json::from_value::<MessageWithI64>(value)?;
        let output = json!({"mapKeyValue": {want_key.to_string(): want_value.to_string()}});
        assert_eq!(
            got,
            MessageWithI64 {
                map_key_value: HashMap::from([(want_key, want_value)]),
                ..Default::default()
            }
        );
        let trip = serde_json::to_value(&got)?;
        assert_eq!(trip, output);
        Ok(())
    }

    #[test_case(json!({"mapKeyValue": {}}))]
    #[test_case(json!({}))]
    fn test_map_key_value_default(input: Value) -> Result {
        let want = MessageWithI64::default();
        let got = serde_json::from_value::<MessageWithI64>(input)?;
        assert_eq!(got, want);
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, json!({}));
        Ok(())
    }
}
