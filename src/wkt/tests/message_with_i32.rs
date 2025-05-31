// Copyright 2025 Google LLC
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
    pub struct MessageWithI32 {
        #[serde(skip_serializing_if = "google_cloud_wkt::internal::is_default")]
        #[serde_as(as = "google_cloud_wkt::internal::I32")]
        pub singular: i32,

        #[serde(skip_serializing_if = "std::option::Option::is_none")]
        #[serde_as(as = "Option<google_cloud_wkt::internal::I32>")]
        pub optional: Option<i32>,

        #[serde(skip_serializing_if = "Vec::is_empty")]
        #[serde_as(as = "Vec<google_cloud_wkt::internal::I32>")]
        pub repeated: Vec<i32>,

        #[serde(skip_serializing_if = "std::collections::HashMap::is_empty")]
        #[serde_as(as = "std::collections::HashMap<_, google_cloud_wkt::internal::I32>")]
        pub map_value: std::collections::HashMap<String, i32>,

        #[serde(skip_serializing_if = "std::collections::HashMap::is_empty")]
        #[serde_as(as = "std::collections::HashMap<google_cloud_wkt::internal::I32, _>")]
        pub map_key: std::collections::HashMap<i32, String>,

        #[serde(skip_serializing_if = "std::collections::HashMap::is_empty")]
        #[serde_as(
            as = "std::collections::HashMap<google_cloud_wkt::internal::I32, google_cloud_wkt::internal::I32>"
        )]
        pub map_key_value: std::collections::HashMap<i32, i32>,
    }

    #[test_case("123", 123)]
    #[test_case(456, 456)]
    #[test_case("-789", -789)]
    fn test_singular<T>(input: T, want: i32) -> Result
    where
        T: serde::ser::Serialize,
    {
        let value = json!({"singular": input});
        let got = serde_json::from_value::<MessageWithI32>(value)?;
        let output = json!({"singular": want});
        assert_eq!(
            got,
            MessageWithI32 {
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
        let want = MessageWithI32 {
            singular: 0,
            ..Default::default()
        };
        let got = serde_json::from_value::<MessageWithI32>(input)?;
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
    fn test_optional<T>(input: T, want: i32) -> Result
    where
        T: serde::ser::Serialize,
    {
        let value = json!({"optional": input});
        let got = serde_json::from_value::<MessageWithI32>(value)?;
        let output = json!({"optional": want});
        assert_eq!(
            got,
            MessageWithI32 {
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
        let want = MessageWithI32 {
            optional: None,
            ..Default::default()
        };
        let got = serde_json::from_value::<MessageWithI32>(input)?;
        assert_eq!(got, want);
        Ok(())
    }

    #[test_case(0, 0)]
    #[test_case("0", 0; "zero as string")]
    #[test_case("123", 123)]
    #[test_case(456, 456)]
    #[test_case("-789", -789)]
    fn test_repeated<T>(input: T, want: i32) -> Result
    where
        T: serde::ser::Serialize,
    {
        let value = json!({"repeated": [input]});
        let got = serde_json::from_value::<MessageWithI32>(value)?;
        let output = json!({"repeated": [want]});
        assert_eq!(
            got,
            MessageWithI32 {
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
        let want = MessageWithI32 {
            repeated: vec![],
            ..Default::default()
        };
        let got = serde_json::from_value::<MessageWithI32>(input)?;
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
    fn test_map_value<T>(input: T, want: i32) -> Result
    where
        T: serde::ser::Serialize,
    {
        let value = json!({"mapValue": {"test": input}});
        let got = serde_json::from_value::<MessageWithI32>(value)?;
        let output = json!({"mapValue": {"test": want}});
        assert_eq!(
            got,
            MessageWithI32 {
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
        let want = MessageWithI32::default();
        let got = serde_json::from_value::<MessageWithI32>(input)?;
        assert_eq!(got, want);
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, json!({}));
        Ok(())
    }

    #[test_case("0", 0)]
    #[test_case("123", 123)]
    #[test_case("-789", -789)]
    fn test_map_key<T>(input: T, want: i32) -> Result
    where
        T: Into<String>,
    {
        let value = json!({"mapKey": {input: "test"}});
        let got = serde_json::from_value::<MessageWithI32>(value)?;
        let output = json!({"mapKey": {want.to_string(): "test"}});
        assert_eq!(
            got,
            MessageWithI32 {
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
        let want = MessageWithI32::default();
        let got = serde_json::from_value::<MessageWithI32>(input)?;
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
    fn test_map_key_value<K, V>(key: K, value: V, want_key: i32, want_value: i32) -> Result
    where
        K: Into<String>,
        V: serde::Serialize,
    {
        let value = json!({"mapKeyValue": {key: value}});
        let got = serde_json::from_value::<MessageWithI32>(value)?;
        let output = json!({"mapKeyValue": {want_key.to_string(): want_value}});
        assert_eq!(
            got,
            MessageWithI32 {
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
        let want = MessageWithI32::default();
        let got = serde_json::from_value::<MessageWithI32>(input)?;
        assert_eq!(got, want);
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, json!({}));
        Ok(())
    }
}
