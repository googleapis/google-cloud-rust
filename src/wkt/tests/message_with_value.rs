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
    use google_cloud_wkt::Value;
    use serde_json::json;
    use test_case::test_case;

    type Result = anyhow::Result<()>;

    #[allow(dead_code)]
    mod protos {
        use google_cloud_wkt as wkt;
        include!("generated/mod.rs");
    }
    use protos::MessageWithValue;

    #[test_case(json!({"singular": null}), Value::Null)]
    #[test_case(json!({"singular": "abc"}), json!("abc"))]
    #[test_case(json!({"singular": 1}), json!(1))]
    #[test_case(json!({"singular": true}), json!(true))]
    #[test_case(json!({"singular": [1, 2, "a"]}), json!([1, 2, "a"]))]
    #[test_case(json!({"singular": {"a": 1}}), json!({"a": 1}))]
    fn test_singular(value: Value, want: Value) -> Result {
        let got = serde_json::from_value::<MessageWithValue>(value.clone())?;
        assert_eq!(got.singular, Some(want));
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, value);
        Ok(())
    }

    #[test_case(json!({}))]
    fn test_singular_default(input: Value) -> Result {
        let got = serde_json::from_value::<MessageWithValue>(input)?;
        let want = MessageWithValue::default();
        assert_eq!(got, want);
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, json!({}));
        Ok(())
    }

    #[test_case(json!({"optional": ""}), json!(""))]
    #[test_case(json!({"optional": null}), json!(null))]
    fn test_optional(value: Value, want: Value) -> Result {
        let got = serde_json::from_value::<MessageWithValue>(value.clone())?;
        assert_eq!(got.optional, Some(want));
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, value);
        Ok(())
    }

    #[test_case(json!({}))]
    fn test_optional_default(input: Value) -> Result {
        let got = serde_json::from_value::<MessageWithValue>(input)?;
        let want = MessageWithValue::default();
        assert_eq!(got, want);
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, json!({}));
        Ok(())
    }

    #[test_case(json!({"repeated": [""]}), MessageWithValue::new().set_repeated([json!("")]))]
    #[test_case(json!({"repeated": [1, 2, "a"]}), MessageWithValue::new().set_repeated([json!(1), json!(2), json!("a")]))]
    fn test_repeated(value: Value, want: MessageWithValue) -> Result {
        let got = serde_json::from_value::<MessageWithValue>(value.clone())?;
        assert_eq!(got, want);
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, value);
        Ok(())
    }

    #[test_case(json!({}))]
    #[test_case(json!({"repeated": []}))]
    #[test_case(json!({"repeated": null}))]
    fn test_repeated_default(input: Value) -> Result {
        let got = serde_json::from_value::<MessageWithValue>(input)?;
        let want = MessageWithValue::default();
        assert_eq!(got, want);
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, json!({}));
        Ok(())
    }

    #[test_case(json!({"map": {"key": ""}}), MessageWithValue::new().set_map([("key", json!(""))]))]
    #[test_case(json!({"map": {"key": [1, 2, "a"]}}), MessageWithValue::new().set_map([("key", json!([1, 2, "a"]))]))]
    fn test_map(value: Value, want: MessageWithValue) -> Result {
        let got = serde_json::from_value::<MessageWithValue>(value.clone())?;
        assert_eq!(got, want);
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, value);
        Ok(())
    }

    #[test_case(json!({}))]
    #[test_case(json!({"map": {}}))]
    #[test_case(json!({"map": null}))]
    fn test_map_default(input: Value) -> Result {
        let got = serde_json::from_value::<MessageWithValue>(input)?;
        let want = MessageWithValue::default();
        assert_eq!(got, want);
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, json!({}));
        Ok(())
    }
}
