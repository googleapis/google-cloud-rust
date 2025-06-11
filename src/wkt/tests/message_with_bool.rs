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
    use common::{__MessageWithBool, MessageWithBool};
    use serde_json::{Value, json};
    use test_case::test_case;
    type Result = anyhow::Result<()>;

    #[test_case(MessageWithBool::new(), json!({}))]
    #[test_case(MessageWithBool::new().set_singular(false), json!({}))]
    #[test_case(MessageWithBool::new().set_singular(true), json!({"singular": true}))]
    #[test_case(MessageWithBool::new().set_optional(false), json!({"optional": false}))]
    #[test_case(MessageWithBool::new().set_or_clear_optional(None::<bool>), json!({}))]
    #[test_case(MessageWithBool::new().set_repeated([false; 0]), json!({}))]
    #[test_case(MessageWithBool::new().set_repeated([true, true, false]), json!({"repeated": [true, true, false]}))]
    fn test_ser(input: MessageWithBool, want: Value) -> Result {
        let got = serde_json::to_value(__MessageWithBool(input))?;
        assert_eq!(got, want);
        Ok(())
    }

    #[test_case(MessageWithBool::new(), json!({}))]
    #[test_case(MessageWithBool::new().set_singular(false), json!({}))]
    #[test_case(MessageWithBool::new().set_singular(true), json!({"singular": true}))]
    #[test_case(MessageWithBool::new().set_optional(false), json!({"optional": false}))]
    #[test_case(MessageWithBool::new().set_or_clear_optional(None::<bool>), json!({}))]
    #[test_case(MessageWithBool::new().set_repeated([false; 0]), json!({}))]
    #[test_case(MessageWithBool::new().set_repeated([true, true, false]), json!({"repeated": [true, true, false]}))]
    fn test_de(want: MessageWithBool, input: Value) -> Result {
        let got = serde_json::from_value::<__MessageWithBool>(input)?;
        assert_eq!(got.0, want);
        Ok(())
    }

    #[test_case(json!({"unknown": "test-value"}))]
    #[test_case(json!({"unknown": "test-value", "moreUnknown": {"a": 1, "b": 2}}))]
    fn test_unknown(input: Value) -> Result {
        let deser = serde_json::from_value::<__MessageWithBool>(input.clone())?;
        let got = serde_json::to_value(deser)?;
        assert_eq!(got, input);
        Ok(())
    }

    #[test]
    fn test_singular() -> Result {
        let value = json!({"singular": true});
        let got = serde_json::from_value::<MessageWithBool>(value)?;
        let output = json!({"singular": true});
        assert_eq!(got, MessageWithBool::new().set_singular(true));
        let trip = serde_json::to_value(&got)?;
        assert_eq!(trip, output);
        Ok(())
    }

    #[test_case(json!({}))]
    #[test_case(json!({"singular": false}))]
    #[test_case(json!({"singular": null}))]
    fn test_singular_default(input: Value) -> Result {
        let want = MessageWithBool::new().set_singular(false);
        let got = serde_json::from_value::<MessageWithBool>(input)?;
        assert_eq!(got, want);
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, json!({}));
        Ok(())
    }

    #[test_case(true)]
    #[test_case(false)]
    fn test_optional(input: bool) -> Result {
        let value = json!({"optional": input});
        let got = serde_json::from_value::<MessageWithBool>(value)?;
        let output = json!({"optional": input});
        assert_eq!(got, MessageWithBool::new().set_optional(input));
        let trip = serde_json::to_value(&got)?;
        assert_eq!(trip, output);
        Ok(())
    }

    #[test_case(json!({}))]
    #[test_case(json!({"optional": null}))]
    fn test_optional_none(input: Value) -> Result {
        let want = MessageWithBool::new().set_or_clear_optional(None::<bool>);
        let got = serde_json::from_value::<MessageWithBool>(input)?;
        assert_eq!(got, want);
        Ok(())
    }

    #[test_case(true)]
    fn test_repeated(input: bool) -> Result {
        let value = json!({"repeated": [input]});
        let got = serde_json::from_value::<MessageWithBool>(value)?;
        let output = json!({"repeated": [input]});
        assert_eq!(got, MessageWithBool::new().set_repeated([input]));
        let trip = serde_json::to_value(&got)?;
        assert_eq!(trip, output);
        Ok(())
    }

    #[test_case(json!({}))]
    #[test_case(json!({"repeated": []}))]
    #[test_case(json!({"repeated": null}))]
    fn test_repeated_default(input: Value) -> Result {
        let want = MessageWithBool::new();
        let got = serde_json::from_value::<MessageWithBool>(input)?;
        assert_eq!(got, want);
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, json!({}));
        Ok(())
    }

    #[test_case(true)]
    #[test_case(false)]
    fn test_map_value(input: bool) -> Result {
        let value = json!({"mapValue": {"test": input}});
        let got = serde_json::from_value::<MessageWithBool>(value)?;
        let output = json!({"mapValue": {"test": input}});
        assert_eq!(
            got,
            MessageWithBool::new().set_map_value([("test".to_string(), input)])
        );
        let trip = serde_json::to_value(&got)?;
        assert_eq!(trip, output);
        Ok(())
    }

    #[test_case(json!({}))]
    #[test_case(json!({"mapValue": {}}))]
    #[test_case(json!({"mapValue": null}))]
    fn test_map_value_default(input: Value) -> Result {
        let want = MessageWithBool::default();
        let got = serde_json::from_value::<MessageWithBool>(input)?;
        assert_eq!(got, want);
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, json!({}));
        Ok(())
    }

    #[test_case("true", true)]
    #[test_case("false", false)]
    fn test_map_key(input: &str, want: bool) -> Result {
        let value = json!({"mapKey": {input: "test"}});
        let got = serde_json::from_value::<MessageWithBool>(value)?;
        let output = json!({"mapKey": {want.to_string(): "test"}});
        assert_eq!(
            got,
            MessageWithBool::new().set_map_key([(want, "test".to_string())])
        );
        let trip = serde_json::to_value(&got)?;
        assert_eq!(trip, output);
        Ok(())
    }

    #[test_case(json!({}))]
    #[test_case(json!({"mapKey": {}}))]
    #[test_case(json!({"mapKey": null}))]
    fn test_map_key_default(input: Value) -> Result {
        let want = MessageWithBool::default();
        let got = serde_json::from_value::<MessageWithBool>(input)?;
        assert_eq!(got, want);
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, json!({}));
        Ok(())
    }

    #[test_case("true", false, true, false)]
    #[test_case("false", false, false, false)]
    fn test_map_key_value<K, V>(key: K, value: V, want_key: bool, want_value: bool) -> Result
    where
        K: Into<String>,
        V: serde::Serialize,
    {
        let value = json!({"mapKeyValue": {key: value}});
        let got = serde_json::from_value::<MessageWithBool>(value)?;
        let output = json!({"mapKeyValue": {want_key.to_string(): want_value}});
        assert_eq!(
            got,
            MessageWithBool::new().set_map_key_value([(want_key, want_value)])
        );
        let trip = serde_json::to_value(&got)?;
        assert_eq!(trip, output);
        Ok(())
    }

    #[test_case(json!({}))]
    #[test_case(json!({"mapKeyValue": {}}))]
    #[test_case(json!({"mapKeyValue": null}))]
    fn test_map_key_value_default(input: Value) -> Result {
        let want = MessageWithBool::default();
        let got = serde_json::from_value::<MessageWithBool>(input)?;
        assert_eq!(got, want);
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, json!({}));
        Ok(())
    }
}
