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
    use common::{__MessageWithString, MessageWithString};
    use serde_json::{Value, json};
    use test_case::test_case;
    type Result = anyhow::Result<()>;

    #[test_case(MessageWithString::new(), json!({}))]
    #[test_case(MessageWithString::new().set_singular(""), json!({}))]
    #[test_case(MessageWithString::new().set_singular("abc"), json!({"singular": "abc"}))]
    #[test_case(MessageWithString::new().set_optional(""), json!({"optional": ""}))]
    #[test_case(MessageWithString::new().set_optional("abc"), json!({"optional": "abc"}))]
    #[test_case(MessageWithString::new().set_or_clear_optional(None::<String>), json!({}))]
    #[test_case(MessageWithString::new().set_repeated(["a", "b", "c"]), json!({"repeated": ["a", "b", "c"]}))]
    #[test_case(MessageWithString::new().set_map_key_value([("a", "1"), ("b", "2")]), json!({"mapKeyValue": {"a": "1", "b": "2"}}))]
    fn test_ser(input: MessageWithString, want: Value) -> Result {
        let got = serde_json::to_value(__MessageWithString(input))?;
        assert_eq!(got, want);
        Ok(())
    }

    #[test_case(MessageWithString::new(), json!({}))]
    #[test_case(MessageWithString::new().set_singular(""), json!({}))]
    #[test_case(MessageWithString::new().set_singular("abc"), json!({"singular": "abc"}))]
    #[test_case(MessageWithString::new().set_optional(""), json!({"optional": ""}))]
    #[test_case(MessageWithString::new().set_optional("abc"), json!({"optional": "abc"}))]
    #[test_case(MessageWithString::new().set_or_clear_optional(None::<String>), json!({}))]
    #[test_case(MessageWithString::new().set_repeated(["a", "b", "c"]), json!({"repeated": ["a", "b", "c"]}))]
    #[test_case(MessageWithString::new().set_map_key_value([("a", "1"), ("b", "2")]), json!({"mapKeyValue": {"a": "1", "b": "2"}}))]
    #[test_case(MessageWithString::new(), json!({"singular": null}))]
    #[test_case(MessageWithString::new(), json!({"repeated": null}))]
    #[test_case(MessageWithString::new(), json!({"mapKey": null}))]
    #[test_case(MessageWithString::new(), json!({"mapValue": null}))]
    #[test_case(MessageWithString::new(), json!({"mapKeyValue": null}))]
    fn test_de(want: MessageWithString, input: Value) -> Result {
        let got = serde_json::from_value::<__MessageWithString>(input)?;
        assert_eq!(got.0, want);
        Ok(())
    }

    #[test_case(json!({"unknown": "test-value"}))]
    #[test_case(json!({"unknown": "test-value", "moreUnknown": {"a": 1, "b": 2}}))]
    fn test_unknown(input: Value) -> Result {
        let deser = serde_json::from_value::<__MessageWithString>(input.clone())?;
        let got = serde_json::to_value(deser)?;
        assert_eq!(got, input);
        Ok(())
    }

    #[test_case("the quick brown fox jumps over the lazy dog")]
    #[test_case(concat!("Benjamín pidió una bebida de kiwi y fresa. ",
            "Noé, sin vergüenza, la más exquisita champaña del menú"))]
    fn test_singular(input: &str) -> Result {
        let value = json!({"singular": input});
        let got = serde_json::from_value::<MessageWithString>(value)?;
        let output = json!({"singular": input});
        assert_eq!(got, MessageWithString::new().set_singular(input));
        let trip = serde_json::to_value(&got)?;
        assert_eq!(trip, output);
        Ok(())
    }

    #[test_case(json!({}))]
    #[test_case(json!({"singular": ""}))]
    #[test_case(json!({"singular": null}))]
    fn test_singular_default(input: Value) -> Result {
        let want = MessageWithString::new().set_singular("");
        let got = serde_json::from_value::<MessageWithString>(input)?;
        assert_eq!(got, want);
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, json!({}));
        Ok(())
    }

    #[test_case("")]
    #[test_case("abc")]
    fn test_optional(input: &str) -> Result {
        let value = json!({"optional": input});
        let got = serde_json::from_value::<MessageWithString>(value)?;
        let output = json!({"optional": input});
        assert_eq!(got, MessageWithString::new().set_optional(input));
        let trip = serde_json::to_value(&got)?;
        assert_eq!(trip, output);
        Ok(())
    }

    #[test_case(json!({}))]
    #[test_case(json!({"optional": null}))]
    fn test_optional_none(input: Value) -> Result {
        let want = MessageWithString::new().set_or_clear_optional(None::<String>);
        let got = serde_json::from_value::<MessageWithString>(input)?;
        assert_eq!(got, want);
        Ok(())
    }

    #[test_case("")]
    #[test_case("abc")]
    fn test_repeated(input: &str) -> Result {
        let value = json!({"repeated": [input]});
        let got = serde_json::from_value::<MessageWithString>(value)?;
        let output = json!({"repeated": [input]});
        assert_eq!(got, MessageWithString::new().set_repeated([input]));
        let trip = serde_json::to_value(&got)?;
        assert_eq!(trip, output);
        Ok(())
    }

    #[test_case(json!({}))]
    #[test_case(json!({"repeated": []}))]
    #[test_case(json!({"repeated": null}))]
    fn test_repeated_default(input: Value) -> Result {
        let want = MessageWithString::new();
        let got = serde_json::from_value::<MessageWithString>(input)?;
        assert_eq!(got, want);
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, json!({}));
        Ok(())
    }

    #[test_case("abc")]
    #[test_case("")]
    fn test_map_value(input: &str) -> Result {
        let value = json!({"mapValue": {"42": input}});
        let got = serde_json::from_value::<MessageWithString>(value)?;
        let output = json!({"mapValue": {"42": input}});
        assert_eq!(got, MessageWithString::new().set_map_value([(42, input)]));
        let trip = serde_json::to_value(&got)?;
        assert_eq!(trip, output);
        Ok(())
    }

    #[test_case(json!({}))]
    #[test_case(json!({"mapValue": {}}))]
    #[test_case(json!({"mapValue": null}))]
    fn test_map_value_default(input: Value) -> Result {
        let want = MessageWithString::default();
        let got = serde_json::from_value::<MessageWithString>(input)?;
        assert_eq!(got, want);
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, json!({}));
        Ok(())
    }

    #[test_case("k0")]
    #[test_case("")]
    fn test_map_key(input: &str) -> Result {
        let value = json!({"mapKey": {input: 42}});
        let got = serde_json::from_value::<MessageWithString>(value)?;
        let output = json!({"mapKey": {input: 42}});
        assert_eq!(got, MessageWithString::new().set_map_key([(input, 42)]));
        let trip = serde_json::to_value(&got)?;
        assert_eq!(trip, output);
        Ok(())
    }

    #[test_case(json!({}))]
    #[test_case(json!({"mapKey": {}}))]
    #[test_case(json!({"mapKey": null}))]
    fn test_map_key_default(input: Value) -> Result {
        let want = MessageWithString::default();
        let got = serde_json::from_value::<MessageWithString>(input)?;
        assert_eq!(got, want);
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, json!({}));
        Ok(())
    }

    #[test_case("empty", "")]
    #[test_case("not-empty", "abc")]
    fn test_map_key_value(key: &str, value: &str) -> Result {
        let input = json!({"mapKeyValue": {key: value}});
        let got = serde_json::from_value::<MessageWithString>(input)?;
        let output = json!({"mapKeyValue": {key: value}});
        assert_eq!(
            got,
            MessageWithString::new().set_map_key_value([(key, value)])
        );
        let trip = serde_json::to_value(&got)?;
        assert_eq!(trip, output);
        Ok(())
    }

    #[test_case(json!({}))]
    #[test_case(json!({"mapKeyValue": {}}))]
    #[test_case(json!({"mapKeyValue": null}))]
    fn test_map_key_value_default(input: Value) -> Result {
        let want = MessageWithString::default();
        let got = serde_json::from_value::<MessageWithString>(input)?;
        assert_eq!(got, want);
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, json!({}));
        Ok(())
    }
}
