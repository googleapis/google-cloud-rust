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
    use common::{__MessageWithEnum, MessageWithEnum, message_with_enum::TestEnum};
    use serde_json::{Value, json};
    use test_case::test_case;
    type Result = anyhow::Result<()>;

    #[test_case(MessageWithEnum::new(), json!({}))]
    #[test_case(MessageWithEnum::new().set_singular(TestEnum::Unspecified), json!({}))]
    #[test_case(MessageWithEnum::new().set_singular(TestEnum::Red), json!({"singular": 1}))]
    #[test_case(MessageWithEnum::new().set_optional(TestEnum::Unspecified), json!({"optional": 0}))]
    #[test_case(MessageWithEnum::new().set_or_clear_optional(None::<TestEnum>), json!({}))]
    #[test_case(MessageWithEnum::new().set_optional(TestEnum::Red), json!({"optional": 1}))]
    #[test_case(MessageWithEnum::new().set_repeated([TestEnum::Red, TestEnum::Green]), json!({"repeated": [1, 2]}))]
    #[test_case(MessageWithEnum::new().set_map([("red", TestEnum::Red), ("green", TestEnum::Green)]), json!({"map": {"red": 1, "green": 2}}))]
    fn test_ser(input: MessageWithEnum, want: Value) -> Result {
        let got = serde_json::to_value(__MessageWithEnum(input))?;
        assert_eq!(got, want);
        Ok(())
    }

    #[test_case(MessageWithEnum::new(), json!({}))]
    #[test_case(MessageWithEnum::new().set_singular(TestEnum::Unspecified), json!({}))]
    #[test_case(MessageWithEnum::new().set_singular(TestEnum::Red), json!({"singular": 1}))]
    #[test_case(MessageWithEnum::new().set_optional(TestEnum::Unspecified), json!({"optional": 0}))]
    #[test_case(MessageWithEnum::new().set_or_clear_optional(None::<TestEnum>), json!({}))]
    #[test_case(MessageWithEnum::new().set_optional(TestEnum::Red), json!({"optional": 1}))]
    #[test_case(MessageWithEnum::new().set_repeated([TestEnum::Red, TestEnum::Green]), json!({"repeated": [1, 2]}))]
    #[test_case(MessageWithEnum::new().set_map([("red", TestEnum::Red), ("green", TestEnum::Green)]), json!({"map": {"red": 1, "green": 2}}))]
    fn test_de(want: MessageWithEnum, input: Value) -> Result {
        let got = serde_json::from_value::<__MessageWithEnum>(input)?;
        assert_eq!(got.0, want);
        Ok(())
    }

    #[test_case(r#"{"singular": 0,  "singular": 0}"#)]
    #[test_case(r#"{"optional": 0,  "optional": 0}"#)]
    #[test_case(r#"{"repeated": [], "repeated": []}"#)]
    #[test_case(r#"{"map":      {}, "map":      {}}"#)]
    fn reject_duplicate_fields(input: &str) -> Result {
        let err = serde_json::from_str::<__MessageWithEnum>(input).unwrap_err();
        assert!(err.is_data(), "{err:?}");
        Ok(())
    }

    #[test_case(json!({"unknown": "test-value"}))]
    #[test_case(json!({"unknown": "test-value", "moreUnknown": {"a": 1, "b": 2}}))]
    fn test_unknown(input: Value) -> Result {
        let deser = serde_json::from_value::<__MessageWithEnum>(input.clone())?;
        let got = serde_json::to_value(deser)?;
        assert_eq!(got, input);
        Ok(())
    }

    #[test_case(json!({"singular": "RED"}), TestEnum::Red)]
    #[test_case(json!({"singular": 1}), TestEnum::Red)]
    fn test_singular(input: Value, want: TestEnum) -> Result {
        let got = serde_json::from_value::<MessageWithEnum>(input)?;
        assert_eq!(got.singular, want, "{got:?}");
        let roundtrip = serde_json::from_value(serde_json::to_value(&got)?)?;
        assert_eq!(got, roundtrip);
        Ok(())
    }

    #[test_case(json!({}))]
    #[test_case(json!({"singular": null}))]
    #[test_case(json!({"singular": 0}))]
    #[test_case(json!({"singular": "TEST_ENUM_UNSPECIFIED"}))]
    fn test_singular_default(input: Value) -> Result {
        let got = serde_json::from_value::<MessageWithEnum>(input)?;
        assert_eq!(got, MessageWithEnum::default());
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, json!({}));
        Ok(())
    }

    #[test_case(json!({"optional": "RED"}), TestEnum::Red)]
    #[test_case(json!({"optional": 1}), TestEnum::Red)]
    fn test_optional(input: Value, want: TestEnum) -> Result {
        let got = serde_json::from_value::<MessageWithEnum>(input)?;
        assert_eq!(got.optional, Some(want), "{got:?}");
        let roundtrip = serde_json::from_value(serde_json::to_value(&got)?)?;
        assert_eq!(got, roundtrip);
        Ok(())
    }

    #[test_case(json!({}))]
    #[test_case(json!({"optional": null}))]
    fn test_optional_default(input: Value) -> Result {
        let got = serde_json::from_value::<MessageWithEnum>(input)?;
        assert_eq!(got, MessageWithEnum::default());
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, json!({}));
        Ok(())
    }

    #[test]
    fn test_repeated() -> Result {
        let msg = MessageWithEnum::new().set_repeated([TestEnum::Red, TestEnum::Unspecified]);
        let got = serde_json::to_value(&msg)?;
        let want = json!({"repeated": [1, 0]});
        assert_eq!(want, got);
        let roundtrip = serde_json::from_value(serde_json::to_value(&msg)?)?;
        assert_eq!(msg, roundtrip);
        Ok(())
    }

    #[test_case(json!({}))]
    #[test_case(json!({"repeated": []}))]
    #[test_case(json!({"repeated": null}))]
    fn test_repeated_default(input: Value) -> Result {
        let got = serde_json::from_value::<MessageWithEnum>(input)?;
        assert_eq!(got, MessageWithEnum::default());
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, json!({}));
        Ok(())
    }

    #[test]
    fn test_map() -> Result {
        let input = json!({"map": { "favorite": "RED", "not-so-much": "GREEN"}});
        let got = serde_json::from_value::<MessageWithEnum>(input)?;
        let want = MessageWithEnum::new().set_map([
            ("favorite", TestEnum::Red),
            ("not-so-much", TestEnum::Green),
        ]);
        assert_eq!(want, got);
        let roundtrip = serde_json::from_value(serde_json::to_value(&got)?)?;
        assert_eq!(got, roundtrip);
        Ok(())
    }

    #[test_case(json!({}))]
    #[test_case(json!({"map": {}}))]
    #[test_case(json!({"map": null}))]
    fn test_map_default(input: Value) -> Result {
        let got = serde_json::from_value::<MessageWithEnum>(input)?;
        assert_eq!(got, MessageWithEnum::default());
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, json!({}));
        Ok(())
    }
}
