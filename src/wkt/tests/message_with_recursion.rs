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
    use common::{
        __MessageWithRecursion, MessageWithRecursion,
        message_with_recursion::{Level0, NonRecursive},
    };
    use serde_json::{Value, json};
    use test_case::test_case;
    type Result = anyhow::Result<()>;

    fn test_level_0() -> Level0 {
        Level0::new().set_side(NonRecursive::new().set_value("abc"))
    }

    #[test_case(MessageWithRecursion::new(), json!({}))]
    #[test_case(MessageWithRecursion::new().set_singular(test_level_0()), json!({"singular": {"side": {"value": "abc"}}}))]
    #[test_case(MessageWithRecursion::new().set_optional(Level0::new()), json!({"optional": {}}))]
    #[test_case(MessageWithRecursion::new().set_or_clear_optional(None::<Level0>), json!({}))]
    #[test_case(MessageWithRecursion::new().set_optional(test_level_0()), json!({"optional": {"side": {"value": "abc"}}}))]
    #[test_case(MessageWithRecursion::new().set_repeated([Level0::new()]), json!({"repeated": [{}]}))]
    #[test_case(MessageWithRecursion::new().set_map([("test", test_level_0())]), json!({"map": {"test": {"side": {"value": "abc"}}}} ))]
    fn test_ser(input: MessageWithRecursion, want: Value) -> Result {
        let got = serde_json::to_value(__MessageWithRecursion(input))?;
        assert_eq!(got, want);
        Ok(())
    }

    #[test_case(MessageWithRecursion::new(), json!({}))]
    #[test_case(MessageWithRecursion::new().set_singular(test_level_0()), json!({"singular": {"side": {"value": "abc"}}}))]
    #[test_case(MessageWithRecursion::new().set_optional(Level0::new()), json!({"optional": {}}))]
    #[test_case(MessageWithRecursion::new().set_or_clear_optional(None::<Level0>), json!({}))]
    #[test_case(MessageWithRecursion::new().set_optional(test_level_0()), json!({"optional": {"side": {"value": "abc"}}}))]
    #[test_case(MessageWithRecursion::new().set_repeated([Level0::new()]), json!({"repeated": [{}]}))]
    #[test_case(MessageWithRecursion::new().set_map([("test", test_level_0())]), json!({"map": {"test": {"side": {"value": "abc"}}}} ))]
    fn test_de(want: MessageWithRecursion, input: Value) -> Result {
        let got = serde_json::from_value::<__MessageWithRecursion>(input)?;
        assert_eq!(got.0, want);
        Ok(())
    }

    #[test_case(r#"{"singular": {}, "singular": {}}"#)]
    #[test_case(r#"{"optional": {}, "optional": {}}"#)]
    #[test_case(r#"{"repeated": [], "repeated": []}"#)]
    #[test_case(r#"{"map":      {}, "map":      {}}"#)]
    fn reject_duplicate_fields(input: &str) -> Result {
        let err = serde_json::from_str::<__MessageWithRecursion>(input).unwrap_err();
        assert!(err.is_data(), "{err:?}");
        Ok(())
    }

    #[test_case(json!({"unknown": "test-value"}))]
    #[test_case(json!({"unknown": "test-value", "moreUnknown": {"a": 1, "b": 2}}))]
    fn test_unknown(input: Value) -> Result {
        let deser = serde_json::from_value::<__MessageWithRecursion>(input.clone())?;
        let got = serde_json::to_value(deser)?;
        assert_eq!(got, input);
        Ok(())
    }

    #[test_case(json!({"singular": {}}), Level0::default())]
    #[test_case(json!({"singular": {"side": {"value": "abc"}}}), test_level_0())]
    fn test_singular(value: Value, want: Level0) -> Result {
        let got = serde_json::from_value::<MessageWithRecursion>(value.clone())?;
        assert_eq!(got.singular, Some(Box::new(want)));
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, value);
        Ok(())
    }

    #[test_case(json!({}))]
    #[test_case(json!({"singular": null}))]
    fn test_singular_default(input: Value) -> Result {
        let got = serde_json::from_value::<MessageWithRecursion>(input)?;
        let want = MessageWithRecursion::default();
        assert_eq!(got, want);
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, json!({}));
        Ok(())
    }

    #[test_case(json!({"optional": {}}), Level0::default())]
    #[test_case(json!({"optional": {"side": {"value": "abc"}}}), test_level_0())]
    fn test_optional(value: Value, want: Level0) -> Result {
        let got = serde_json::from_value::<MessageWithRecursion>(value.clone())?;
        assert_eq!(got.optional, Some(Box::new(want)));
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, value);
        Ok(())
    }

    #[test_case(json!({}))]
    #[test_case(json!({"optional": null}))]
    fn test_optional_default(input: Value) -> Result {
        let got = serde_json::from_value::<MessageWithRecursion>(input)?;
        let want = MessageWithRecursion::default();
        assert_eq!(got, want);
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, json!({}));
        Ok(())
    }

    #[test_case(json!({"repeated": [{}]}), MessageWithRecursion::new().set_repeated([Level0::default()]))]
    #[test_case(json!({"repeated": [{"side": {"value": "abc"}}]}), MessageWithRecursion::new().set_repeated([test_level_0()]))]
    fn test_repeated(value: Value, want: MessageWithRecursion) -> Result {
        let got = serde_json::from_value::<MessageWithRecursion>(value.clone())?;
        assert_eq!(got, want);
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, value);
        Ok(())
    }

    #[test_case(json!({}))]
    #[test_case(json!({"repeated": []}))]
    #[test_case(json!({"repeated": null}))]
    fn test_repeated_default(input: Value) -> Result {
        let got = serde_json::from_value::<MessageWithRecursion>(input)?;
        let want = MessageWithRecursion::default();
        assert_eq!(got, want);
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, json!({}));
        Ok(())
    }

    #[test_case(json!({"map": {"key": {}}}), MessageWithRecursion::new().set_map([("key", Level0::default())]))]
    #[test_case(json!({"map": {"key": {"side": {"value": "abc"}}}}), MessageWithRecursion::new().set_map([("key", test_level_0())]))]
    fn test_map(value: Value, want: MessageWithRecursion) -> Result {
        let got = serde_json::from_value::<MessageWithRecursion>(value.clone())?;
        assert_eq!(got, want);
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, value);
        Ok(())
    }

    #[test_case(json!({}))]
    #[test_case(json!({"map": {}}))]
    #[test_case(json!({"map": null}))]
    fn test_map_default(input: Value) -> Result {
        let got = serde_json::from_value::<MessageWithRecursion>(input)?;
        let want = MessageWithRecursion::default();
        assert_eq!(got, want);
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, json!({}));
        Ok(())
    }
}
