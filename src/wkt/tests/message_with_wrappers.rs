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
        __MessageWithBoolValue, __MessageWithBytesValue, __MessageWithDoubleValue,
        __MessageWithInt32Value, __MessageWithInt64Value, __MessageWithStringValue,
        MessageWithBoolValue, MessageWithBytesValue, MessageWithDoubleValue, MessageWithInt32Value,
        MessageWithInt64Value, MessageWithStringValue,
    };
    use serde_json::{Value, json};
    use test_case::test_case;
    type Result = anyhow::Result<()>;

    const LAZY: &str = "the quick brown fox jumps over the lazy dog";
    const LAZY_BYTES: &[u8] = b"the quick brown fox jumps over the lazy dog";
    const LAZY_BASE64: &str = "dGhlIHF1aWNrIGJyb3duIGZveCBqdW1wcyBvdmVyIHRoZSBsYXp5IGRvZw==";

    #[test_case(json!({}), MessageWithBoolValue::new(), json!({}))]
    #[test_case(json!({"singular": null}), MessageWithBoolValue::new(), json!({}))]
    #[test_case(json!({"singular": true}), MessageWithBoolValue::new().set_singular(true), json!({"singular": true}))]
    #[test_case(json!({"repeated": []}), MessageWithBoolValue::new(), json!({}))]
    #[test_case(json!({"repeated": null}), MessageWithBoolValue::new(), json!({}))]
    #[test_case(json!({"repeated": [false]}), MessageWithBoolValue::new().set_repeated([false]), json!({"repeated": [false]}))]
    #[test_case(json!({"map": {}}), MessageWithBoolValue::new(), json!({}))]
    #[test_case(json!({"map": null}), MessageWithBoolValue::new(), json!({}))]
    #[test_case(json!({"map": {"key": true}}), MessageWithBoolValue::new().set_map([("key", true)]), json!({"map": {"key": true}}))]
    fn bool_value_fields(input: Value, want: MessageWithBoolValue, output: Value) -> Result {
        let got = serde_json::from_value::<MessageWithBoolValue>(input)?;
        assert_eq!(got, want);
        let ser = serde_json::to_value(got)?;
        assert_eq!(ser, output);
        Ok(())
    }

    #[test_case(json!({}), MessageWithBoolValue::new(), json!({}))]
    #[test_case(json!({"singular": null}), MessageWithBoolValue::new(), json!({}))]
    #[test_case(json!({"singular": true}), MessageWithBoolValue::new().set_singular(true), json!({"singular": true}))]
    #[test_case(json!({"repeated": []}), MessageWithBoolValue::new(), json!({}))]
    // TODO(#2376) - #[test_case(json!({"repeated": null}), MessageWithBoolValue::new(), json!({}))]
    #[test_case(json!({"repeated": [false]}), MessageWithBoolValue::new().set_repeated([false]), json!({"repeated": [false]}))]
    #[test_case(json!({"map": {}}), MessageWithBoolValue::new(), json!({}))]
    // TODO(#2376) - #[test_case(json!({"map": null}), MessageWithBoolValue::new(), json!({}))]
    #[test_case(json!({"map": {"key": true}}), MessageWithBoolValue::new().set_map([("key", true)]), json!({"map": {"key": true}}))]
    fn generated_bool_value_fields(
        input: Value,
        want: MessageWithBoolValue,
        output: Value,
    ) -> Result {
        let got = serde_json::from_value::<__MessageWithBoolValue>(input)?;
        assert_eq!(got.0, want);
        let ser = serde_json::to_value(got)?;
        assert_eq!(ser, output);
        Ok(())
    }

    #[test_case(json!({}), MessageWithBytesValue::new(), json!({}))]
    #[test_case(json!({"singular": null}), MessageWithBytesValue::new(), json!({}))]
    #[test_case(json!({"singular": LAZY_BASE64}), MessageWithBytesValue::new().set_singular(LAZY_BYTES), json!({"singular": LAZY_BASE64}))]
    #[test_case(json!({"repeated": []}), MessageWithBytesValue::new(), json!({}))]
    #[test_case(json!({"repeated": null}), MessageWithBytesValue::new(), json!({}))]
    #[test_case(json!({"repeated": [LAZY_BASE64]}), MessageWithBytesValue::new().set_repeated([LAZY_BYTES]), json!({"repeated": [LAZY_BASE64]}))]
    #[test_case(json!({"map": {}}), MessageWithBytesValue::new(), json!({}))]
    #[test_case(json!({"map": null}), MessageWithBytesValue::new(), json!({}))]
    // TODO(#2376) - #[test_case(json!({"map": {"key": LAZY_BASE64}}), MessageWithBytesValue::new().set_map([("key", LAZY_BYTES)]), json!({"map": {"key": LAZY_BASE64}}))]
    fn bytes_value_fields(input: Value, want: MessageWithBytesValue, output: Value) -> Result {
        let got = serde_json::from_value::<MessageWithBytesValue>(input)?;
        assert_eq!(got, want);
        let ser = serde_json::to_value(got)?;
        assert_eq!(ser, output);
        Ok(())
    }

    #[test_case(json!({}), MessageWithBytesValue::new(), json!({}))]
    #[test_case(json!({"singular": null}), MessageWithBytesValue::new(), json!({}))]
    #[test_case(json!({"singular": LAZY_BASE64}), MessageWithBytesValue::new().set_singular(LAZY_BYTES), json!({"singular": LAZY_BASE64}))]
    #[test_case(json!({"repeated": []}), MessageWithBytesValue::new(), json!({}))]
    #[test_case(json!({"repeated": null}), MessageWithBytesValue::new(), json!({}))]
    #[test_case(json!({"repeated": [LAZY_BASE64]}), MessageWithBytesValue::new().set_repeated([LAZY_BYTES]), json!({"repeated": [LAZY_BASE64]}))]
    #[test_case(json!({"map": {}}), MessageWithBytesValue::new(), json!({}))]
    #[test_case(json!({"map": null}), MessageWithBytesValue::new(), json!({}))]
    #[test_case(json!({"map": {"key": LAZY_BASE64}}), MessageWithBytesValue::new().set_map([("key", LAZY_BYTES)]), json!({"map": {"key": LAZY_BASE64}}))]
    fn generated_bytes_value_fields(
        input: Value,
        want: MessageWithBytesValue,
        output: Value,
    ) -> Result {
        let got = serde_json::from_value::<__MessageWithBytesValue>(input)?;
        assert_eq!(got.0, want);
        let ser = serde_json::to_value(got)?;
        assert_eq!(ser, output);
        Ok(())
    }

    #[test_case(json!({}), MessageWithDoubleValue::new(), json!({}))]
    #[test_case(json!({"singular": null}), MessageWithDoubleValue::new(), json!({}))]
    #[test_case(json!({"singular": 1.5}), MessageWithDoubleValue::new().set_singular(1.5), json!({"singular": 1.5}))]
    #[test_case(json!({"singular": "2.5"}), MessageWithDoubleValue::new().set_singular(2.5), json!({"singular": 2.5}))]
    #[test_case(json!({"repeated": []}), MessageWithDoubleValue::new(), json!({}))]
    #[test_case(json!({"repeated": null}), MessageWithDoubleValue::new(), json!({}))]
    #[test_case(json!({"repeated": [1.5, "2.5"]}), MessageWithDoubleValue::new().set_repeated([1.5, 2.5]), json!({"repeated": [1.5, 2.5]}))]
    #[test_case(json!({"map": {}}), MessageWithDoubleValue::new(), json!({}))]
    #[test_case(json!({"map": null}), MessageWithDoubleValue::new(), json!({}))]
    // TODO(#2376) - #[test_case(json!({"map": {"a": 1.5, "b": "2.5"}}), MessageWithDoubleValue::new().set_map([("a", 1.5), ("b", 2.5)]), json!({"map": {"a": 1.5, "b": 2.5}}))]
    fn double_value_fields(input: Value, want: MessageWithDoubleValue, output: Value) -> Result {
        let got = serde_json::from_value::<MessageWithDoubleValue>(input)?;
        assert_eq!(got, want);
        let ser = serde_json::to_value(got)?;
        assert_eq!(ser, output);
        Ok(())
    }

    #[test_case(json!({}), MessageWithDoubleValue::new(), json!({}))]
    #[test_case(json!({"singular": null}), MessageWithDoubleValue::new(), json!({}))]
    #[test_case(json!({"singular": 1.5}), MessageWithDoubleValue::new().set_singular(1.5), json!({"singular": 1.5}))]
    #[test_case(json!({"singular": "2.5"}), MessageWithDoubleValue::new().set_singular(2.5), json!({"singular": 2.5}))]
    #[test_case(json!({"repeated": []}), MessageWithDoubleValue::new(), json!({}))]
    #[test_case(json!({"repeated": null}), MessageWithDoubleValue::new(), json!({}))]
    #[test_case(json!({"repeated": [1.5, "2.5"]}), MessageWithDoubleValue::new().set_repeated([1.5, 2.5]), json!({"repeated": [1.5, 2.5]}))]
    #[test_case(json!({"map": {}}), MessageWithDoubleValue::new(), json!({}))]
    #[test_case(json!({"map": null}), MessageWithDoubleValue::new(), json!({}))]
    #[test_case(json!({"map": {"a": 1.5, "b": "2.5"}}), MessageWithDoubleValue::new().set_map([("a", 1.5), ("b", 2.5)]), json!({"map": {"a": 1.5, "b": 2.5}}))]
    fn generated_double_value_fields(
        input: Value,
        want: MessageWithDoubleValue,
        output: Value,
    ) -> Result {
        let got = serde_json::from_value::<__MessageWithDoubleValue>(input)?;
        assert_eq!(got.0, want);
        let ser = serde_json::to_value(got)?;
        assert_eq!(ser, output);
        Ok(())
    }

    #[test_case("Infinity", f64::INFINITY)]
    #[test_case("-Infinity", f64::NEG_INFINITY; "minus inf")]
    #[test_case("NaN", f64::NAN)]
    fn double_value_fields_exceptional(input: &str, value: f64) -> Result {
        use std::cmp::Ordering;
        let input = json!({
            "singular": input,
            "repeated": [input],
            // TODO(#2376) - "map": {"a": input},
        });
        let got = serde_json::from_value::<MessageWithDoubleValue>(input.clone())?;
        assert_eq!(
            got.singular.map(|v| v.total_cmp(&value)),
            Some(Ordering::Equal),
            "{got:?} != {input:?}"
        );
        assert_eq!(
            got.repeated
                .iter()
                .map(|v| v.total_cmp(&value))
                .collect::<Vec<_>>(),
            vec![Ordering::Equal],
            "{got:?} != {input:?}"
        );
        // TODO(#2376) - assert_eq!(got.map.iter().map(|(_, v)| v.total_cmp(&value)).collect::<Vec<_>>(), vec![Ordering::Equal], "{got:?} != {input:?}");
        let output = serde_json::to_value(got)?;
        assert_eq!(output, input);
        Ok(())
    }

    #[test_case("Infinity", f64::INFINITY)]
    #[test_case("-Infinity", f64::NEG_INFINITY; "minus inf")]
    #[test_case("NaN", f64::NAN)]
    fn generated_double_value_fields_exceptional(input: &str, value: f64) -> Result {
        use std::cmp::Ordering;
        let input = json!({
            "singular": input,
            "repeated": [input],
            "map": {"a": input},
        });
        let got = serde_json::from_value::<__MessageWithDoubleValue>(input.clone())?;
        assert_eq!(
            got.0.singular.map(|v| v.total_cmp(&value)),
            Some(Ordering::Equal),
            "{got:?} != {input:?}"
        );
        assert_eq!(
            got.0
                .repeated
                .iter()
                .map(|v| v.total_cmp(&value))
                .collect::<Vec<_>>(),
            vec![Ordering::Equal],
            "{got:?} != {input:?}"
        );
        assert_eq!(
            got.0
                .map
                .values()
                .map(|v| v.total_cmp(&value))
                .collect::<Vec<_>>(),
            vec![Ordering::Equal],
            "{got:?} != {input:?}"
        );
        let output = serde_json::to_value(got)?;
        assert_eq!(output, input);
        Ok(())
    }

    #[test_case(json!({}), MessageWithInt32Value::new(), json!({}))]
    #[test_case(json!({"singular": null}), MessageWithInt32Value::new(), json!({}))]
    #[test_case(json!({"singular": 42}), MessageWithInt32Value::new().set_singular(42), json!({"singular": 42}))]
    #[test_case(json!({"singular": 84.0}), MessageWithInt32Value::new().set_singular(84), json!({"singular": 84}))]
    #[test_case(json!({"singular": "7"}), MessageWithInt32Value::new().set_singular(7), json!({"singular": 7}))]
    #[test_case(json!({"repeated": []}), MessageWithInt32Value::new(), json!({}))]
    #[test_case(json!({"repeated": null}), MessageWithInt32Value::new(), json!({}))]
    #[test_case(json!({"repeated": [42, 84.0, "7"]}), MessageWithInt32Value::new().set_repeated([42, 84, 7]), json!({"repeated": [42, 84, 7]}))]
    #[test_case(json!({"map": {}}), MessageWithInt32Value::new(), json!({}))]
    #[test_case(json!({"map": null}), MessageWithInt32Value::new(), json!({}))]
    // TODO(#2376) - #[test_case(json!({"map": {"a": 42, "b": 84.0, "c": "7"}}), MessageWithInt32Value::new().set_map([("a", 42), ("b", 84), ("c", 7)]), json!({"map": {"a": 42, "b": 84, "c": 7}}))]
    fn int32_value_fields(input: Value, want: MessageWithInt32Value, output: Value) -> Result {
        let got = serde_json::from_value::<MessageWithInt32Value>(input)?;
        assert_eq!(got, want);
        let ser = serde_json::to_value(got)?;
        assert_eq!(ser, output);
        Ok(())
    }

    #[test_case(json!({}), MessageWithInt32Value::new(), json!({}))]
    #[test_case(json!({"singular": null}), MessageWithInt32Value::new(), json!({}))]
    #[test_case(json!({"singular": 42}), MessageWithInt32Value::new().set_singular(42), json!({"singular": 42}))]
    #[test_case(json!({"singular": 84.0}), MessageWithInt32Value::new().set_singular(84), json!({"singular": 84}))]
    #[test_case(json!({"singular": "7"}), MessageWithInt32Value::new().set_singular(7), json!({"singular": 7}))]
    #[test_case(json!({"repeated": []}), MessageWithInt32Value::new(), json!({}))]
    #[test_case(json!({"repeated": null}), MessageWithInt32Value::new(), json!({}))]
    #[test_case(json!({"repeated": [42, 84.0, "7"]}), MessageWithInt32Value::new().set_repeated([42, 84, 7]), json!({"repeated": [42, 84, 7]}))]
    #[test_case(json!({"map": {}}), MessageWithInt32Value::new(), json!({}))]
    #[test_case(json!({"map": null}), MessageWithInt32Value::new(), json!({}))]
    #[test_case(json!({"map": {"a": 42, "b": 84.0, "c": "7"}}), MessageWithInt32Value::new().set_map([("a", 42), ("b", 84), ("c", 7)]), json!({"map": {"a": 42, "b": 84, "c": 7}}))]
    fn generated_int32_value_fields(
        input: Value,
        want: MessageWithInt32Value,
        output: Value,
    ) -> Result {
        let got = serde_json::from_value::<__MessageWithInt32Value>(input)?;
        assert_eq!(got.0, want);
        let ser = serde_json::to_value(got)?;
        assert_eq!(ser, output);
        Ok(())
    }

    #[test_case(json!({}), MessageWithInt64Value::new(), json!({}))]
    #[test_case(json!({"singular": null}), MessageWithInt64Value::new(), json!({}))]
    #[test_case(json!({"singular": 42}), MessageWithInt64Value::new().set_singular(42), json!({"singular": "42"}))]
    #[test_case(json!({"singular": 84.0}), MessageWithInt64Value::new().set_singular(84), json!({"singular": "84"}))]
    #[test_case(json!({"singular": "7"}), MessageWithInt64Value::new().set_singular(7), json!({"singular": "7"}))]
    #[test_case(json!({"repeated": []}), MessageWithInt64Value::new(), json!({}))]
    #[test_case(json!({"repeated": null}), MessageWithInt64Value::new(), json!({}))]
    #[test_case(json!({"repeated": [42, 84.0, "7"]}), MessageWithInt64Value::new().set_repeated([42, 84, 7]), json!({"repeated": ["42", "84", "7"]}))]
    #[test_case(json!({"map": {}}), MessageWithInt64Value::new(), json!({}))]
    #[test_case(json!({"map": null}), MessageWithInt64Value::new(), json!({}))]
    // TODO(#2376) - #[test_case(json!({"map": {"a": 42, "b": 84.0, "c": "7"}}), MessageWithInt64Value::new().set_map([("a", 42), ("b", 84), ("c", 7)]), json!({"map": {"a": "42", "b": "84", "c": "7"}}))]
    fn int64_value_fields(input: Value, want: MessageWithInt64Value, output: Value) -> Result {
        let got = serde_json::from_value::<MessageWithInt64Value>(input)?;
        assert_eq!(got, want);
        let ser = serde_json::to_value(got)?;
        assert_eq!(ser, output);
        Ok(())
    }

    #[test_case(json!({}), MessageWithInt64Value::new(), json!({}))]
    #[test_case(json!({"singular": null}), MessageWithInt64Value::new(), json!({}))]
    #[test_case(json!({"singular": 42}), MessageWithInt64Value::new().set_singular(42), json!({"singular": "42"}))]
    #[test_case(json!({"singular": 84.0}), MessageWithInt64Value::new().set_singular(84), json!({"singular": "84"}))]
    #[test_case(json!({"singular": "7"}), MessageWithInt64Value::new().set_singular(7), json!({"singular": "7"}))]
    #[test_case(json!({"repeated": []}), MessageWithInt64Value::new(), json!({}))]
    #[test_case(json!({"repeated": null}), MessageWithInt64Value::new(), json!({}))]
    #[test_case(json!({"repeated": [42, 84.0, "7"]}), MessageWithInt64Value::new().set_repeated([42, 84, 7]), json!({"repeated": ["42", "84", "7"]}))]
    #[test_case(json!({"map": {}}), MessageWithInt64Value::new(), json!({}))]
    #[test_case(json!({"map": null}), MessageWithInt64Value::new(), json!({}))]
    #[test_case(json!({"map": {"a": 42, "b": 84.0, "c": "7"}}), MessageWithInt64Value::new().set_map([("a", 42), ("b", 84), ("c", 7)]), json!({"map": {"a": "42", "b": "84", "c": "7"}}))]
    fn generated_int64_value_fields(
        input: Value,
        want: MessageWithInt64Value,
        output: Value,
    ) -> Result {
        let got = serde_json::from_value::<__MessageWithInt64Value>(input)?;
        assert_eq!(got.0, want);
        let ser = serde_json::to_value(got)?;
        assert_eq!(ser, output);
        Ok(())
    }

    #[test_case(json!({}), MessageWithStringValue::new(), json!({}))]
    #[test_case(json!({"singular": null}), MessageWithStringValue::new(), json!({}))]
    #[test_case(json!({"singular": LAZY}), MessageWithStringValue::new().set_singular(LAZY), json!({"singular": LAZY}))]
    #[test_case(json!({"repeated": []}), MessageWithStringValue::new(), json!({}))]
    #[test_case(json!({"repeated": null}), MessageWithStringValue::new(), json!({}))]
    #[test_case(json!({"repeated": [LAZY]}), MessageWithStringValue::new().set_repeated([LAZY]), json!({"repeated": [LAZY]}))]
    #[test_case(json!({"map": {}}), MessageWithStringValue::new(), json!({}))]
    #[test_case(json!({"map": null}), MessageWithStringValue::new(), json!({}))]
    // TODO(#2376) - #[test_case(json!({"map": {"key": LAZY}}), MessageWithStringValue::new().set_map([("key", LAZY)]), json!({"map": {"key": LAZY}}))]
    fn string_value_fields(input: Value, want: MessageWithStringValue, output: Value) -> Result {
        let got = serde_json::from_value::<MessageWithStringValue>(input)?;
        assert_eq!(got, want);
        let ser = serde_json::to_value(got)?;
        assert_eq!(ser, output);
        Ok(())
    }

    #[test_case(json!({}), MessageWithStringValue::new(), json!({}))]
    #[test_case(json!({"singular": null}), MessageWithStringValue::new(), json!({}))]
    #[test_case(json!({"singular": LAZY}), MessageWithStringValue::new().set_singular(LAZY), json!({"singular": LAZY}))]
    #[test_case(json!({"repeated": []}), MessageWithStringValue::new(), json!({}))]
    // TODO(#2376) - #[test_case(json!({"repeated": null}), MessageWithStringValue::new(), json!({}))]
    #[test_case(json!({"repeated": [LAZY]}), MessageWithStringValue::new().set_repeated([LAZY]), json!({"repeated": [LAZY]}))]
    #[test_case(json!({"map": {}}), MessageWithStringValue::new(), json!({}))]
    // TODO(#2376) - #[test_case(json!({"map": null}), MessageWithStringValue::new(), json!({}))]
    #[test_case(json!({"map": {"key": LAZY}}), MessageWithStringValue::new().set_map([("key", LAZY)]), json!({"map": {"key": LAZY}}))]
    fn generated_string_value_fields(
        input: Value,
        want: MessageWithStringValue,
        output: Value,
    ) -> Result {
        let got = serde_json::from_value::<__MessageWithStringValue>(input)?;
        assert_eq!(got.0, want);
        let ser = serde_json::to_value(got)?;
        assert_eq!(ser, output);
        Ok(())
    }

    #[test_case::test_matrix(
        [
            r#"{"singular": null, "singular": null}"#,
            r#"{"repeated": [], "repeated": []}"#,
            r#"{"map": {}, "map": {}}"#,
        ],
        [
            __MessageWithBoolValue(MessageWithBoolValue::new()),
            __MessageWithBytesValue(MessageWithBytesValue::new()),
            __MessageWithDoubleValue(MessageWithDoubleValue::new()),
            __MessageWithInt32Value(MessageWithInt32Value::new()),
            __MessageWithInt64Value(MessageWithInt64Value::new()),
            __MessageWithStringValue(MessageWithStringValue::new()),
        ]
    )]
    fn reject_duplicate_fields<T>(input: &str, _unused: T) -> Result
    where
        T: serde::de::DeserializeOwned + std::fmt::Debug,
    {
        let err = serde_json::from_str::<T>(input).unwrap_err();
        assert!(err.is_data(), "{err:?}");
        Ok(())
    }
}
