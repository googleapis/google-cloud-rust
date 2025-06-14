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
        __MessageWithComplexOneOf, MessageWithComplexOneOf,
        message_with_complex_one_of::{Inner, TestEnum},
    };
    use google_cloud_wkt as wkt;
    use serde_json::{Value, json};
    use test_case::test_case;
    use wkt::Duration;
    type Result = anyhow::Result<()>;

    const LAZY: &str = "the quick brown fox jumps over the lazy dog";
    const LAZY_BYTES: &[u8] = b"the quick brown fox jumps over the lazy dog";
    const LAZY_BASE64: &str = "dGhlIHF1aWNrIGJyb3duIGZveCBqdW1wcyBvdmVyIHRoZSBsYXp5IGRvZw==";

    #[test_case(MessageWithComplexOneOf::new(), json!({}))]
    #[test_case(MessageWithComplexOneOf::new().set_null(wkt::NullValue), json!({"null": null}))]
    #[test_case(MessageWithComplexOneOf::new().set_bool_value(false), json!({"boolValue": false}))]
    #[test_case(MessageWithComplexOneOf::new().set_bytes_value(""), json!({"bytesValue": ""}))]
    #[test_case(MessageWithComplexOneOf::new().set_bytes_value(LAZY_BYTES), json!({"bytesValue": LAZY_BASE64}))]
    #[test_case(MessageWithComplexOneOf::new().set_string_value(""), json!({"stringValue": ""}))]
    #[test_case(MessageWithComplexOneOf::new().set_float_value(0.0), json!({"floatValue": 0.0}))]
    #[test_case(MessageWithComplexOneOf::new().set_float_value(1.5), json!({"floatValue": 1.5}))]
    #[test_case(MessageWithComplexOneOf::new().set_float_value(2.5), json!({"floatValue": 2.5}))]
    #[test_case(MessageWithComplexOneOf::new().set_float_value(3.0), json!({"floatValue": 3.0}))]
    #[test_case(MessageWithComplexOneOf::new().set_double_value(0.0), json!({"doubleValue": 0.0}))]
    #[test_case(MessageWithComplexOneOf::new().set_double_value(1.5), json!({"doubleValue": 1.5}))]
    #[test_case(MessageWithComplexOneOf::new().set_double_value(2.5), json!({"doubleValue": 2.5}))]
    #[test_case(MessageWithComplexOneOf::new().set_double_value(3.0), json!({"doubleValue": 3.0}))]
    #[test_case(MessageWithComplexOneOf::new().set_int(0), json!({"int": 0}))]
    #[test_case(MessageWithComplexOneOf::new().set_int(1), json!({"int": 1}))]
    #[test_case(MessageWithComplexOneOf::new().set_int(2), json!({"int": 2}))]
    #[test_case(MessageWithComplexOneOf::new().set_int(3), json!({"int": 3}))]
    #[test_case(MessageWithComplexOneOf::new().set_long(0), json!({"long": "0"}))]
    #[test_case(MessageWithComplexOneOf::new().set_long(1), json!({"long": "1"}))]
    #[test_case(MessageWithComplexOneOf::new().set_long(2), json!({"long": "2"}))]
    #[test_case(MessageWithComplexOneOf::new().set_long(3), json!({"long": "3"}))]
    #[test_case(MessageWithComplexOneOf::new().set_enum(TestEnum::default()), json!({"enum": 0}))]
    #[test_case(MessageWithComplexOneOf::new().set_inner(Inner::default().set_strings(["a", "b"])), json!({"inner": {"strings": ["a", "b"]}}))]
    #[test_case(MessageWithComplexOneOf::new().set_duration(Duration::clamp(-1, -750_000_000)), json!({"duration": "-1.75s"}))]
    #[test_case(MessageWithComplexOneOf::new().set_value(json!({"a": 1})), json!({"value": {"a": 1}}))]
    #[test_case(MessageWithComplexOneOf::new().set_value(wkt::Value::Null), json!({"value": null}))]
    fn test_ser(input: MessageWithComplexOneOf, want: Value) -> Result {
        let got = serde_json::to_value(__MessageWithComplexOneOf(input))?;
        assert_eq!(got, want);
        Ok(())
    }

    #[test_case(MessageWithComplexOneOf::new(), json!({}))]
    #[test_case(MessageWithComplexOneOf::new().set_null(wkt::NullValue), json!({"null": null}))]
    #[test_case(MessageWithComplexOneOf::new().set_bool_value(false), json!({"boolValue": false}))]
    #[test_case(MessageWithComplexOneOf::new().set_bytes_value(""), json!({"bytesValue": ""}))]
    #[test_case(MessageWithComplexOneOf::new().set_bytes_value(LAZY), json!({"bytesValue": LAZY_BASE64}))]
    #[test_case(MessageWithComplexOneOf::new().set_string_value(""), json!({"stringValue": ""}))]
    #[test_case(MessageWithComplexOneOf::new().set_float_value(0.0), json!({"floatValue": 0}))]
    #[test_case(MessageWithComplexOneOf::new().set_float_value(1.5), json!({"floatValue": "1.5"}))]
    #[test_case(MessageWithComplexOneOf::new().set_float_value(2.5), json!({"floatValue": 2.5}))]
    #[test_case(MessageWithComplexOneOf::new().set_float_value(3.0), json!({"floatValue": 3}))]
    #[test_case(MessageWithComplexOneOf::new().set_double_value(0.0), json!({"doubleValue": 0}))]
    #[test_case(MessageWithComplexOneOf::new().set_double_value(1.5), json!({"doubleValue": "1.5"}))]
    #[test_case(MessageWithComplexOneOf::new().set_double_value(2.5), json!({"doubleValue": 2.5}))]
    #[test_case(MessageWithComplexOneOf::new().set_double_value(3.0), json!({"doubleValue": 3}))]
    #[test_case(MessageWithComplexOneOf::new().set_int(0), json!({"int": 0}))]
    #[test_case(MessageWithComplexOneOf::new().set_int(1), json!({"int": "1"}))]
    #[test_case(MessageWithComplexOneOf::new().set_int(2), json!({"int": 2.0}))]
    #[test_case(MessageWithComplexOneOf::new().set_int(3), json!({"int": 3e0}))]
    #[test_case(MessageWithComplexOneOf::new().set_long(0), json!({"long": 0}))]
    #[test_case(MessageWithComplexOneOf::new().set_long(1), json!({"long": "1"}))]
    #[test_case(MessageWithComplexOneOf::new().set_long(2), json!({"long": 2.0}))]
    #[test_case(MessageWithComplexOneOf::new().set_long(3), json!({"long": 3e0}))]
    #[test_case(MessageWithComplexOneOf::new().set_enum(TestEnum::default()), json!({"enum": 0}))]
    #[test_case(MessageWithComplexOneOf::new().set_inner(Inner::default().set_strings(["a", "b"])), json!({"inner": {"strings": ["a", "b"]}}))]
    #[test_case(MessageWithComplexOneOf::new().set_duration(Duration::clamp(-1, -750_000_000)), json!({"duration": "-1.75s"}))]
    #[test_case(MessageWithComplexOneOf::new().set_bool_value(false), json!({"bool_value": false}))]
    #[test_case(MessageWithComplexOneOf::new().set_bytes_value(LAZY_BYTES), json!({"bytes_value": LAZY_BASE64}))]
    #[test_case(MessageWithComplexOneOf::new().set_string_value(LAZY), json!({"string_value": LAZY}))]
    #[test_case(MessageWithComplexOneOf::new().set_float_value(1.5), json!({"float_value": 1.5}))]
    #[test_case(MessageWithComplexOneOf::new().set_double_value(2.5), json!({"double_value": 2.5}))]
    #[test_case(MessageWithComplexOneOf::new().set_value(json!({"a": 1})), json!({"value": {"a": 1}}))]
    #[test_case(MessageWithComplexOneOf::new().set_value(wkt::Value::Null), json!({"value": null}))]
    fn test_de(want: MessageWithComplexOneOf, input: Value) -> Result {
        let got = serde_json::from_value::<__MessageWithComplexOneOf>(input)?;
        assert_eq!(got.0, want);
        Ok(())
    }

    // For these fields, `null` must have an effect:
    //     #[test_case(MessageWithComplexOneOf::new(), r#"{"null": null}"#)]
    //     #[test_case(MessageWithComplexOneOf::new(), r#"{"value": null}"#)]
    #[test_case(MessageWithComplexOneOf::new(), r#"{"boolValue": null}"#)]
    #[test_case(MessageWithComplexOneOf::new(), r#"{"bytesValue": null}"#)]
    #[test_case(MessageWithComplexOneOf::new(), r#"{"stringValue": null}"#)]
    #[test_case(MessageWithComplexOneOf::new(), r#"{"floatValue": null}"#)]
    #[test_case(MessageWithComplexOneOf::new(), r#"{"doubleValue": null}"#)]
    #[test_case(MessageWithComplexOneOf::new(), r#"{"int": null}"#)]
    #[test_case(MessageWithComplexOneOf::new(), r#"{"long": null}"#)]
    #[test_case(MessageWithComplexOneOf::new(), r#"{"enum": null}"#)]
    #[test_case(MessageWithComplexOneOf::new(), r#"{"inner": null}"#)]
    #[test_case(MessageWithComplexOneOf::new(), r#"{"duration": null}"#)]
    #[test_case(MessageWithComplexOneOf::new().set_bool_value(true),                r#"{"boolValue": null, "boolValue": true, "boolValue": null}"#)]
    #[test_case(MessageWithComplexOneOf::new().set_bytes_value(""),                 r#"{"bytesValue": null, "bytesValue": "", "bytesValue": null}"#)]
    #[test_case(MessageWithComplexOneOf::new().set_string_value(""),                r#"{"stringValue": null, "stringValue": "", "stringValue": null}"#)]
    #[test_case(MessageWithComplexOneOf::new().set_float_value(0_f32),              r#"{"floatValue": null, "floatValue": 0, "floatValue": null}"#)]
    #[test_case(MessageWithComplexOneOf::new().set_double_value(0_f64),             r#"{"doubleValue": null, "doubleValue": 0, "doubleValue": null}"#)]
    #[test_case(MessageWithComplexOneOf::new().set_int(0),                          r#"{"int": null, "int": 0, "int": null}"#)]
    #[test_case(MessageWithComplexOneOf::new().set_long(0),                         r#"{"long": null, "long": 0, "long": null}"#)]
    #[test_case(MessageWithComplexOneOf::new().set_enum(TestEnum::Black),           r#"{"enum": null, "enum": "BLACK", "enum": null}"#)]
    #[test_case(MessageWithComplexOneOf::new().set_inner(Inner::default()),         r#"{"inner": null, "inner": {}, "inner": null}"#)]
    #[test_case(MessageWithComplexOneOf::new().set_duration(Duration::clamp(2, 0)), r#"{"duration": null, "duration": "2.0s", "duration": null}"#)]
    fn null_values_have_no_effect(want: MessageWithComplexOneOf, input: &str) -> Result {
        let got = serde_json::from_str::<__MessageWithComplexOneOf>(input)?;
        assert_eq!(got.0, want);
        Ok(())
    }

    #[test_case(r#"{"null": null, "null": null}"#)]
    #[test_case(r#"{"null": null, "boolValue": true}"#)]
    #[test_case(r#"{"null": null, "bytesValue": ""}"#)]
    #[test_case(r#"{"null": null, "stringValue": ""}"#)]
    #[test_case(r#"{"null": null, "floatValue": 0}"#)]
    #[test_case(r#"{"null": null, "doubleValue": 0}"#)]
    #[test_case(r#"{"null": null, "int": 0}"#)]
    #[test_case(r#"{"null": null, "long": 0}"#)]
    #[test_case(r#"{"null": null, "enum": "BLACK"}"#)]
    #[test_case(r#"{"null": null, "inner": {}}"#)]
    #[test_case(r#"{"null": null, "duration": "2.0s"}"#)]
    #[test_case(r#"{"null": null, "value": "abc"}"#)]
    fn dup_fields_are_errors(input: &str) -> Result {
        let got = serde_json::from_str::<__MessageWithComplexOneOf>(input).unwrap_err();
        assert!(got.is_data(), "{got:?}");
        Ok(())
    }

    #[test_case(json!({}))]
    fn test_default(input: Value) -> Result {
        let got = serde_json::from_value::<MessageWithComplexOneOf>(input)?;
        assert_eq!(got, MessageWithComplexOneOf::default());
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, json!({}));
        Ok(())
    }

    #[test_case(json!({"null": null}), wkt::NullValue)]
    fn test_null(input: Value, want: wkt::NullValue) -> Result {
        let got = serde_json::from_value::<MessageWithComplexOneOf>(input)?;
        assert_eq!(got.null(), Some(&want), "{got:?}");
        let roundtrip = serde_json::from_value(serde_json::to_value(&got)?)?;
        assert_eq!(got, roundtrip);
        Ok(())
    }

    #[test_case(json!({"boolValue": true}), true)]
    fn test_bool(input: Value, want: bool) -> Result {
        let got = serde_json::from_value::<MessageWithComplexOneOf>(input)?;
        assert_eq!(got.bool_value(), Some(&want), "{got:?}");
        let roundtrip = serde_json::from_value(serde_json::to_value(&got)?)?;
        assert_eq!(got, roundtrip);
        Ok(())
    }

    #[test_case(json!({"bytesValue": ""}), b"")]
    #[test_case(json!({"bytesValue": "dGhlIHF1aWNrIGJyb3duIGZveCBqdW1wcyBvdmVyIHRoZSBsYXp5IGRvZw=="}), b"the quick brown fox jumps over the lazy dog")]
    fn test_bytes(input: Value, want: &[u8]) -> Result {
        let got = serde_json::from_value::<MessageWithComplexOneOf>(input)?;
        assert_eq!(
            got.bytes_value(),
            Some(&bytes::Bytes::copy_from_slice(want)),
            "{got:?}"
        );
        let roundtrip = serde_json::from_value(serde_json::to_value(&got)?)?;
        assert_eq!(got, roundtrip);
        Ok(())
    }

    #[test_case(json!({"stringValue": ""}), "")]
    #[test_case(json!({"stringValue": "abc"}), "abc")]
    fn test_string(input: Value, want: &str) -> Result {
        let got = serde_json::from_value::<MessageWithComplexOneOf>(input)?;
        assert_eq!(got.string_value(), Some(&want.to_string()), "{got:?}");
        let roundtrip = serde_json::from_value(serde_json::to_value(&got)?)?;
        assert_eq!(got, roundtrip);
        Ok(())
    }

    #[test_case(json!({"floatValue": 0}), 0.0)]
    #[test_case(json!({"floatValue": "0"}), 0.0; "0 as str")]
    #[test_case(json!({"floatValue": 1}), 1.0)]
    #[test_case(json!({"floatValue": "1"}), 1.0; "1 as str")]
    fn test_float(input: Value, want: f32) -> Result {
        let got = serde_json::from_value::<MessageWithComplexOneOf>(input)?;
        assert_eq!(got.float_value(), Some(&want), "{got:?}");
        let roundtrip = serde_json::from_value(serde_json::to_value(&got)?)?;
        assert_eq!(got, roundtrip);
        Ok(())
    }

    #[test_case(json!({"doubleValue": 0}), 0.0)]
    #[test_case(json!({"doubleValue": "0"}), 0.0; "0 as str")]
    #[test_case(json!({"doubleValue": 1}), 1.0)]
    #[test_case(json!({"doubleValue": "1"}), 1.0; "1 as str")]
    fn test_double(input: Value, want: f64) -> Result {
        let got = serde_json::from_value::<MessageWithComplexOneOf>(input)?;
        assert_eq!(got.double_value(), Some(&want), "{got:?}");
        let roundtrip = serde_json::from_value(serde_json::to_value(&got)?)?;
        assert_eq!(got, roundtrip);
        Ok(())
    }

    #[test_case(json!({"int": 0}), 0)]
    #[test_case(json!({"int": "0"}), 0; "0 as str")]
    #[test_case(json!({"int": 1}), 1)]
    #[test_case(json!({"int": "1"}), 1; "1 as str")]
    fn test_int(input: Value, want: i32) -> Result {
        let got = serde_json::from_value::<MessageWithComplexOneOf>(input)?;
        assert_eq!(got.int(), Some(&want), "{got:?}");
        let roundtrip = serde_json::from_value(serde_json::to_value(&got)?)?;
        assert_eq!(got, roundtrip);
        Ok(())
    }

    #[test_case(json!({"long": 0}), 0)]
    #[test_case(json!({"long": "0"}), 0; "0 as str")]
    #[test_case(json!({"long": 1}), 1)]
    #[test_case(json!({"long": "1"}), 1; "1 as str")]
    fn test_long(input: Value, want: i64) -> Result {
        let got = serde_json::from_value::<MessageWithComplexOneOf>(input)?;
        assert_eq!(got.long(), Some(&want), "{got:?}");
        let roundtrip = serde_json::from_value(serde_json::to_value(&got)?)?;
        assert_eq!(got, roundtrip);
        Ok(())
    }

    #[test_case(json!({"enum": 0}), TestEnum::default())]
    #[test_case(json!({"enum": "TEST_ENUM_UNSPECIFIED"}), TestEnum::default())]
    #[test_case(json!({"enum": "BLACK"}), TestEnum::Black)]
    #[test_case(json!({"enum": 1}), TestEnum::from(1))]
    fn test_enum(input: Value, want: TestEnum) -> Result {
        let got = serde_json::from_value::<MessageWithComplexOneOf>(input)?;
        assert_eq!(got.r#enum(), Some(&want), "{got:?}");
        let roundtrip = serde_json::from_value(serde_json::to_value(&got)?)?;
        assert_eq!(got, roundtrip);
        Ok(())
    }

    #[test_case(json!({"inner": {"strings": ["a", "b", "c"]}}), Inner::new().set_strings(["a", "b", "c"]))]
    #[test_case(json!({"inner": {}}), Inner::new())]
    fn test_inner(input: Value, want: Inner) -> Result {
        let got = serde_json::from_value::<MessageWithComplexOneOf>(input)?;
        assert_eq!(got.inner(), Some(&Box::new(want)), "{got:?}");
        let roundtrip = serde_json::from_value(serde_json::to_value(&got)?)?;
        assert_eq!(got, roundtrip);
        Ok(())
    }

    #[test_case(json!({"duration": "123.456s"}), wkt::Duration::clamp(123, 456_000_000))]
    #[test_case(json!({"duration": "0s"}), wkt::Duration::default())]
    fn test_duration(input: Value, want: wkt::Duration) -> Result {
        let got = serde_json::from_value::<MessageWithComplexOneOf>(input)?;
        assert_eq!(got.duration(), Some(&Box::new(want)), "{got:?}");
        let roundtrip = serde_json::from_value(serde_json::to_value(&got)?)?;
        assert_eq!(got, roundtrip);
        Ok(())
    }
}
