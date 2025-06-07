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
    use google_cloud_wkt as wkt;
    use serde_json::{Value, json};
    use test_case::test_case;
    type Result = anyhow::Result<()>;

    #[allow(dead_code)]
    mod protos {
        use google_cloud_wkt as wkt;
        include!("generated/mod.rs");
    }
    use protos::{
        MessageWithComplexOneOf,
        message_with_complex_one_of::{Inner, TestEnum},
    };

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
