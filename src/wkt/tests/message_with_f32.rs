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
    use common::{__MessageWithF32, MessageWithF32};
    use serde_json::{Value, json};
    use test_case::test_case;
    type Result = anyhow::Result<()>;

    #[test_case(MessageWithF32::new(), json!({}))]
    fn test_ser(input: MessageWithF32, want: Value) -> Result {
        let got = serde_json::to_value(__MessageWithF32(input))?;
        assert_eq!(got, want);
        Ok(())
    }

    #[test_case(MessageWithF32::new(), json!({}))]
    fn test_de(want: MessageWithF32, input: Value) -> Result {
        let got = serde_json::from_value::<__MessageWithF32>(input)?;
        assert_eq!(got.0, want);
        Ok(())
    }
    #[test_case(json!({"unknown": "test-value"}))]
    #[test_case(json!({"unknown": "test-value", "moreUnknown": {"a": 1, "b": 2}}))]
    fn test_unknown(input: Value) -> Result {
        let deser = serde_json::from_value::<__MessageWithF32>(input.clone())?;
        let got = serde_json::to_value(deser)?;
        assert_eq!(got, input);
        Ok(())
    }

    #[test_case(9876.5, 9876.5)]
    #[test_case(f32::INFINITY, "Infinity")]
    #[test_case(f32::NEG_INFINITY, "-Infinity")]
    #[test_case(f32::NAN, "NaN")]
    fn test_singular<T>(input: f32, want: T) -> Result
    where
        T: serde::ser::Serialize,
    {
        let msg = MessageWithF32::new().set_singular(input);
        let got = serde_json::to_value(&msg)?;
        let want = json!({"singular": want});
        assert_eq!(want, got);

        let roundtrip = serde_json::from_value::<MessageWithF32>(got)?;
        assert_float_eq(msg.singular, roundtrip.singular);
        Ok(())
    }

    #[test_case(-1, -1.0)]
    #[test_case(-2, -2.0)]
    #[test_case(3, 3.0)]
    #[test_case(4, 4.0)]
    fn test_singular_as_int(input: i32, want: f32) -> Result {
        let input = json!({"singular": input});
        let got = serde_json::from_value::<MessageWithF32>(input)?;
        assert_eq!(got.singular, want);
        Ok(())
    }

    #[test_case("-1", -1.0)]
    #[test_case("-2", -2.0)]
    #[test_case("3", 3.0)]
    #[test_case("4", 4.0)]
    fn test_singular_as_string(input: &str, want: f32) -> Result {
        let input = json!({"singular": input});
        let got = serde_json::from_value::<MessageWithF32>(input)?;
        assert_eq!(got.singular, want);
        Ok(())
    }

    #[test_case(json!({}))]
    #[test_case(json!({"singular": null}))]
    #[test_case(json!({"singular": 0}))]
    #[test_case(json!({"singular": 0.0}))]
    #[test_case(json!({"singular": 0e0}))]
    #[test_case(json!({"singular": "0"}); "0 string")]
    #[test_case(json!({"singular": "0.0"}); "0.0 string")]
    #[test_case(json!({"singular": "0e0"}); "0e0 string")]
    fn test_singular_default(input: Value) -> Result {
        let got = serde_json::from_value::<MessageWithF32>(input)?;
        assert_eq!(got, MessageWithF32::default());
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, json!({}));
        Ok(())
    }

    #[test_case(9876.5, 9876.5)]
    #[test_case(f32::INFINITY, "Infinity")]
    #[test_case(f32::NEG_INFINITY, "-Infinity")]
    #[test_case(f32::NAN, "NaN")]
    fn test_optional<T>(input: f32, want: T) -> Result
    where
        T: serde::ser::Serialize,
    {
        let msg = MessageWithF32::new().set_optional(input);
        let got = serde_json::to_value(&msg)?;
        let want = json!({"optional": want});
        assert_eq!(want, got);

        let roundtrip = serde_json::from_value::<MessageWithF32>(got)?;
        assert_float_eq(msg.optional.unwrap(), roundtrip.optional.unwrap());
        Ok(())
    }

    #[test_case(json!({}))]
    #[test_case(json!({"optional": null}))]
    fn test_optional_default(input: Value) -> Result {
        let got = serde_json::from_value::<MessageWithF32>(input)?;
        assert_eq!(got, MessageWithF32::default());
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, json!({}));
        Ok(())
    }

    #[test]
    fn test_repeated() -> Result {
        let msg = MessageWithF32::new().set_repeated([
            f32::INFINITY,
            f32::NEG_INFINITY,
            f32::NAN,
            9876.5_f32,
        ]);
        let got = serde_json::to_value(&msg)?;
        let want = json!({"repeated": ["Infinity", "-Infinity", "NaN", 9876.5]});
        assert_eq!(want, got);

        let roundtrip = serde_json::from_value::<MessageWithF32>(got)?;
        for (roundtrip, msg) in roundtrip.repeated.iter().zip(msg.repeated.iter()) {
            assert_float_eq(*roundtrip, *msg);
        }
        Ok(())
    }

    #[test_case(json!({}))]
    #[test_case(json!({"repeated": []}))]
    #[test_case(json!({"repeated": null}))]
    fn test_repeated_default(input: Value) -> Result {
        let got = serde_json::from_value::<MessageWithF32>(input)?;
        assert_eq!(got, MessageWithF32::default());
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, json!({}));
        Ok(())
    }

    #[test]
    fn test_map() -> Result {
        let want = MessageWithF32::new().set_map([
            ("number", 9876.5),
            ("inf", f32::INFINITY),
            ("-inf", f32::NEG_INFINITY),
            ("nan", f32::NAN),
            ("int", 1.0),
            ("str", 2.0),
            ("str_int", 3.0),
        ]);

        let input = json!({
            "map": {
                "number": 9876.5,
                "inf": "Infinity",
                "-inf": "-Infinity",
                "nan": "NaN",
                "int": 1,
                "str": "2.0",
                "str_int": "3",
            }
        });
        let got = serde_json::from_value::<MessageWithF32>(input.clone())?;
        for (k, v) in want.map.iter() {
            let w = got
                .map
                .get(k)
                .unwrap_or_else(|| panic!("missing {k} in got.map"));
            assert_float_eq(*v, *w);
        }

        let want_value = serde_json::to_value(&want)?;
        let got_value = serde_json::to_value(&got)?;
        assert_eq!(got_value, want_value);
        Ok(())
    }

    #[test_case(json!({}))]
    #[test_case(json!({"map": {}}))]
    #[test_case(json!({"map": null}))]
    fn test_map_default(input: Value) -> Result {
        let got = serde_json::from_value::<MessageWithF32>(input)?;
        assert_eq!(got, MessageWithF32::default());
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, json!({}));
        Ok(())
    }

    fn assert_float_eq(left: f32, right: f32) {
        // Consider all NaN as equal.
        if left.is_nan() && right.is_nan() {
            return;
        }
        // Consider all infinites floats of the same sign as equal.
        if left.is_infinite()
            && right.is_infinite()
            && left.is_sign_positive() == right.is_sign_positive()
        {
            return;
        }
        assert_eq!(left, right);
    }
}
