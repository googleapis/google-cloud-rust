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
    use serde_json::json;
    type Result = std::result::Result<(), Box<dyn std::error::Error>>;
    use test_case::test_case;

    #[allow(dead_code)]
    mod protos {
        use google_cloud_wkt as wkt;
        include!("generated/mod.rs");
    }
    use protos::MessageWithF64;

    #[test_case(9876.5, 9876.5)]
    #[test_case(f64::INFINITY, "Infinity")]
    #[test_case(f64::NEG_INFINITY, "-Infinity")]
    #[test_case(f64::NAN, "NaN")]
    fn test_singular<T>(input: f64, want: T) -> Result
    where
        T: serde::ser::Serialize,
    {
        let msg = MessageWithF64::new().set_singular(input);
        let got = serde_json::to_value(&msg)?;
        let want = json!({"singular": want});
        assert_eq!(want, got);

        let roundtrip = serde_json::from_value::<MessageWithF64>(got)?;
        assert_float_eq(msg.singular, roundtrip.singular);
        Ok(())
    }

    #[test_case(-1, -1.0)]
    #[test_case(-2, -2.0)]
    #[test_case(3, 3.0)]
    #[test_case(4, 4.0)]
    fn test_singular_as_int(input: i64, want: f64) -> Result {
        let input = json!({"singular": input});
        let got = serde_json::from_value::<MessageWithF64>(input)?;
        assert_eq!(got.singular, want);
        Ok(())
    }

    #[test_case(9876.5, 9876.5)]
    #[test_case(f64::INFINITY, "Infinity")]
    #[test_case(f64::NEG_INFINITY, "-Infinity")]
    #[test_case(f64::NAN, "NaN")]
    fn test_optional<T>(input: f64, want: T) -> Result
    where
        T: serde::ser::Serialize,
    {
        let msg = MessageWithF64::new().set_optional(input);
        let got = serde_json::to_value(&msg)?;
        let want = json!({"optional": want});
        assert_eq!(want, got);

        let roundtrip = serde_json::from_value::<MessageWithF64>(got)?;
        assert_float_eq(msg.optional.unwrap(), roundtrip.optional.unwrap());
        Ok(())
    }

    #[test]
    fn test_repeated() -> Result {
        let msg = MessageWithF64::new().set_repeated([
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::NAN,
            9876.5_f64,
        ]);
        let got = serde_json::to_value(&msg)?;
        let want = json!({"repeated": ["Infinity", "-Infinity", "NaN", 9876.5]});
        assert_eq!(want, got);

        let roundtrip = serde_json::from_value::<MessageWithF64>(got)?;
        for (roundtrip, msg) in roundtrip.repeated.iter().zip(msg.repeated.iter()) {
            assert_float_eq(*roundtrip, *msg);
        }
        Ok(())
    }

    #[test]
    fn test_hashmap() -> Result {
        let mut map = std::collections::HashMap::new();
        map.insert("number".to_string(), 9876.5);
        map.insert("inf".to_string(), f64::INFINITY);
        map.insert("-inf".to_string(), f64::NEG_INFINITY);
        map.insert("nan".to_string(), f64::NAN);

        let msg = MessageWithF64::new().set_map(map);

        let got = serde_json::to_value(&msg)?;
        let want = json!({
            "map": {
                "number": 9876.5,
                "inf": "Infinity",
                "-inf": "-Infinity",
                "nan": "NaN"
            }
        });
        assert_eq!(want, got);

        let roundtrip = serde_json::from_value::<MessageWithF64>(got)?;
        for (k, roundtrip) in roundtrip.map.iter() {
            let msg = msg.map.get(k).unwrap();
            assert_float_eq(*roundtrip, *msg);
        }
        Ok(())
    }

    fn assert_float_eq(left: f64, right: f64) {
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
