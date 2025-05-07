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

    #[serde_with::serde_as]
    #[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]
    #[serde(default, rename_all = "camelCase")]
    pub struct MessageWithF64 {
        #[serde(skip_serializing_if = "google_cloud_wkt::internal::is_default")]
        #[serde_as(as = "google_cloud_wkt::internal::F64")]
        pub singular: f64,
        #[serde(skip_serializing_if = "std::option::Option::is_none")]
        #[serde_as(as = "Option<google_cloud_wkt::internal::F64>")]
        pub optional: Option<f64>,
        #[serde(skip_serializing_if = "Vec::is_empty")]
        #[serde_as(as = "Vec<google_cloud_wkt::internal::F64>")]
        pub repeated: Vec<f64>,
        #[serde(skip_serializing_if = "std::collections::HashMap::is_empty")]
        #[serde_as(as = "std::collections::HashMap<_, google_cloud_wkt::internal::F64>")]
        pub hashmap: std::collections::HashMap<String, f64>,
    }

    #[test_case(9876.5, 9876.5)]
    #[test_case(f64::INFINITY, "Infinity")]
    #[test_case(f64::NEG_INFINITY, "-Infinity")]
    #[test_case(f64::NAN, "NaN")]
    fn test_singular<T>(input: f64, want: T) -> Result
    where
        T: serde::ser::Serialize,
    {
        let msg = MessageWithF64 {
            singular: input,
            ..Default::default()
        };
        let got = serde_json::to_value(&msg)?;
        let want = json!({"singular": want});
        assert_eq!(want, got);

        let roundtrip = serde_json::from_value::<MessageWithF64>(got)?;
        assert_float_eq(msg.singular, roundtrip.singular);
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
        let msg = MessageWithF64 {
            optional: Some(input),
            ..Default::default()
        };
        let got = serde_json::to_value(&msg)?;
        let want = json!({"optional": want});
        assert_eq!(want, got);

        let roundtrip = serde_json::from_value::<MessageWithF64>(got)?;
        assert_float_eq(msg.optional.unwrap(), roundtrip.optional.unwrap());
        Ok(())
    }

    #[test]
    fn test_repeated() -> Result {
        let msg = MessageWithF64 {
            repeated: vec![f64::INFINITY, f64::NEG_INFINITY, f64::NAN, 9876.5_f64],
            ..Default::default()
        };
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
        let mut hashmap = std::collections::HashMap::new();
        hashmap.insert("number".to_string(), 9876.5);
        hashmap.insert("inf".to_string(), f64::INFINITY);
        hashmap.insert("-inf".to_string(), f64::NEG_INFINITY);
        hashmap.insert("nan".to_string(), f64::NAN);

        let msg = MessageWithF64 {
            hashmap,
            ..Default::default()
        };

        let got = serde_json::to_value(&msg)?;
        let want = json!({
            "hashmap": {
                "number": 9876.5,
                "inf": "Infinity",
                "-inf": "-Infinity",
                "nan": "NaN"
            }
        });
        assert_eq!(want, got);

        let roundtrip = serde_json::from_value::<MessageWithF64>(got)?;
        for (k, roundtrip) in roundtrip.hashmap.iter() {
            let msg = msg.hashmap.get(k).unwrap();
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
