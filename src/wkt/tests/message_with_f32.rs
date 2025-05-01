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
    #[serde_with::skip_serializing_none]
    #[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct MessageWithF32 {
        #[serde_as(as = "google_cloud_wkt::internal::F32")]
        pub singular: f32,
        #[serde_as(as = "Option<google_cloud_wkt::internal::F32>")]
        pub optional: Option<f32>,
        #[serde_as(as = "Vec<google_cloud_wkt::internal::F32>")]
        pub repeated: Vec<f32>,
        #[serde_as(as = "std::collections::HashMap<_, google_cloud_wkt::internal::F32>")]
        pub hashmap: std::collections::HashMap<String, f32>,
    }

    #[test_case(9876.5, 9876.5)]
    #[test_case(f32::INFINITY, "Infinity")]
    #[test_case(f32::NEG_INFINITY, "-Infinity")]
    #[test_case(f32::NAN, "NaN")]
    fn test_singular<T>(input: f32, want: T) -> Result
    where
        T: serde::ser::Serialize,
    {
        let msg = MessageWithF32 {
            singular: input,
            ..Default::default()
        };
        let got = serde_json::to_value(&msg)?;
        let want = json!({"singular": want, "repeated": [], "hashmap": {}});
        assert_eq!(want, got);

        let roundtrip = serde_json::from_value::<MessageWithF32>(got)?;
        assert_float_eq(msg.singular, roundtrip.singular);
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
        let msg = MessageWithF32 {
            optional: Some(input),
            ..Default::default()
        };
        let got = serde_json::to_value(&msg)?;
        let want = json!({"singular": 0.0, "optional": want, "repeated": [], "hashmap": {}});
        assert_eq!(want, got);

        let roundtrip = serde_json::from_value::<MessageWithF32>(got)?;
        assert_float_eq(msg.optional.unwrap(), roundtrip.optional.unwrap());
        Ok(())
    }

    #[test]
    fn test_repeated() -> Result {
        let msg = MessageWithF32 {
            repeated: vec![f32::INFINITY, f32::NEG_INFINITY, f32::NAN, 9876.5_f32],
            ..Default::default()
        };
        let got = serde_json::to_value(&msg)?;
        let want = json!({"singular": 0.0, "repeated": ["Infinity", "-Infinity", "NaN", 9876.5], "hashmap": {}});
        assert_eq!(want, got);

        let roundtrip = serde_json::from_value::<MessageWithF32>(got)?;
        for (roundtrip, msg) in roundtrip.repeated.iter().zip(msg.repeated.iter()) {
            assert_float_eq(*roundtrip, *msg);
        }
        Ok(())
    }

    #[test]
    fn test_hashmap() -> Result {
        let mut hashmap = std::collections::HashMap::new();
        hashmap.insert("number".to_string(), 9876.5);
        hashmap.insert("inf".to_string(), f32::INFINITY);
        hashmap.insert("-inf".to_string(), f32::NEG_INFINITY);
        hashmap.insert("nan".to_string(), f32::NAN);

        let msg = MessageWithF32 {
            hashmap,
            ..Default::default()
        };

        let got = serde_json::to_value(&msg)?;
        let want = json!({"singular": 0.0, "repeated": [],"hashmap": {"number": 9876.5, "inf": "Infinity", "-inf": "-Infinity", "nan": "NaN"}});
        assert_eq!(want, got);

        let roundtrip = serde_json::from_value::<MessageWithF32>(got)?;
        for (k, roundtrip) in roundtrip.hashmap.iter() {
            let msg = msg.hashmap.get(k).unwrap();
            assert_float_eq(*roundtrip, *msg);
        }
        Ok(())
    }

    fn assert_float_eq(left: f32, right: f32) {
        // Consider all NaN as equal.
        if left.is_nan() && right.is_nan() {
            return;
        }
        // Consider all infinites floats of the same sign as equal.
        if (left.is_infinite() && right.is_infinite())
            && left.is_sign_positive() == right.is_sign_positive()
        {
            return;
        }
        assert_eq!(left, right);
    }
}
