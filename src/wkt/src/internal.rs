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

pub struct F32;

impl serde_with::SerializeAs<f32> for F32 {
    fn serialize_as<S>(value: &f32, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        float_serialize(value, serializer)
    }
}

impl<'de> serde_with::DeserializeAs<'de, f32> for F32 {
    fn deserialize_as<D>(deserializer: D) -> Result<f32, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        float_deserialize(deserializer)
    }
}

/// A helper to serialize `f32` to ProtoJSON format.
pub fn float_serialize<S>(x: &f32, s: S) -> std::result::Result<S::Ok, S::Error>
where
    S: serde::ser::Serializer,
{
    // Handle special strings, see https://protobuf.dev/programming-guides/json/.
    match x {
        x if x.is_nan() => s.serialize_str("NaN"),
        x if x.is_infinite() && x.is_sign_negative() => s.serialize_str("-Infinity"),
        x if x.is_infinite() => s.serialize_str("Infinity"),
        x => s.serialize_f32(*x),
    }
}

struct FloatVisitor;

/// A helper to deserialize `f32` from ProtoJSON format.
pub fn float_deserialize<'de, D>(deserializer: D) -> std::result::Result<f32, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    deserializer.deserialize_any(FloatVisitor)
}

impl serde::de::Visitor<'_> for FloatVisitor {
    type Value = f32;

    fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        // Handle special strings, see https://protobuf.dev/programming-guides/json/.
        match value {
            "NaN" => Ok(f32::NAN),
            "Infinity" => Ok(f32::INFINITY),
            "-Infinity" => Ok(f32::NEG_INFINITY),
            _ => Err(serde::de::Error::invalid_value(
                serde::de::Unexpected::Other(value),
                &"a valid ProtoJSON string for f32 (NaN, Infinity, -Infinity)",
            )),
        }
    }

    fn visit_f32<E>(self, value: f32) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        // TODO(#1767): Find a way to test this code path, serde_json floats
        // stored as f64.
        Ok(value)
    }

    // Floats in serde_json are f64.
    fn visit_f64<E>(self, value: f64) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        // Cast f64 to f32 to produce the closest possible float value.
        // See https://doc.rust-lang.org/reference/expressions/operator-expr.html#r-expr.as.numeric.float-narrowing
        Ok(value as f32)
    }

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a 32-bit floating point in ProtoJSON format")
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use test_case::test_case;
    type Result = std::result::Result<(), Box<dyn std::error::Error>>;

    #[test_case(9876.5)]
    #[test_case(0.0)]
    fn roundtrip(input: f32) -> Result {
        let got = float_serialize(&input, serde_json::value::Serializer)?;
        assert_eq!(input, got);
        let rt = float_deserialize(got)?;
        assert_eq!(input, rt);
        Ok(())
    }

    #[test_case(f32::NAN)]
    #[test_case(-f32::NAN)]
    fn roundtrip_nan(input: f32) -> Result {
        let got = float_serialize(&input, serde_json::value::Serializer)?;
        assert_eq!("NaN", got);
        let rt = float_deserialize(got)?;
        assert!(rt.is_nan(), "expected NaN, got {rt}");
        Ok(())
    }

    #[test_case(f32::INFINITY, "Infinity")]
    #[test_case(2.0*f32::INFINITY, "Infinity")]
    #[test_case(f32::NEG_INFINITY, "-Infinity")]
    #[test_case(2.0*f32::NEG_INFINITY, "-Infinity")]
    fn roundtrip_inf(input: f32, want: &str) -> Result {
        let got = float_serialize(&input, serde_json::value::Serializer)?;
        assert_eq!(want, got);
        let rt = float_deserialize(got)?;
        assert!(rt.is_infinite(), "expected infinite, got {rt}");
        assert_eq!(rt.is_sign_positive(), input.is_sign_positive());
        Ok(())
    }

    #[test]
    fn deserialize_expect_err() {
        assert!(
            float_deserialize(serde_json::Value::String(
                "not a special float string".to_string()
            ))
            .is_err()
        );
    }
}
