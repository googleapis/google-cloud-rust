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

//! Implementation details provided by the `google-cloud-sdk` crate.
//!
//! These types are intended for developers of the Google Cloud client libraries
//! for Rust. They are undocumented and may change at any time.

pub type F32 = Float<f32>;
pub type F64 = Float<f64>;

pub struct Float<T>(std::marker::PhantomData<T>);

impl<T> serde_with::SerializeAs<T> for Float<T>
where
    T: num_traits::Float,
{
    fn serialize_as<S>(value: &T, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        match value {
            x if x.is_nan() => serializer.serialize_str("NaN"),
            x if x.is_infinite() && x.is_sign_negative() => serializer.serialize_str("-Infinity"),
            x if x.is_infinite() => serializer.serialize_str("Infinity"),
            x => x.serialize(serializer),
        }
    }
}

impl<'de, T> serde_with::DeserializeAs<'de, T> for Float<T>
where
    T: num_traits::Float + 'de,
{
    fn deserialize_as<D>(deserializer: D) -> Result<T, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        deserializer.deserialize_any(FloatVisitor::<T>::new())
    }
}

struct FloatVisitor<T> {
    phantom: std::marker::PhantomData<T>,
}

impl<T> FloatVisitor<T> {
    fn new() -> Self {
        FloatVisitor {
            phantom: std::marker::PhantomData,
        }
    }
}

impl<'de, T> serde::de::Visitor<'de> for FloatVisitor<T>
where
    T: num_traits::Float,
{
    type Value = T;

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        match value {
            "NaN" => Ok(T::nan()),
            "Infinity" => Ok(T::infinity()),
            "-Infinity" => Ok(T::neg_infinity()),
            _ => Err(serde::de::Error::invalid_value(
                serde::de::Unexpected::Other(value),
                &format!(
                    "a valid ProtoJSON string for {} (NaN, Infinity, -Infinity)",
                    std::any::type_name::<T>()
                )
                .as_str(),
            )),
        }
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        num_traits::NumCast::from(value).ok_or_else(|| {
            serde::de::Error::invalid_value(
                serde::de::Unexpected::Float(value),
                // This error condition should be unreachable, since precision loss
                // is allowed.
                &format!("a valid {} value", std::any::type_name::<T>()).as_str(),
            )
        })
    }

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str(&format!(
            "a {}-bit floating point in ProtoJSON format",
            std::mem::size_of::<T>() * 8 // bit size = byte size of T * 8
        ))
    }
}

// Trait to abstract over f32 and f64
pub trait FloatExt: num_traits::Float {
    fn serialize<S>(self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer;
}

impl FloatExt for f32 {
    fn serialize<S>(self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        serializer.serialize_f32(self)
    }
}

impl FloatExt for f64 {
    fn serialize<S>(self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        serializer.serialize_f64(self)
    }
}

// For skipping serialization of default values of bool/numeric types.
pub fn is_default<T>(t: &T) -> bool
where
    T: Default + PartialEq,
{
    *t == T::default()
}

mod enums;
pub use enums::*;

#[cfg(test)]
mod test {
    use super::*;
    use serde_with::{DeserializeAs, SerializeAs};
    use test_case::test_case;
    type Result = std::result::Result<(), Box<dyn std::error::Error>>;

    #[test_case(9876.5)]
    #[test_case(0.0)]
    fn roundtrip(input: f32) -> Result {
        let got = F32::serialize_as(&input, serde_json::value::Serializer)?;
        assert_eq!(input, got);
        let rt = F32::deserialize_as(got)?;
        assert_eq!(input, rt);
        Ok(())
    }

    #[test_case(f32::NAN)]
    #[test_case(-f32::NAN)]
    fn roundtrip_nan(input: f32) -> Result {
        let got = F32::serialize_as(&input, serde_json::value::Serializer)?;
        assert_eq!("NaN", got);
        let rt = F32::deserialize_as(got)?;
        assert!(rt.is_nan(), "expected NaN, got {rt}");
        Ok(())
    }

    #[test_case(f32::INFINITY, "Infinity")]
    #[test_case(2.0*f32::INFINITY, "Infinity")]
    #[test_case(f32::NEG_INFINITY, "-Infinity")]
    #[test_case(2.0*f32::NEG_INFINITY, "-Infinity")]
    fn roundtrip_inf(input: f32, want: &str) -> Result {
        let got = F32::serialize_as(&input, serde_json::value::Serializer)?;
        assert_eq!(want, got);
        let rt = F32::deserialize_as(got)?;
        assert!(rt.is_infinite(), "expected infinite, got {rt}");
        assert_eq!(rt.is_sign_positive(), input.is_sign_positive());
        Ok(())
    }

    #[test]
    fn deserialize_expect_err() {
        assert!(
            F32::deserialize_as(serde_json::Value::String(
                "not a special float string".to_string()
            ))
            .is_err()
        );
        assert!(F32::deserialize_as(serde_json::Value::Bool(false)).is_err());
    }
}
