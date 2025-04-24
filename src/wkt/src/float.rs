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

fn float_serialize<S>(x: &f32, s: S) -> std::result::Result<S::Ok, S::Error>
where
    S: serde::ser::Serializer,
{
    if x.is_nan() {
        return s.serialize_str("NaN");
    }
    s.serialize_f32(*x)
}

struct FloatVisitor;

fn float_deserialize<'de, D>(deserializer: D) -> std::result::Result<f32, D::Error>
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
        match value {
            "NaN" => Ok(f32::NAN),
            "Infinity" => Ok(f32::INFINITY),
            _ => Err(serde::de::Error::invalid_value(
                serde::de::Unexpected::Other(&value.to_string()),
                &"a valid ProtoJSON string for f32 (NaN, Infinity, -Infinity)",
            )),
        }
    }

    fn visit_f32<E>(self, value: f32) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(value)
    }

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a 32-bit floating point in ProtoJSON format")
    }
}
