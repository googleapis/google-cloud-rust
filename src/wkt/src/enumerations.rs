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

//! Implements common code for enumerations.

#[derive(Clone, Debug, PartialEq)]
pub enum Enumeration {
    Known { str: &'static str, val: i32 },
    UnknownValue { str: String },
    UnknownName { val: i32, formatted: String },
}

impl Enumeration {
    pub const fn known(str: &'static str, val: i32) -> Self {
        Self::Known { str, val }
    }
    pub fn unknown_str(str: String) -> Self {
        Self::UnknownValue { str }
    }
    pub fn unknown_i32(val: i32) -> Self {
        let formatted = format!("UNKNOWN-NAME:{}", val);
        Self::UnknownName { val, formatted }
    }
    pub fn value(&self) -> &str {
        match &self {
            Self::Known { str: s, val: _ } => s,
            Self::UnknownValue { str: s } => s,
            Self::UnknownName {
                val: _,
                formatted: s,
            } => s,
        }
    }
    pub fn numeric_value(&self) -> Option<i32> {
        match &self {
            Self::Known { str: _, val } => Some(*val),
            Self::UnknownValue { str: _ } => None,
            Self::UnknownName { val, formatted: _ } => Some(*val),
        }
    }
}

impl serde::ser::Serialize for Enumeration {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        match &self {
            Self::Known { str: s, val: _ } => s.serialize(serializer),
            Self::UnknownName {
                val: v,
                formatted: _,
            } => v.serialize(serializer),
            Self::UnknownValue { str: s } => s.serialize(serializer),
        }
    }
}

impl<'de> serde::de::Deserialize<'de> for Enumeration {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Visitor;
        impl serde::de::Visitor<'_> for Visitor {
            type Value = Enumeration;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("integer or a string")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Enumeration::unknown_str(value.to_string()))
            }
            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                if value >= i32::MAX as u64 {
                    return Err(serde::de::Error::invalid_value(
                        serde::de::Unexpected::Unsigned(value),
                        &"an integer",
                    ));
                }
                Ok(Enumeration::unknown_i32(value as i32))
            }
            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                if value >= i32::MAX as i64 || value <= i32::MIN as i64 {
                    return Err(serde::de::Error::invalid_value(
                        serde::de::Unexpected::Signed(value),
                        &"an integer",
                    ));
                }
                Ok(Enumeration::unknown_i32(value as i32))
            }
        }

        deserializer.deserialize_any(Visitor)
    }
}
