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
    UnknownNum { str: String },
    UnknownStr { val: i32, str: String },
}

impl Enumeration {
    pub const fn known(str: &'static str, val: i32) -> Self {
        Self::Known { str, val }
    }
    pub fn known_str<T: Into<String>>(str: T) -> Self {
        Self::UnknownNum { str: str.into() }
    }
    pub fn known_num(val: i32) -> Self {
        let str = format!("UNKNOWN-NAME:{}", val);
        Self::UnknownStr { val, str }
    }
    pub fn value(&self) -> &str {
        match &self {
            Self::Known { str: s, val: _ } => s,
            Self::UnknownNum { str: s } => s,
            Self::UnknownStr { val: _, str: s } => s,
        }
    }
    pub fn numeric_value(&self) -> Option<i32> {
        match &self {
            Self::Known { str: _, val } => Some(*val),
            Self::UnknownNum { str: _ } => None,
            Self::UnknownStr { val, str: _ } => Some(*val),
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
            Self::UnknownStr { val: v, str: _ } => v.serialize(serializer),
            Self::UnknownNum { str: s } => s.serialize(serializer),
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
                Ok(Enumeration::known_str(value.to_string()))
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
                Ok(Enumeration::known_num(value as i32))
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
                Ok(Enumeration::known_num(value as i32))
            }
        }

        deserializer.deserialize_any(Visitor)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::json;
    use test_case::test_case;
    type TestResult = Result<(), Box<dyn std::error::Error>>;

    #[test]
    fn ctors() {
        let input = Enumeration::known("123", 123);
        assert_eq!(input.value(), "123");
        assert_eq!(input.numeric_value(), Some(123));

        let input = Enumeration::known_num(123);
        assert!(
            input.value().contains("123"),
            "input={input:?}, input.value()={}",
            input.value()
        );
        assert_eq!(input.numeric_value(), Some(123));

        let input = Enumeration::known_str("BLAH");
        assert_eq!(input.value(), "BLAH");
        assert_eq!(input.numeric_value(), None);
    }

    #[test]
    fn serialize() -> TestResult {
        let input = Enumeration::known("123", 123);
        let got = serde_json::to_value(&input)?;
        assert_eq!(got.as_str(), Some("123"));

        let input = Enumeration::known_num(123);
        let got = serde_json::to_value(&input)?;
        assert_eq!(got.as_number().cloned(), serde_json::Number::from_i128(123));

        let input = Enumeration::known_str("BLAH");
        let got = serde_json::to_value(&input)?;
        assert_eq!(got.as_str(), Some("BLAH"));

        Ok(())
    }

    #[test]
    fn deserialize() -> TestResult {
        let input = json!("BLAH");
        let got = serde_json::from_value::<Enumeration>(input)?;
        assert_eq!(got.value(), "BLAH");
        assert_eq!(got.numeric_value(), None);

        let input = json!(123);
        let got = serde_json::from_value::<Enumeration>(input)?;
        assert!(
            got.value().contains("123"),
            "got={got:?}, got.value()={}",
            got.value()
        );
        assert_eq!(got.numeric_value(), Some(123));

        let input = json!(-123);
        let got = serde_json::from_value::<Enumeration>(input)?;
        assert!(
            got.value().contains("-123"),
            "got={got:?}, got.value()={}",
            got.value()
        );
        assert_eq!(got.numeric_value(), Some(-123));

        Ok(())
    }

    #[test_case(i64::MAX)]
    #[test_case(i64::MIN)]
    #[test_case(u32::MAX as i64)]
    fn deserialize_out_of_range(value: i64) {
        let input = json!(value);
        let got = serde_json::from_value::<Enumeration>(input);
        assert!(got.is_err(), "{got:?}")
    }

    #[test]
    fn deserialize_invalid_type() {
        let input = json!({ "name": 123 });
        let got = serde_json::from_value::<Enumeration>(input);
        assert!(got.is_err(), "{got:?}")
    }
}
