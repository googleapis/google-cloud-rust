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

//! Implements common code for generated enumerations.
//!
//! This is only used as an implementation detail. Applications should never
//! need to use these types. The generator hides them in types that are more
//! usable.

/// Implementation details for sidekick-generated enumerations.
///
/// Google services use enumerations as part of their interface. These
/// enumerations are open: the service may send new values at any time, as the
/// client libraries and applications should be ready to receive them.
///
/// Depending on the transport, the unknown enumeration values may be received
/// as integers or strings. The client libraries must be able to handle both,
/// and send back the same value it received if using the same transport.
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

/// Hold a deserialized enumeration value.
///
/// Deserialized enumeration values are either in string form or integer form.
/// They never contain both. This is a case where using a Rust lifetime makes
/// sense: we can avoid memory allocations if the string is known.
#[derive(Debug, PartialEq)]
pub enum EnumerationValue<'de> {
    Integer(i32),
    String(std::borrow::Cow<'de, str>),
}

impl<'de> serde::de::Deserialize<'de> for EnumerationValue<'de> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(Visitor)
    }
}

const EXPECTED_MSG: &str = "an integer in the i32 range";
struct Visitor;
impl<'de> serde::de::Visitor<'de> for Visitor {
    type Value = EnumerationValue<'de>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("integer or a string")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(EnumerationValue::String(std::borrow::Cow::Owned(
            value.to_string(),
        )))
    }
    fn visit_borrowed_str<E>(self, value: &'de str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(EnumerationValue::String(std::borrow::Cow::Borrowed(value)))
    }
    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if value >= i32::MAX as u64 {
            return Err(serde::de::Error::invalid_value(
                serde::de::Unexpected::Unsigned(value),
                &EXPECTED_MSG,
            ));
        }
        Ok(EnumerationValue::Integer(value as i32))
    }
    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if value >= i32::MAX as i64 || value <= i32::MIN as i64 {
            return Err(serde::de::Error::invalid_value(
                serde::de::Unexpected::Signed(value),
                &EXPECTED_MSG,
            ));
        }
        Ok(EnumerationValue::Integer(value as i32))
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
        use serde_test::assert_de_tokens;
        let want = EnumerationValue::String(std::borrow::Cow::Borrowed("BLAH"));
        assert_de_tokens(&want, &[serde_test::Token::Str("BLAH")]);

        let want = EnumerationValue::String(std::borrow::Cow::Borrowed("BLAH"));
        assert_de_tokens(&want, &[serde_test::Token::BorrowedStr("BLAH")]);

        let want = EnumerationValue::Integer(123);
        assert_de_tokens(&want, &[serde_test::Token::I32(123)]);

        let want = EnumerationValue::Integer(-123);
        assert_de_tokens(&want, &[serde_test::Token::I32(-123)]);

        Ok(())
    }

    #[test_case(i64::MAX)]
    #[test_case(i64::MIN)]
    #[test_case(u32::MAX as i64)]
    fn deserialize_out_of_range(value: i64) {
        serde_test::assert_de_tokens_error::<EnumerationValue>(
            &[serde_test::Token::I64(value)],
            &format!("invalid value: integer `{value}`, expected an integer in the i32 range"),
        );
    }

    #[test]
    fn deserialize_invalid_type() {
        serde_test::assert_de_tokens_error::<EnumerationValue>(
            &[serde_test::Token::Struct {
                name: "Unused",
                len: 2,
            }],
            "invalid type: map, expected integer or a string",
        );
    }

    #[derive(Clone, Debug, PartialEq)]
    pub struct TestEnum(Enumeration);

    impl<'de> serde::de::Deserialize<'de> for TestEnum {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            let value = EnumerationValue::deserialize(deserializer)?;
            match value {
                EnumerationValue::Integer(val) => Ok(TestEnum(Enumeration::known_num(val))),
                EnumerationValue::String(val) => Ok(TestEnum(Enumeration::known_str(val))),
            }
        }
    }

    #[test]
    fn test_deserialize_wrapped() -> TestResult {
        let input = json!("BLAH");
        let got = serde_json::from_str::<TestEnum>(&input.to_string())?;
        assert_eq!(got.0, Enumeration::known_str("BLAH"));

        let input = json!(123);
        let got = serde_json::from_str::<TestEnum>(&input.to_string())?;
        assert_eq!(got.0, Enumeration::known_num(123));

        Ok(())
    }
}
