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

use std::marker::PhantomData;

#[derive(Clone, Debug, PartialEq)]
pub enum UnknownEnumValue {
    Integer(i32),
    String(String),
}

impl UnknownEnumValue {
    pub fn value(&self) -> Option<i32> {
        match self {
            Self::Integer(x) => Some(*x),
            Self::String(_) => None,
        }
    }
    pub fn name(&self) -> Option<&str> {
        match self {
            Self::Integer(_) => None,
            Self::String(x) => Some(x.as_str()),
        }
    }
}

impl serde::ser::Serialize for UnknownEnumValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::Integer(x) => serializer.serialize_i32(*x),
            Self::String(x) => serializer.serialize_str(x.as_str()),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct EnumVisitor<'lf, T> {
    name: &'lf str,
    _unused: PhantomData<T>,
}

impl<'lf, T> EnumVisitor<'lf, T> {
    pub fn new(name: &'lf str) -> Self {
        Self {
            name,
            _unused: Default::default(),
        }
    }
}

impl<T> serde::de::Visitor<'_> for EnumVisitor<'_, T>
where
    T: From<i32> + for<'a> From<&'a str>,
{
    type Value = T;
    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(T::from(value))
    }
    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if value > (i32::MAX as u64) {
            return Err(E::custom(format!(
                "out of range enum value {value} for {}",
                self.name,
            )));
        }
        Ok(T::from(value as i32))
    }
    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if value < (i32::MIN as i64) || value > (i32::MAX as i64) {
            return Err(E::custom(format!(
                "out of range enum value {value} for {}",
                self.name,
            )));
        }
        Ok(T::from(value as i32))
    }
    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str(&format!(
            "a {} enum value in string or integer format",
            self.name
        ))
    }
}

pub fn display_enum(
    f: &mut std::fmt::Formatter<'_>,
    name: Option<&str>,
    value: Option<i32>,
) -> Result<(), std::fmt::Error> {
    match (name, value) {
        (Some(n), _) => f.write_str(n),
        (None, Some(v)) => write!(f, "{v}"),
        (None, None) => unreachable!("enums must have a numeric or string value"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::field_descriptor_proto::Label;
    use serde_json::json;
    use test_case::test_case;

    #[test]
    fn unknown_enum_value_accessors() {
        let u = UnknownEnumValue::Integer(123);
        assert_eq!(u.value(), Some(123));
        assert_eq!(u.name(), None);

        let u = UnknownEnumValue::String("RED".into());
        assert_eq!(u.value(), None);
        assert_eq!(u.name(), Some("RED"));
    }

    #[test]
    fn unknown_enum_value_serialize() -> anyhow::Result<()> {
        let u = UnknownEnumValue::Integer(123);
        let got = serde_json::to_value(&u)?;
        assert_eq!(got, json!(123));

        let u = UnknownEnumValue::String("RED".into());
        let got = serde_json::to_value(&u)?;
        assert_eq!(got, json!("RED"));

        Ok(())
    }

    // This type is here to drive the `EnumVisitor` and test its functionality.
    #[derive(Clone, Debug, PartialEq)]
    enum FakeEnum {
        Red,
        Green,
        Blue,
        UnknownValue(super::UnknownEnumValue),
    }
    impl From<i32> for FakeEnum {
        fn from(value: i32) -> Self {
            match value {
                0 => Self::Red,
                1 => Self::Green,
                2 => Self::Blue,
                x => Self::UnknownValue(super::UnknownEnumValue::Integer(x)),
            }
        }
    }
    impl From<&str> for FakeEnum {
        fn from(value: &str) -> Self {
            match value {
                "RED" => Self::Red,
                "GREEN" => Self::Green,
                "BLUE" => Self::Blue,
                x => Self::UnknownValue(super::UnknownEnumValue::String(x.to_string())),
            }
        }
    }
    impl<'de> serde::de::Deserialize<'de> for FakeEnum {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            let visitor = super::EnumVisitor::new(".test.v1.FakeEnum");
            deserializer.deserialize_any(visitor)
        }
    }

    #[test_case(json!("RED"), FakeEnum::Red)]
    #[test_case(json!("GREEN"), FakeEnum::Green)]
    #[test_case(json!("BLUE"), FakeEnum::Blue)]
    #[test_case(json!("UNKNOWN"), FakeEnum::UnknownValue(super::UnknownEnumValue::String("UNKNOWN".into())))]
    #[test_case(json!(0), FakeEnum::Red)]
    #[test_case(json!(1), FakeEnum::Green)]
    #[test_case(json!(2), FakeEnum::Blue)]
    #[test_case(json!(42), FakeEnum::UnknownValue(super::UnknownEnumValue::Integer(42)))]
    fn visitor(input: serde_json::Value, want: FakeEnum) -> anyhow::Result<()> {
        let got = serde_json::from_value::<FakeEnum>(input)?;
        assert_eq!(got, want);
        Ok(())
    }

    #[test_case(json!(i32::MAX as u64 + 2))]
    #[test_case(json!(i32::MAX as i64 + 2))]
    #[test_case(json!(i32::MIN as i64 - 2))]
    #[test_case(json!({}))]
    fn visitor_out_of_range(input: serde_json::Value) {
        let got = serde_json::from_value::<FakeEnum>(input);
        assert!(got.is_err(), "{got:?}");
        assert!(format!("{got:?}").contains(".test.v1.FakeEnum"), "{got:?}");
    }

    struct TestDisplay {
        name: Option<String>,
        value: Option<i32>,
    }
    impl std::fmt::Display for TestDisplay {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let tmp = self.name.clone();
            super::display_enum(f, tmp.as_deref(), self.value)
        }
    }

    #[test_case(Some("NAME".into()), Some(1), "NAME")]
    #[test_case(Some("NAME".into()), None, "NAME")]
    #[test_case(None, Some(1), "1")]
    fn test_display(name: Option<String>, value: Option<i32>, want: &str) {
        let input = TestDisplay { name, value };
        let got = format!("{input}");
        assert_eq!(got.as_str(), want);
    }

    #[test_case(json!("LABEL_OPTIONAL"), Label::Optional)]
    #[test_case(json!(1), Label::Optional)]
    #[test_case(json!("UNKNOWN_VALUE"), Label::from("UNKNOWN_VALUE"))]
    #[test_case(json!(42), Label::from(42))]
    fn deserialize(input: serde_json::Value, want: Label) -> anyhow::Result<()> {
        let got = serde_json::from_value::<Label>(input)?;
        assert_eq!(got, want);
        Ok(())
    }

    #[test_case(Label::Optional, "LABEL_OPTIONAL")]
    #[test_case(Label::from(1), "LABEL_OPTIONAL")]
    #[test_case(Label::from("LABEL_OPTIONAL"), "LABEL_OPTIONAL")]
    #[test_case(Label::from("UNKNOWN_VALUE"), "UNKNOWN_VALUE")]
    #[test_case(Label::from(42), "42")]
    fn display_enum(input: Label, want: &str) {
        let got = format!("{input}");
        assert_eq!(got, want);
    }
}
