// Copyright 2026 Google LLC
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

pub use crate::value::Value;
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use prost_types::Value as ProtoValue;
use rust_decimal::Decimal;

use std::time::SystemTime;
use time::{Date, OffsetDateTime};

/// Converts Rust types to Spanner [Value].
pub trait ToValue {
    fn to_value(&self) -> Value;
}

impl<T> ToValue for Option<T>
where
    T: ToValue,
{
    fn to_value(&self) -> Value {
        match self {
            Some(v) => v.to_value(),
            None => Value(ProtoValue {
                kind: Some(prost_types::value::Kind::NullValue(0)),
            }),
        }
    }
}

impl ToValue for Value {
    fn to_value(&self) -> Value {
        self.clone()
    }
}

impl ToValue for ProtoValue {
    fn to_value(&self) -> Value {
        Value(self.clone())
    }
}

impl ToValue for String {
    fn to_value(&self) -> Value {
        Value(ProtoValue {
            kind: Some(prost_types::value::Kind::StringValue(self.clone())),
        })
    }
}

impl ToValue for &str {
    fn to_value(&self) -> Value {
        Value(ProtoValue {
            kind: Some(prost_types::value::Kind::StringValue(self.to_string())),
        })
    }
}

impl ToValue for i64 {
    fn to_value(&self) -> Value {
        Value(ProtoValue {
            kind: Some(prost_types::value::Kind::StringValue(self.to_string())),
        })
    }
}

impl ToValue for i32 {
    fn to_value(&self) -> Value {
        Value(ProtoValue {
            kind: Some(prost_types::value::Kind::StringValue(self.to_string())),
        })
    }
}

impl ToValue for Decimal {
    fn to_value(&self) -> Value {
        Value(ProtoValue {
            kind: Some(prost_types::value::Kind::StringValue(self.to_string())),
        })
    }
}

impl ToValue for SystemTime {
    fn to_value(&self) -> Value {
        let dt = OffsetDateTime::from(*self);
        Value(ProtoValue {
            kind: Some(prost_types::value::Kind::StringValue(
                dt.format(crate::value::SPANNER_TIMESTAMP_FORMAT)
                    .expect("failed to format time"),
            )),
        })
    }
}

impl ToValue for OffsetDateTime {
    fn to_value(&self) -> Value {
        Value(ProtoValue {
            kind: Some(prost_types::value::Kind::StringValue(
                self.format(crate::value::SPANNER_TIMESTAMP_FORMAT)
                    .expect("failed to format time"),
            )),
        })
    }
}

impl ToValue for Date {
    fn to_value(&self) -> Value {
        Value(ProtoValue {
            kind: Some(prost_types::value::Kind::StringValue(
                self.format(crate::value::SPANNER_DATE_FORMAT)
                    .expect("failed to format date"),
            )),
        })
    }
}

impl ToValue for bool {
    fn to_value(&self) -> Value {
        Value(ProtoValue {
            kind: Some(prost_types::value::Kind::BoolValue(*self)),
        })
    }
}

impl ToValue for f64 {
    fn to_value(&self) -> Value {
        Value(ProtoValue {
            kind: Some(prost_types::value::Kind::NumberValue(*self)),
        })
    }
}

impl ToValue for f32 {
    fn to_value(&self) -> Value {
        Value(ProtoValue {
            kind: Some(prost_types::value::Kind::NumberValue(*self as f64)),
        })
    }
}

impl ToValue for Vec<u8> {
    fn to_value(&self) -> Value {
        Value(ProtoValue {
            kind: Some(prost_types::value::Kind::StringValue(
                BASE64_STANDARD.encode(self),
            )),
        })
    }
}

impl<T> ToValue for Vec<T>
where
    T: ToValue,
{
    fn to_value(&self) -> Value {
        Value(ProtoValue {
            kind: Some(prost_types::value::Kind::ListValue(
                prost_types::ListValue {
                    values: self.iter().map(|v| v.to_value().0).collect(),
                },
            )),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::Kind;
    use std::str::FromStr;

    #[test]
    fn test_to_value_string() {
        let v = "hello".to_string().to_value();
        assert_eq!(v.kind(), Kind::String);
        assert_eq!(v.as_string(), "hello");

        let v = "world".to_value();
        assert_eq!(v.kind(), Kind::String);
        assert_eq!(v.as_string(), "world");
    }

    #[test]
    fn test_to_value_int() {
        let v = 42i64.to_value();
        assert_eq!(v.kind(), Kind::String);
        assert_eq!(v.as_string(), "42");

        let v = 42i32.to_value();
        assert_eq!(v.kind(), Kind::String);
        assert_eq!(v.as_string(), "42");
    }

    #[test]
    fn test_to_value_float() {
        let v = 42.5f64.to_value();
        assert_eq!(v.kind(), Kind::Number);
        assert_eq!(v.as_f64(), 42.5);

        let v = 42.5f32.to_value();
        assert_eq!(v.kind(), Kind::Number);
        assert_eq!(v.as_f64(), 42.5);
    }

    #[test]
    fn test_to_value_bool() {
        let v = true.to_value();
        assert_eq!(v.kind(), Kind::Bool);
        assert!(v.as_bool());

        let v = false.to_value();
        assert_eq!(v.kind(), Kind::Bool);
        assert!(!v.as_bool());
    }

    #[test]
    fn test_to_value_bytes() {
        let bytes: Vec<u8> = vec![1, 2, 3];
        let v = bytes.to_value();
        assert_eq!(v.kind(), Kind::String);
        assert_eq!(v.as_string(), "AQID"); // Base64 encoded
    }

    #[test]
    fn test_to_value_decimal() {
        let d = Decimal::from_str("123.456").unwrap();
        let v = d.to_value();
        assert_eq!(v.kind(), Kind::String);
        assert_eq!(v.as_string(), "123.456");
    }

    #[test]
    fn test_to_value_date() {
        let d = Date::from_calendar_date(2023, time::Month::October, 27).unwrap();
        let v = d.to_value();
        assert_eq!(v.kind(), Kind::String);
        assert_eq!(v.as_string(), "2023-10-27");
    }

    #[test]
    fn test_to_value_timestamp() {
        let dt = OffsetDateTime::parse(
            "2023-10-27T10:00:00Z",
            &time::format_description::well_known::Rfc3339,
        )
        .unwrap();
        let v = dt.to_value();
        assert_eq!(v.kind(), Kind::String);
        assert_eq!(v.as_string(), "2023-10-27T10:00:00.000000000Z");

        let system_time: SystemTime = dt.into();
        let v = system_time.to_value();
        assert_eq!(v.kind(), Kind::String);
        assert_eq!(v.as_string(), "2023-10-27T10:00:00.000000000Z");
    }

    #[test]
    fn test_to_value_option() {
        let some_val: Option<i32> = Some(42);
        let v = some_val.to_value();
        assert_eq!(v.kind(), Kind::String);
        assert_eq!(v.as_string(), "42");

        let none_val: Option<i32> = None;
        let v = none_val.to_value();
        assert_eq!(v.kind(), Kind::Null);
    }

    #[test]
    fn test_to_value_value() {
        let v_original = 42i32.to_value();
        let v = v_original.to_value();
        assert_eq!(v, v_original);

        let v_proto = ProtoValue {
            kind: Some(prost_types::value::Kind::BoolValue(true)),
        };
        let v = v_proto.to_value();
        assert_eq!(v.kind(), Kind::Bool);
        assert!(v.as_bool());
    }

    #[test]
    fn test_to_value_array() {
        let str_array = vec!["one".to_string(), "two".to_string()];
        let v = str_array.to_value();
        assert_eq!(v.kind(), Kind::List);
        let list = v.as_list();
        assert_eq!(list.len(), 2);
        assert_eq!(
            list.get(0).expect("element 0 should exist").as_string(),
            "one"
        );
        assert_eq!(
            list.get(1).expect("element 1 should exist").as_string(),
            "two"
        );

        let int_array = vec![42i64, 100i64];
        let v = int_array.to_value();
        assert_eq!(v.kind(), Kind::List);
        let list = v.as_list();
        assert_eq!(list.len(), 2);
        assert_eq!(
            list.get(0).expect("element 0 should exist").as_string(),
            "42"
        );
        assert_eq!(
            list.get(1).expect("element 1 should exist").as_string(),
            "100"
        );

        let bool_array = vec![true, false];
        let v = bool_array.to_value();
        assert_eq!(v.kind(), Kind::List);
        let list = v.as_list();
        assert_eq!(list.len(), 2);
        assert!(list.get(0).expect("element 0 should exist").as_bool());
        assert!(!list.get(1).expect("element 1 should exist").as_bool());

        let float_array = vec![9.9f64, -2.5f64];
        let v = float_array.to_value();
        assert_eq!(v.kind(), Kind::List);
        let list = v.as_list();
        assert_eq!(list.len(), 2);
        assert_eq!(list.get(0).expect("element 0 should exist").as_f64(), 9.9);
        assert_eq!(list.get(1).expect("element 1 should exist").as_f64(), -2.5);

        let empty_array: Vec<f64> = vec![];
        let v = empty_array.to_value();
        assert_eq!(v.kind(), Kind::List);
        assert_eq!(v.as_list().len(), 0);

        let null_array: Option<Vec<i64>> = None;
        let v = null_array.to_value();
        assert_eq!(v.kind(), Kind::Null);

        let opt_array: Vec<Option<i64>> = vec![Some(42), None, Some(100)];
        let v = opt_array.to_value();
        assert_eq!(v.kind(), Kind::List);
        let list = v.as_list();
        assert_eq!(list.len(), 3);
        assert_eq!(
            list.get(0).expect("element 0 should exist").as_string(),
            "42"
        );
        assert_eq!(
            list.get(1).expect("element 1 should exist").kind(),
            Kind::Null
        );
        assert_eq!(
            list.get(2).expect("element 2 should exist").as_string(),
            "100"
        );
    }
}
