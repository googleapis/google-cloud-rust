use crate::value::Value;
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use bigdecimal::BigDecimal;
use prost_types::Value as ProtoValue;
use std::time::SystemTime;

/// A trait for converting a Rust type to a Spanner Value.
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

impl ToValue for BigDecimal {
    fn to_value(&self) -> Value {
        Value(ProtoValue {
            kind: Some(prost_types::value::Kind::StringValue(self.to_string())),
        })
    }
}

impl ToValue for SystemTime {
    fn to_value(&self) -> Value {
        let dt: chrono::DateTime<chrono::Utc> = (*self).into();
        Value(ProtoValue {
            kind: Some(prost_types::value::Kind::StringValue(dt.to_rfc3339())),
        })
    }
}

impl ToValue for chrono::DateTime<chrono::Utc> {
    fn to_value(&self) -> Value {
        Value(ProtoValue {
            kind: Some(prost_types::value::Kind::StringValue(self.to_rfc3339())),
        })
    }
}

impl ToValue for chrono::NaiveDate {
    fn to_value(&self) -> Value {
        Value(ProtoValue {
            kind: Some(prost_types::value::Kind::StringValue(
                self.format("%Y-%m-%d").to_string(),
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
