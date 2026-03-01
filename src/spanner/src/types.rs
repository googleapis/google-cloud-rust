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

use crate::generated::gapic_dataplane::model::TypeAnnotationCode;
use gaxi::prost::ConvertError;
use std::sync::LazyLock;

/// Spanner type definition.
#[derive(Clone, Debug, PartialEq, Default)]
#[repr(transparent)]
pub struct Type(pub(crate) crate::generated::gapic_dataplane::model::Type);

macro_rules! define_type_code {
    ($($variant:ident = $val:expr),* $(,)?) => {
        /// Spanner type code.
        #[derive(Clone, Debug, PartialEq, Copy, Default)]
        #[repr(i32)]
        pub enum TypeCode {
            #[default]
            Unspecified = 0,
            $($variant = $val),*,
            Unknown(i32),
        }

        impl From<i32> for TypeCode {
            fn from(value: i32) -> Self {
                match value {
                    0 => TypeCode::Unspecified,
                    $($val => TypeCode::$variant),*,
                    v => TypeCode::Unknown(v),
                }
            }
        }

        impl From<TypeCode> for i32 {
            fn from(value: TypeCode) -> Self {
                match value {
                    TypeCode::Unspecified => 0,
                    $(TypeCode::$variant => $val),*,
                    TypeCode::Unknown(v) => v,
                }
            }
        }

        impl From<crate::generated::gapic_dataplane::model::TypeCode> for TypeCode {
            fn from(value: crate::generated::gapic_dataplane::model::TypeCode) -> Self {
                 match value.value() {
                    Some(v) => v.into(),
                    None => TypeCode::Unspecified,
                }
            }
        }

        impl From<TypeCode> for crate::generated::gapic_dataplane::model::TypeCode {
            fn from(value: TypeCode) -> Self {
                let v: i32 = value.into();
                v.into()
            }
        }
    };
}

// The values here must match the values in the `google.spanner.v1.TypeCode` enum.
// We cannot use the generated constants directly because they are not exposed as public constants.
// See https://github.com/googleapis/googleapis/blob/master/google/spanner/v1/type.proto
define_type_code!(
    Bool = 1,
    Int64 = 2,
    Float64 = 3,
    Float32 = 15,
    Timestamp = 4,
    Date = 5,
    String = 6,
    Bytes = 7,
    Array = 8,
    Struct = 9,
    Numeric = 10,
    Json = 11,
    Proto = 13,
    Enum = 14,
    Interval = 16,
    Uuid = 17,
);

impl From<crate::generated::gapic_dataplane::model::Type> for Type {
    fn from(value: crate::generated::gapic_dataplane::model::Type) -> Self {
        Type(value)
    }
}

impl From<Type> for crate::generated::gapic_dataplane::model::Type {
    fn from(value: Type) -> Self {
        value.0
    }
}

impl Type {
    /// Returns the type code.
    pub fn code(&self) -> TypeCode {
        self.0.code.clone().into()
    }
}

impl gaxi::prost::ToProto<i32> for TypeCode {
    type Output = i32;

    fn to_proto(self) -> Result<i32, ConvertError> {
        let internal: crate::generated::gapic_dataplane::model::TypeCode = self.into();

        internal.to_proto()
    }
}

impl gaxi::prost::ToProto<crate::generated::gapic_dataplane::model::Type> for Type {
    type Output = crate::generated::gapic_dataplane::model::Type;

    fn to_proto(self) -> Result<crate::generated::gapic_dataplane::model::Type, ConvertError> {
        Ok(self.0)
    }
}

static TYPE_INT64: LazyLock<Type> = LazyLock::new(|| create_type(TypeCode::Int64));
static TYPE_STRING: LazyLock<Type> = LazyLock::new(|| create_type(TypeCode::String));
static TYPE_BOOL: LazyLock<Type> = LazyLock::new(|| create_type(TypeCode::Bool));
static TYPE_FLOAT32: LazyLock<Type> = LazyLock::new(|| create_type(TypeCode::Float32));
static TYPE_FLOAT64: LazyLock<Type> = LazyLock::new(|| create_type(TypeCode::Float64));
static TYPE_JSON: LazyLock<Type> = LazyLock::new(|| create_type(TypeCode::Json));
static TYPE_BYTES: LazyLock<Type> = LazyLock::new(|| create_type(TypeCode::Bytes));
static TYPE_TIMESTAMP: LazyLock<Type> = LazyLock::new(|| create_type(TypeCode::Timestamp));
static TYPE_DATE: LazyLock<Type> = LazyLock::new(|| create_type(TypeCode::Date));
static TYPE_NUMERIC: LazyLock<Type> = LazyLock::new(|| create_type(TypeCode::Numeric));
static TYPE_PG_NUMERIC: LazyLock<Type> = LazyLock::new(|| {
    let mut t = create_type(TypeCode::Numeric);
    t.0.type_annotation = TypeAnnotationCode::PgNumeric;
    t
});
static TYPE_PG_JSONB: LazyLock<Type> = LazyLock::new(|| {
    let mut t = create_type(TypeCode::Json);
    t.0.type_annotation = TypeAnnotationCode::PgJsonb;
    t
});
static TYPE_PG_OID: LazyLock<Type> = LazyLock::new(|| {
    let mut t = create_type(TypeCode::Int64);
    t.0.type_annotation = TypeAnnotationCode::PgOid;
    t
});

/// Returns a `Type` representing `INT64` (GoogleSQL) or `bigint` (PostgreSQL).
pub fn int64() -> Type {
    TYPE_INT64.clone()
}

/// Returns a `Type` representing `STRING` (GoogleSQL) or `character varying` (PostgreSQL).
pub fn string() -> Type {
    TYPE_STRING.clone()
}

/// Returns a `Type` representing `BOOL` (GoogleSQL) or `boolean` (PostgreSQL).
pub fn bool() -> Type {
    TYPE_BOOL.clone()
}

/// Returns a `Type` representing `FLOAT32` (GoogleSQL) or `real` (PostgreSQL).
pub fn float32() -> Type {
    TYPE_FLOAT32.clone()
}

/// Returns a `Type` representing `FLOAT64` (GoogleSQL) or `double precision` (PostgreSQL).
pub fn float64() -> Type {
    TYPE_FLOAT64.clone()
}

/// Returns a `Type` representing `JSON` (GoogleSQL) or `jsonb` (PostgreSQL).
pub fn json() -> Type {
    TYPE_JSON.clone()
}

/// Returns a `Type` representing `BYTES` (GoogleSQL) or `bytea` (PostgreSQL).
pub fn bytes() -> Type {
    TYPE_BYTES.clone()
}

/// Returns a `Type` representing `TIMESTAMP` (GoogleSQL) or `timestamp with time zone` (PostgreSQL).
pub fn timestamp() -> Type {
    TYPE_TIMESTAMP.clone()
}

/// Returns a `Type` representing `DATE` (GoogleSQL) or `date` (PostgreSQL).
pub fn date() -> Type {
    TYPE_DATE.clone()
}

/// Returns a `Type` representing `NUMERIC` (GoogleSQL) or `numeric` (PostgreSQL).
pub fn numeric() -> Type {
    TYPE_NUMERIC.clone()
}

/// Returns a `Type` representing `numeric` (PostgreSQL).
pub fn pg_numeric() -> Type {
    TYPE_PG_NUMERIC.clone()
}

/// Returns a `Type` representing `jsonb` (PostgreSQL).
pub fn pg_jsonb() -> Type {
    TYPE_PG_JSONB.clone()
}

/// Returns a `Type` representing `oid` (PostgreSQL).
pub fn pg_oid() -> Type {
    TYPE_PG_OID.clone()
}

/// Returns a `Type` representing `ARRAY<t>` (GoogleSQL) or `t[]` (PostgreSQL).
pub fn array(element_type: Type) -> Type {
    let mut t = create_type(TypeCode::Array);
    t.0.array_element_type = Some(Box::new(element_type.0));
    t
}

/// Returns a `Type` representing `UUID` (GoogleSQL) or `uuid` (PostgreSQL).
pub fn uuid() -> Type {
    static TYPE_UUID: LazyLock<Type> = LazyLock::new(|| create_type(TypeCode::Uuid));
    TYPE_UUID.clone()
}

/// Returns a `Type` representing `INTERVAL` (GoogleSQL).
pub fn interval() -> Type {
    static TYPE_INTERVAL: LazyLock<Type> = LazyLock::new(|| create_type(TypeCode::Interval));
    TYPE_INTERVAL.clone()
}

pub(crate) fn create_type(code: TypeCode) -> Type {
    Type(crate::generated::gapic_dataplane::model::Type {
        code: code.into(),
        array_element_type: None,
        struct_type: None,
        type_annotation: TypeAnnotationCode::Unspecified,
        proto_type_fqn: String::new(),
        _unknown_fields: Default::default(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_type_code_round_trip() {
        let codes = vec![
            TypeCode::Unspecified,
            TypeCode::Bool,
            TypeCode::Int64,
            TypeCode::Float64,
            TypeCode::Float32,
            TypeCode::Timestamp,
            TypeCode::Date,
            TypeCode::String,
            TypeCode::Bytes,
            TypeCode::Array,
            TypeCode::Struct,
            TypeCode::Numeric,
            TypeCode::Json,
            TypeCode::Proto,
            TypeCode::Enum,
            TypeCode::Interval,
            TypeCode::Uuid,
        ];

        for code in codes {
            let i: i32 = code.into();
            let c: TypeCode = i.into();
            assert_eq!(code, c);

            let generated: crate::generated::gapic_dataplane::model::TypeCode = code.into();
            let back: TypeCode = generated.into();
            assert_eq!(code, back);
        }
    }

    #[test]
    fn test_unknown_type_code() {
        let i = 999;
        let code: TypeCode = i.into();
        assert_eq!(code, TypeCode::Unknown(i));
        assert_eq!(i32::from(code), i);
    }

    #[test]
    fn test_simple_types() {
        assert_eq!(int64().code(), TypeCode::Int64);
        assert_eq!(string().code(), TypeCode::String);
        assert_eq!(bool().code(), TypeCode::Bool);
        assert_eq!(float64().code(), TypeCode::Float64);
        assert_eq!(bytes().code(), TypeCode::Bytes);
        assert_eq!(timestamp().code(), TypeCode::Timestamp);
        assert_eq!(date().code(), TypeCode::Date);
        assert_eq!(numeric().code(), TypeCode::Numeric);
        assert_eq!(json().code(), TypeCode::Json);
        assert_eq!(float32().code(), TypeCode::Float32);
        assert_eq!(uuid().code(), TypeCode::Uuid);
        assert_eq!(interval().code(), TypeCode::Interval);
        // PG types are tested in test_pg_types
    }

    #[test]
    fn test_default_type() {
        let t = Type::default();
        assert_eq!(t.code(), TypeCode::Unspecified);
        assert_eq!(
            t.0.code,
            crate::generated::gapic_dataplane::model::TypeCode::Unspecified
        );
    }

    #[test]
    fn test_to_proto_traits() {
        use gaxi::prost::ToProto;
        let t = int64();
        let proto: crate::generated::gapic_dataplane::model::Type = t.clone().to_proto().unwrap();
        assert_eq!(
            proto.code,
            crate::generated::gapic_dataplane::model::TypeCode::Int64
        );

        let code = TypeCode::Int64;
        let proto_code: i32 = code.to_proto().unwrap();
        assert_eq!(proto_code, 2);
    }

    #[test]
    fn test_from_type_traits() {
        let internal_type = crate::generated::gapic_dataplane::model::Type {
            code: crate::generated::gapic_dataplane::model::TypeCode::Bool,
            ..Default::default()
        };
        let t: Type = internal_type.clone().into();
        assert_eq!(t.code(), TypeCode::Bool);

        let back: crate::generated::gapic_dataplane::model::Type = t.into();
        assert_eq!(back.code, internal_type.code);
    }

    #[test]
    fn test_array_type() {
        let t = array(int64());
        assert_eq!(t.code(), TypeCode::Array);
        assert_eq!(
            t.0.array_element_type.unwrap().code,
            crate::generated::gapic_dataplane::model::TypeCode::Int64
        );
    }

    #[test]
    fn test_pg_types() {
        assert_eq!(pg_numeric().code(), TypeCode::Numeric);
        assert_eq!(
            pg_numeric().0.type_annotation,
            TypeAnnotationCode::PgNumeric
        );

        assert_eq!(pg_jsonb().code(), TypeCode::Json);
        assert_eq!(pg_jsonb().0.type_annotation, TypeAnnotationCode::PgJsonb);

        assert_eq!(pg_oid().code(), TypeCode::Int64);
        assert_eq!(pg_oid().0.type_annotation, TypeAnnotationCode::PgOid);
    }

    #[test]
    fn test_auto_traits() {
        static_assertions::assert_impl_all!(Type: Send, Sync, Clone, std::fmt::Debug);
        static_assertions::assert_impl_all!(TypeCode: Send, Sync, Clone, std::fmt::Debug);
    }
}
