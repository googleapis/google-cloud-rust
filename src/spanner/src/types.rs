use crate::generated::gapic_dataplane::model::{Type, TypeAnnotationCode, TypeCode};
use std::sync::LazyLock;

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
    t.type_annotation = TypeAnnotationCode::PgNumeric;
    t
});
static TYPE_PG_JSONB: LazyLock<Type> = LazyLock::new(|| {
    let mut t = create_type(TypeCode::Json);
    t.type_annotation = TypeAnnotationCode::PgJsonb;
    t
});
static TYPE_PG_OID: LazyLock<Type> = LazyLock::new(|| {
    let mut t = create_type(TypeCode::Int64);
    t.type_annotation = TypeAnnotationCode::PgOid;
    t
});

pub fn int64() -> Type {
    TYPE_INT64.clone()
}

pub fn string() -> Type {
    TYPE_STRING.clone()
}

pub fn bool() -> Type {
    TYPE_BOOL.clone()
}

pub fn float32() -> Type {
    TYPE_FLOAT32.clone()
}

pub fn float64() -> Type {
    TYPE_FLOAT64.clone()
}

pub fn json() -> Type {
    TYPE_JSON.clone()
}

pub fn bytes() -> Type {
    TYPE_BYTES.clone()
}

pub fn timestamp() -> Type {
    TYPE_TIMESTAMP.clone()
}

pub fn date() -> Type {
    TYPE_DATE.clone()
}

pub fn numeric() -> Type {
    TYPE_NUMERIC.clone()
}

pub fn pg_numeric() -> Type {
    TYPE_PG_NUMERIC.clone()
}

pub fn pg_jsonb() -> Type {
    TYPE_PG_JSONB.clone()
}

pub fn pg_oid() -> Type {
    TYPE_PG_OID.clone()
}

pub fn array(element_type: Type) -> Type {
    let mut t = create_type(TypeCode::Array);
    t.array_element_type = Some(Box::new(element_type));
    t
}

pub(crate) fn create_type(code: TypeCode) -> Type {
    Type {
        code,
        array_element_type: None,
        struct_type: None,
        type_annotation: TypeAnnotationCode::Unspecified,
        proto_type_fqn: String::new(),
        _unknown_fields: Default::default(),
    }
}
