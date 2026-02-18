use crate::generated::gapic_dataplane::model::Type;
use serde_json::{Map, Value};

pub struct Statement {
    pub sql: String,
    pub params: Map<String, Value>,
    pub param_types: std::collections::HashMap<String, Type>,
}

impl Statement {
    pub fn new(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            params: Map::new(),
            param_types: std::collections::HashMap::new(),
        }
    }

    /// Adds a parameter value to this Statement.
    ///
    /// The parameter value is sent without an explicit type code to Spanner. This allows Spanner
    /// to automatically infer the correct data type from the SQL string of the statement.
    /// It is recommended to use untyped parameter values, unless you explicitly want Spanner to
    /// verify that the type of the parameter value is exactly the same as the type that would
    /// otherwise be inferred from the SQL string.
    pub fn add_param<T: ToSpannerValue + ?Sized>(
        mut self,
        name: impl Into<String>,
        value: &T,
    ) -> Self {
        let name = name.into();
        self.params.insert(name.clone(), value.to_value());
        self
    }

    /// Adds a typed parameter value to this Statement.
    ///
    /// The parameter value is sent with an explicit type code to Spanner. The type code must
    /// correspond with the expression in the SQL string that the query parameter is bound to.
    pub fn add_typed_param<T: ToSpannerValue + ?Sized>(
        mut self,
        name: impl Into<String>,
        value: &T,
        param_type: Type,
    ) -> Self {
        let name = name.into();
        self.params.insert(name.clone(), value.to_value());
        self.param_types.insert(name, param_type);
        self
    }
}

impl From<String> for Statement {
    fn from(sql: String) -> Self {
        Statement::new(sql)
    }
}
impl From<&str> for Statement {
    fn from(sql: &str) -> Self {
        Statement::new(sql)
    }
}

pub trait ToSpannerValue {
    fn to_value(&self) -> Value;
}

impl ToSpannerValue for String {
    fn to_value(&self) -> Value {
        Value::String(self.clone())
    }
}

impl ToSpannerValue for &str {
    fn to_value(&self) -> Value {
        Value::String(self.to_string())
    }
}

impl ToSpannerValue for i64 {
    fn to_value(&self) -> Value {
        Value::String(self.to_string())
    }
}

impl ToSpannerValue for i32 {
    fn to_value(&self) -> Value {
        Value::String(self.to_string())
    }
}

impl ToSpannerValue for bool {
    fn to_value(&self) -> Value {
        Value::Bool(*self)
    }
}

impl ToSpannerValue for f64 {
    fn to_value(&self) -> Value {
        serde_json::Number::from_f64(*self)
            .map(Value::Number)
            .unwrap_or(Value::Null)
    }
}

impl ToSpannerValue for f32 {
    fn to_value(&self) -> Value {
        serde_json::Number::from_f64(*self as f64)
            .map(Value::Number)
            .unwrap_or(Value::Null)
    }
}
