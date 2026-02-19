use crate::generated::gapic_dataplane::model::{
    DirectedReadOptions, ExecuteSqlRequest, RequestOptions, Type,
    execute_sql_request::{QueryMode, QueryOptions},
};
use serde_json::{Map, Value};

#[derive(Clone, Default)]
pub struct Statement {
    pub sql: String,
    pub params: Map<String, Value>,
    pub param_types: std::collections::HashMap<String, Type>,
    pub query_options: Option<QueryOptions>,
    pub request_options: Option<RequestOptions>,
    pub query_mode: Option<QueryMode>,
    pub data_boost_enabled: bool,
    pub directed_read_options: Option<DirectedReadOptions>,
}

#[derive(Clone, Default)]
pub struct StatementBuilder {
    sql: String,
    params: Map<String, Value>,
    param_types: std::collections::HashMap<String, Type>,
    query_options: Option<QueryOptions>,
    request_options: Option<RequestOptions>,
    query_mode: Option<QueryMode>,
    data_boost_enabled: bool,
    directed_read_options: Option<DirectedReadOptions>,
}

impl StatementBuilder {
    pub(crate) fn new(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            params: Map::new(),
            param_types: std::collections::HashMap::new(),
            query_options: None,
            request_options: None,
            query_mode: None,
            data_boost_enabled: false,
            directed_read_options: None,
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

    /// Sets the query optimizer version to use for this statement.
    pub fn optimizer_version(mut self, version: impl Into<String>) -> Self {
        let mut options = self.query_options.unwrap_or_default();
        options.optimizer_version = version.into();
        self.query_options = Some(options);
        self
    }

    /// Sets the query optimizer statistics package to use for this statement.
    pub fn optimizer_statistics_package(mut self, package: impl Into<String>) -> Self {
        let mut options = self.query_options.unwrap_or_default();
        options.optimizer_statistics_package = package.into();
        self.query_options = Some(options);
        self
    }

    /// Sets the request tag to use for this statement.
    pub fn request_tag(mut self, tag: impl Into<String>) -> Self {
        let mut options = self.request_options.unwrap_or_default();
        options.request_tag = tag.into();
        self.request_options = Some(options);
        self
    }

    /// Sets the priority to use for this statement.
    pub fn priority(
        mut self,
        priority: crate::generated::gapic_dataplane::model::request_options::Priority,
    ) -> Self {
        let mut options = self.request_options.unwrap_or_default();
        options.priority = priority;
        self.request_options = Some(options);
        self
    }

    /// Sets the client context to use for this statement.
    pub fn client_context(
        mut self,
        context: crate::generated::gapic_dataplane::model::request_options::ClientContext,
    ) -> Self {
        let mut options = self.request_options.unwrap_or_default();
        options.client_context = Some(context);
        self.request_options = Some(options);
        self
    }

    /// Sets the query mode to use for this statement.
    pub fn query_mode(mut self, mode: QueryMode) -> Self {
        self.query_mode = Some(mode);
        self
    }

    /// Sets whether to use Spanner Data Boost.
    pub fn data_boost_enabled(mut self, enabled: bool) -> Self {
        self.data_boost_enabled = enabled;
        self
    }

    /// Sets the directed read options.
    pub fn directed_read_options(mut self, options: DirectedReadOptions) -> Self {
        self.directed_read_options = Some(options);
        self
    }

    /// Builds and returns the finalized Statement object.
    pub fn build(self) -> Statement {
        Statement {
            sql: self.sql,
            params: self.params,
            param_types: self.param_types,
            query_options: self.query_options,
            request_options: self.request_options,
            query_mode: self.query_mode,
            data_boost_enabled: self.data_boost_enabled,
            directed_read_options: self.directed_read_options,
        }
    }
}

impl Statement {
    /// Creates a new StatementBuilder for the given SQL string.
    pub fn new(sql: impl Into<String>) -> StatementBuilder {
        StatementBuilder::new(sql)
    }

    pub fn build_request(self, session_name: String) -> ExecuteSqlRequest {
        let mut request = ExecuteSqlRequest::new();
        request.session = session_name;
        request.sql = self.sql;
        if !self.params.is_empty() {
            request.params = Some(self.params);
            request.param_types = self.param_types;
        }
        if let Some(query_options) = self.query_options {
            request.query_options = Some(query_options);
        }
        if let Some(request_options) = self.request_options {
            request.request_options = Some(request_options);
        }
        if let Some(query_mode) = self.query_mode {
            request.query_mode = query_mode;
        }
        if self.data_boost_enabled {
            request.data_boost_enabled = self.data_boost_enabled;
        }
        if let Some(directed_read_options) = self.directed_read_options {
            request.directed_read_options = Some(directed_read_options);
        }
        request
    }
}

impl From<String> for Statement {
    fn from(sql: String) -> Self {
        Statement::new(sql).build()
    }
}

impl From<&str> for Statement {
    fn from(sql: &str) -> Self {
        Statement::new(sql).build()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_statement_builder_query_options() {
        let stmt = Statement::new("SELECT 1")
            .optimizer_version("1")
            .optimizer_statistics_package("latest")
            .build();

        assert_eq!(stmt.sql, "SELECT 1");
        assert!(stmt.query_options.is_some());
        let options = stmt.query_options.unwrap();
        assert_eq!(options.optimizer_version, "1");
        assert_eq!(options.optimizer_statistics_package, "latest");
    }

    #[test]
    fn test_statement_builder_request_options() {
        let mut client_context =
            crate::generated::gapic_dataplane::model::request_options::ClientContext::default();
        client_context
            .secure_context
            .insert("key".to_string(), wkt::Value::default());

        let stmt = Statement::new("SELECT 3")
            .request_tag("my-tag")
            .priority(crate::generated::gapic_dataplane::model::request_options::Priority::High)
            .client_context(client_context.clone())
            .build();

        assert_eq!(stmt.sql, "SELECT 3");
        assert!(stmt.request_options.is_some());
        let options = stmt.request_options.unwrap();
        assert_eq!(options.request_tag, "my-tag");
        assert_eq!(
            options.priority,
            crate::generated::gapic_dataplane::model::request_options::Priority::High.into()
        );
        assert_eq!(options.client_context.unwrap(), client_context);
    }

    #[test]
    fn test_statement_builder_query_mode() {
        let stmt = Statement::new("SELECT 4")
            .query_mode(
                crate::generated::gapic_dataplane::model::execute_sql_request::QueryMode::Profile,
            )
            .build();

        assert_eq!(stmt.sql, "SELECT 4");
        assert!(stmt.query_mode.is_some());
        assert_eq!(
            stmt.query_mode.unwrap(),
            crate::generated::gapic_dataplane::model::execute_sql_request::QueryMode::Profile
        );
    }

    #[test]
    fn test_statement_builder_data_boost_and_directed_read() {
        let directed_read_options =
            crate::generated::gapic_dataplane::model::DirectedReadOptions::default();
        let stmt = Statement::new("SELECT 5")
            .data_boost_enabled(true)
            .directed_read_options(directed_read_options.clone())
            .build();

        assert_eq!(stmt.sql, "SELECT 5");
        assert!(stmt.data_boost_enabled);
        assert!(stmt.directed_read_options.is_some());
        assert_eq!(stmt.directed_read_options.unwrap(), directed_read_options);
    }
}
