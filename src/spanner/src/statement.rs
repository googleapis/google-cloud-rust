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

use crate::to_value::ToValue;
use crate::types::Type;
use crate::value::Value;
use std::collections::BTreeMap;

/// A builder for [Statement].
///
/// # Example
/// ```
/// # use google_cloud_spanner::client::Statement;
/// let stmt = Statement::builder("SELECT * FROM users WHERE id = @id")
///     .add_param("id", &42)
///     .build();
/// ```
#[derive(Clone, Debug, PartialEq)]
pub struct StatementBuilder {
    sql: String,
    params: BTreeMap<String, Value>,
    param_types: BTreeMap<String, Type>,
}

impl StatementBuilder {
    pub(crate) fn new(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            params: BTreeMap::new(),
            param_types: BTreeMap::new(),
        }
    }

    /// Adds a parameter value to this Statement.
    ///
    /// The parameter value is sent without an explicit type code to Spanner. This allows Spanner
    /// to automatically infer the correct data type from the SQL string of the statement.
    /// It is recommended to use untyped parameter values, unless you explicitly want Spanner to
    /// verify that the type of the parameter value is exactly the same as the type that would
    /// otherwise be inferred from the SQL string.
    pub fn add_param<T: ToValue + ?Sized>(mut self, name: impl Into<String>, value: &T) -> Self {
        self.params.insert(name.into(), value.to_value());
        self
    }

    /// Adds a typed parameter value to this Statement.
    ///
    /// The parameter value is sent with an explicit type code to Spanner. The type code must
    /// correspond with the expression in the SQL string that the query parameter is bound to.
    pub fn add_typed_param<T: ToValue + ?Sized>(
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

    /// Builds and returns the finalized Statement object.
    pub fn build(self) -> Statement {
        Statement {
            sql: self.sql,
            params: self.params,
            param_types: self.param_types,
        }
    }
}

/// A SQL statement for execution on Spanner.
///
/// # Example
/// ```
/// # use google_cloud_spanner::client::Spanner;
/// # use google_cloud_spanner::client::Statement;
/// # async fn test_doc() -> Result<(), google_cloud_spanner::Error> {
/// let client = Spanner::builder().build().await.unwrap();
/// let db = client.database_client("projects/p/instances/i/databases/d").build().await.unwrap();
///
/// let tx = db.single_use().build();
/// let stmt = Statement::builder("SELECT * FROM users WHERE id = @id")
///     .add_param("id", &42)
///     .build();
/// let mut rs = tx.execute_query(stmt).await?;
///
/// while let Some(row) = rs.next().await {
///     let row = row?;
///     // process row
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, PartialEq)]
pub struct Statement {
    pub sql: String,
    pub(crate) params: BTreeMap<String, Value>,
    pub(crate) param_types: BTreeMap<String, Type>,
}

impl Statement {
    /// Creates a new statement builder.
    pub fn builder(sql: impl Into<String>) -> StatementBuilder {
        StatementBuilder::new(sql)
    }

    pub(crate) fn get_params(&self) -> Option<wkt::Struct> {
        if self.params.is_empty() {
            None
        } else {
            Some(
                self.params
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone().into_serde_value()))
                    .collect(),
            )
        }
    }

    pub(crate) fn get_param_types(&self) -> std::collections::HashMap<String, crate::model::Type> {
        self.param_types
            .iter()
            .map(|(k, v)| (k.clone(), v.0.clone()))
            .collect()
    }
}

impl From<StatementBuilder> for Statement {
    fn from(builder: StatementBuilder) -> Self {
        builder.build()
    }
}

impl From<String> for Statement {
    fn from(sql: String) -> Self {
        Statement::builder(sql).build()
    }
}

impl From<&str> for Statement {
    fn from(sql: &str) -> Self {
        Statement::builder(sql).build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auto_traits() {
        static_assertions::assert_impl_all!(Statement: Clone, std::fmt::Debug, PartialEq, Send, Sync);
        static_assertions::assert_impl_all!(StatementBuilder: Clone, std::fmt::Debug, PartialEq, Send, Sync);
    }

    #[test]
    fn test_untyped_param() {
        let stmt = Statement::builder("SELECT * FROM users WHERE age > @age")
            .add_param("age", &21)
            .build();

        assert_eq!(stmt.sql, "SELECT * FROM users WHERE age > @age");
        assert_eq!(stmt.param_types.len(), 0);
        assert_eq!(stmt.params.len(), 1);

        let val = stmt.params.get("age").unwrap();
        assert_eq!(val.as_string(), "21");
    }

    #[test]
    fn test_typed_param() {
        use crate::types;
        let stmt = Statement::builder("SELECT * FROM users WHERE id = @id")
            .add_typed_param("id", &"user-123", types::string())
            .build();

        assert_eq!(stmt.param_types.len(), 1);
        assert_eq!(stmt.param_types.get("id").unwrap(), &types::string());

        assert_eq!(stmt.params.len(), 1);
        let val = stmt.params.get("id").unwrap();
        assert_eq!(val.as_string(), "user-123");
    }

    #[test]
    fn test_multiple_params() {
        use crate::types;
        let stmt = Statement::builder("SELECT * FROM users WHERE age > @age AND role = @role")
            .add_param("age", &21)
            .add_typed_param("role", &"admin", types::string())
            .build();

        assert_eq!(stmt.params.len(), 2);
        assert_eq!(stmt.param_types.len(), 1);
    }

    #[test]
    fn test_from_string_conversions() {
        let stmt_str: Statement = "SELECT 1".into();
        let stmt_string: Statement = "SELECT 1".to_string().into();
        assert_eq!(stmt_str.sql, "SELECT 1");
        assert_eq!(stmt_string.sql, "SELECT 1");
        assert!(stmt_str.get_params().is_none());
        assert!(stmt_string.get_params().is_none());
        assert!(stmt_str.get_param_types().is_empty());
        assert!(stmt_string.get_param_types().is_empty());
    }

    #[test]
    fn test_from_builder_conversion() {
        use crate::types;
        let builder = Statement::builder("SELECT * FROM users WHERE age > @age AND role = @role")
            .add_param("age", &21)
            .add_typed_param("role", &"admin", types::string());

        let stmt: Statement = builder.into();
        assert_eq!(
            stmt.sql,
            "SELECT * FROM users WHERE age > @age AND role = @role"
        );
        assert_eq!(stmt.params.len(), 2);
        assert_eq!(stmt.param_types.len(), 1);
    }

    #[test]
    fn test_get_params_and_types() {
        use crate::types;
        let stmt = Statement::builder("SELECT * FROM users WHERE age > @age AND role = @role")
            .add_param("age", &21)
            .add_typed_param("role", &"admin", types::string())
            .build();

        // Test get_params mapped to Option<wkt::Struct>
        let params = stmt.get_params().unwrap();
        assert_eq!(params.len(), 2);
        assert!(params.contains_key("age"));
        assert!(params.contains_key("role"));

        // Test get_param_types mapped to HashMap<String, model::Type>
        let param_types = stmt.get_param_types();
        assert_eq!(param_types.len(), 1);
        assert!(param_types.contains_key("role"));
    }
}
