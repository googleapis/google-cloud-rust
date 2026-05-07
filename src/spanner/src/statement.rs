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

use crate::model::DirectedReadOptions;
use crate::model::execute_sql_request::QueryMode;
use crate::model::execute_sql_request::QueryOptions;
use crate::model::request_options::Priority;
use crate::to_value::ToValue;
use crate::types::Type;
use crate::value::Value;
use google_cloud_gax::backoff_policy::BackoffPolicyArg;
use google_cloud_gax::options::RequestOptions as GaxRequestOptions;
use google_cloud_gax::retry_policy::RetryPolicyArg;
use std::collections::BTreeMap;
use std::time::Duration;

/// A builder for [Statement].
///
/// # Example
/// ```
/// # use google_cloud_spanner::client::Statement;
/// let stmt = Statement::builder("SELECT * FROM users WHERE id = @id")
///     .add_param("id", &42)
///     .build();
/// ```
#[derive(Clone, Debug)]
pub struct StatementBuilder {
    sql: String,
    params: BTreeMap<String, Value>,
    param_types: BTreeMap<String, Type>,
    request_options: Option<crate::model::RequestOptions>,
    directed_read_options: Option<DirectedReadOptions>,
    query_options: Option<QueryOptions>,
    query_mode: Option<QueryMode>,
    gax_options: GaxRequestOptions,
}

impl StatementBuilder {
    pub(crate) fn new(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            params: BTreeMap::new(),
            param_types: BTreeMap::new(),
            request_options: None,
            directed_read_options: None,
            query_options: None,
            query_mode: None,
            gax_options: GaxRequestOptions::default(),
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

    /// Sets the request tag to use for this statement.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Statement;
    /// let statement = Statement::builder("SELECT * FROM users")
    ///     .with_request_tag("my-tag")
    ///     .build();
    /// ```
    ///
    /// See also: [Troubleshooting with tags](https://docs.cloud.google.com/spanner/docs/introspection/troubleshooting-with-tags)
    pub fn with_request_tag(mut self, tag: impl Into<String>) -> Self {
        self.request_options
            .get_or_insert_with(crate::model::RequestOptions::default)
            .request_tag = tag.into();
        self
    }

    /// Sets the RPC priority to use for this statement.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Statement;
    /// # use google_cloud_spanner::model::request_options::Priority;
    /// let statement = Statement::builder("SELECT * FROM users")
    ///     .with_priority(Priority::Low)
    ///     .build();
    /// ```
    pub fn with_priority(mut self, priority: Priority) -> Self {
        self.request_options
            .get_or_insert_with(crate::model::RequestOptions::default)
            .priority = priority;
        self
    }

    /// Sets the directed read options for this statement.
    ///
    /// ```
    /// # use google_cloud_spanner::client::Statement;
    /// # use google_cloud_spanner::model::DirectedReadOptions;
    /// let dro = DirectedReadOptions::default();
    /// let stmt = Statement::builder("SELECT * FROM users")
    ///     .with_directed_read_options(dro)
    ///     .build();
    /// ```
    ///
    /// DirectedReadOptions can only be specified for a read-only transaction,
    /// otherwise Spanner returns an INVALID_ARGUMENT error.
    pub fn with_directed_read_options(mut self, options: DirectedReadOptions) -> Self {
        self.directed_read_options = Some(options);
        self
    }

    /// Sets the query options to use for this statement.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Statement;
    /// # use google_cloud_spanner::model::execute_sql_request::QueryOptions;
    /// let options = QueryOptions::default()
    ///     .set_optimizer_version("latest");
    /// let statement = Statement::builder("SELECT * FROM users")
    ///     .with_query_options(options)
    ///     .build();
    /// ```
    pub fn with_query_options(mut self, options: QueryOptions) -> Self {
        self.query_options = Some(options);
        self
    }

    /// Sets the query mode to use for this statement.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Statement;
    /// # use google_cloud_spanner::model::execute_sql_request::QueryMode;
    /// let statement = Statement::builder("SELECT * FROM users")
    ///     .with_query_mode(QueryMode::Plan)
    ///     .build();
    /// ```
    pub fn with_query_mode(mut self, mode: QueryMode) -> Self {
        self.query_mode = Some(mode);
        self
    }

    /// Sets the per-attempt timeout for this statement.
    pub fn with_attempt_timeout(mut self, timeout: Duration) -> Self {
        self.gax_options.set_attempt_timeout(timeout);
        self
    }

    /// Sets the retry policy for this statement.
    pub fn with_retry_policy(mut self, policy: impl Into<RetryPolicyArg>) -> Self {
        self.gax_options.set_retry_policy(policy);
        self
    }

    /// Sets the backoff policy for this statement.
    pub fn with_backoff_policy(mut self, policy: impl Into<BackoffPolicyArg>) -> Self {
        self.gax_options.set_backoff_policy(policy);
        self
    }

    /// Builds and returns the finalized Statement object.
    pub fn build(self) -> Statement {
        Statement {
            sql: self.sql,
            params: self.params,
            param_types: self.param_types,
            request_options: self.request_options,
            directed_read_options: self.directed_read_options,
            query_options: self.query_options,
            query_mode: self.query_mode,
            gax_options: self.gax_options,
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
#[derive(Clone, Debug)]
pub struct Statement {
    pub sql: String,
    pub(crate) params: BTreeMap<String, Value>,
    pub(crate) param_types: BTreeMap<String, Type>,
    pub(crate) request_options: Option<crate::model::RequestOptions>,
    pub(crate) directed_read_options: Option<DirectedReadOptions>,
    pub(crate) query_options: Option<QueryOptions>,
    pub(crate) query_mode: Option<QueryMode>,
    gax_options: GaxRequestOptions,
}

impl Statement {
    /// Creates a new statement builder.
    pub fn builder(sql: impl Into<String>) -> StatementBuilder {
        StatementBuilder::new(sql)
    }

    pub(crate) fn gax_options(&self) -> &GaxRequestOptions {
        &self.gax_options
    }

    /// Sets the query mode to use for this statement.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Statement;
    /// # use google_cloud_spanner::model::execute_sql_request::QueryMode;
    /// # use google_cloud_spanner::client::SingleUseReadOnlyTransaction;
    /// # async fn test_doc(tx: SingleUseReadOnlyTransaction) -> Result<(), google_cloud_spanner::Error> {
    /// let statement = Statement::builder("SELECT * FROM users WHERE id = @id")
    ///     .add_param("id", &42)
    ///     .build();
    /// let mut query_plan = tx.execute_query(statement.clone().with_query_mode(QueryMode::Plan)).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// This method consumes the statement and returns a new one with the specified mode.
    pub fn with_query_mode(mut self, mode: QueryMode) -> Self {
        self.query_mode = Some(mode);
        self
    }

    fn into_parts(
        self,
    ) -> (
        String,
        Option<wkt::Struct>,
        std::collections::HashMap<String, crate::model::Type>,
    ) {
        let params: Option<wkt::Struct> = if self.params.is_empty() {
            None
        } else {
            Some(
                self.params
                    .into_iter()
                    .map(|(k, v)| (k, v.into_serde_value()))
                    .collect(),
            )
        };
        let param_types: std::collections::HashMap<String, crate::model::Type> = self
            .param_types
            .into_iter()
            .map(|(k, v)| (k, v.0))
            .collect();
        (self.sql, params, param_types)
    }

    pub(crate) fn into_request(self) -> crate::model::ExecuteSqlRequest {
        let request_options = self.request_options.clone();
        let directed_read_options = self.directed_read_options.clone();
        let query_options = self.query_options.clone();
        let query_mode = self.query_mode.clone();
        let (sql, params, param_types) = self.into_parts();
        crate::model::ExecuteSqlRequest::default()
            .set_sql(sql)
            .set_or_clear_params(params)
            .set_param_types(param_types)
            .set_or_clear_request_options(request_options)
            .set_or_clear_directed_read_options(directed_read_options)
            .set_or_clear_query_options(query_options)
            .set_query_mode(query_mode.unwrap_or_default())
    }

    pub(crate) fn into_batch_statement(self) -> crate::model::execute_batch_dml_request::Statement {
        let (sql, params, param_types) = self.into_parts();
        crate::model::execute_batch_dml_request::Statement::default()
            .set_sql(sql)
            .set_or_clear_params(params)
            .set_param_types(param_types)
    }

    pub(crate) fn into_partition_query_request(self) -> crate::model::PartitionQueryRequest {
        let (sql, params, param_types) = self.into_parts();
        crate::model::PartitionQueryRequest::default()
            .set_sql(sql)
            .set_or_clear_params(params)
            .set_param_types(param_types)
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
    use anyhow::Context;

    #[test]
    fn test_auto_traits() {
        static_assertions::assert_impl_all!(Statement: Clone, std::fmt::Debug, Send, Sync);
        static_assertions::assert_impl_all!(StatementBuilder: Clone, std::fmt::Debug, Send, Sync);
    }

    #[test]
    fn test_untyped_param() {
        let stmt = Statement::builder("SELECT * FROM users WHERE age > @age")
            .add_param("age", &21)
            .build();

        assert_eq!(stmt.sql, "SELECT * FROM users WHERE age > @age");
        assert_eq!(stmt.param_types.len(), 0);
        assert_eq!(stmt.params.len(), 1);
        assert_eq!(stmt.request_options, None);

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
        assert!(stmt_str.params.is_empty());
        assert!(stmt_string.params.is_empty());
        assert!(stmt_str.param_types.is_empty());
        assert!(stmt_string.param_types.is_empty());
        assert!(stmt_str.request_options.is_none());
        assert!(stmt_string.request_options.is_none());
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
    fn test_into_request() {
        use crate::types;
        let stmt = Statement::builder("SELECT * FROM users WHERE age > @age AND role = @role")
            .add_param("age", &21)
            .add_typed_param("role", &"admin", types::string())
            .build();

        let req = stmt.into_request();

        let params = req
            .params
            .expect("ExecuteSqlRequest parameters should be set after into_request conversion");
        assert_eq!(params.len(), 2);
        assert!(params.contains_key("age"));
        assert!(params.contains_key("role"));

        let param_types = req.param_types;
        assert_eq!(param_types.len(), 1);
        assert!(param_types.contains_key("role"));
    }

    #[test]
    fn with_request_tag() {
        let stmt = Statement::builder("SELECT * FROM users")
            .with_request_tag("tag1")
            .build();
        assert_eq!(
            stmt.request_options
                .expect("request options missing")
                .request_tag,
            "tag1"
        );
    }

    #[test]
    fn with_priority() {
        let stmt = Statement::builder("SELECT * FROM users")
            .with_priority(Priority::High)
            .build();
        assert_eq!(
            stmt.request_options
                .expect("request options missing")
                .priority,
            Priority::High
        );
    }

    #[test]
    fn with_directed_read_options() {
        let dro = DirectedReadOptions::default();
        let stmt = Statement::builder("SELECT * FROM users")
            .with_directed_read_options(dro.clone())
            .build();
        assert_eq!(stmt.directed_read_options, Some(dro));
    }

    #[test]
    fn with_query_options() -> anyhow::Result<()> {
        let query_options = QueryOptions::default().set_optimizer_version("1");
        let stmt = Statement::builder("SELECT * FROM users")
            .with_query_options(query_options.clone())
            .build();
        assert_eq!(
            stmt.query_options
                .as_ref()
                .context("query options missing")?
                .optimizer_version,
            "1"
        );

        let req = stmt.into_request();
        assert_eq!(
            req.query_options
                .context("query options missing in request")?
                .optimizer_version,
            "1"
        );
        Ok(())
    }

    #[test]
    fn with_query_mode() -> anyhow::Result<()> {
        let stmt = Statement::builder("SELECT * FROM users")
            .with_query_mode(QueryMode::Plan)
            .build();
        assert_eq!(stmt.query_mode, Some(QueryMode::Plan));

        let req = stmt.into_request();
        assert_eq!(req.query_mode, QueryMode::Plan);
        Ok(())
    }

    #[test]
    fn statement_with_query_mode() -> anyhow::Result<()> {
        let stmt = Statement::builder("SELECT * FROM users").build();
        assert_eq!(stmt.query_mode, None);

        let stmt = stmt.with_query_mode(QueryMode::Profile);
        assert_eq!(stmt.query_mode, Some(QueryMode::Profile));

        let req = stmt.into_request();
        assert_eq!(req.query_mode, QueryMode::Profile);
        Ok(())
    }

    #[test]
    fn with_gax_options() -> anyhow::Result<()> {
        use google_cloud_gax::exponential_backoff::ExponentialBackoff;
        use google_cloud_gax::retry_policy::NeverRetry;
        use std::time::Duration;

        let stmt = Statement::builder("SELECT * FROM users")
            .with_attempt_timeout(Duration::from_secs(10))
            .with_retry_policy(NeverRetry)
            .with_backoff_policy(ExponentialBackoff::default())
            .build();

        assert_eq!(
            stmt.gax_options.attempt_timeout(),
            &Some(Duration::from_secs(10))
        );
        assert!(stmt.gax_options.retry_policy().is_some());
        assert!(stmt.gax_options.backoff_policy().is_some());

        Ok(())
    }
}
