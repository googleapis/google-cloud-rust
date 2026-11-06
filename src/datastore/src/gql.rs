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

//! GQL queries (`GqlQuery`), parameter bindings (`GqlParameter`), and execution builders.

use crate::client::Datastore;
use crate::error::ValueError;
use crate::explain::ExplainOptions;
use crate::key::PartitionId;
use crate::query::{Cursor, InitialQuerySpec, QueryStream, execute_initial_query};
use crate::read::{LookupReadMode, ReadConsistency, ReadOptions};
use crate::value::{IntoValue, Value};
use crate::{Error, Result};
use bytes::Bytes;
use std::collections::{BTreeMap, HashMap};

/// A bound parameter in a [`GqlQuery`] (either a [`Value`] or a pagination [`Cursor`]).
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum GqlParameter {
    /// A typed Datastore [`Value`] parameter.
    Value(Value),
    /// A query pagination [`Cursor`] parameter.
    Cursor(Cursor),
}

impl GqlParameter {
    pub(crate) fn to_proto(
        &self,
        default_partition: &PartitionId,
    ) -> std::result::Result<crate::model::GqlQueryParameter, ValueError> {
        match self {
            Self::Value(v) => Ok(
                crate::model::GqlQueryParameter::new().set_value(v.to_proto(default_partition)?)
            ),
            Self::Cursor(c) => Ok(crate::model::GqlQueryParameter::new().set_cursor(c.0.clone())),
        }
    }
}

/// A Google Query Language (GQL) query (`google.datastore.v1.GqlQuery`).
///
/// By default, `allow_literals` is `false` so parameter binding via [`bind`][Self::bind]
/// or [`add_positional`][Self::add_positional] is enforced against query injection.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct GqlQuery {
    pub(crate) query_string: String,
    pub(crate) allow_literals: bool,
    pub(crate) named_bindings: BTreeMap<String, GqlParameter>,
    pub(crate) positional_bindings: Vec<GqlParameter>,
}

impl GqlQuery {
    /// Creates a new [`GqlQuery`] with `allow_literals = false`.
    pub fn new(query_string: impl Into<String>) -> Self {
        Self {
            query_string: query_string.into(),
            allow_literals: false,
            named_bindings: BTreeMap::new(),
            positional_bindings: Vec::new(),
        }
    }

    /// Sets whether inline literals are permitted in the GQL query string.
    pub fn allow_literals(mut self, allow: bool) -> Self {
        self.allow_literals = allow;
        self
    }

    /// Binds a named `@name` parameter to `value`.
    pub fn bind(mut self, name: impl Into<String>, value: impl IntoValue) -> Self {
        self.named_bindings
            .insert(name.into(), GqlParameter::Value(value.into_value()));
        self
    }

    /// Binds a named `@name` parameter to a pagination [`Cursor`].
    pub fn bind_cursor(mut self, name: impl Into<String>, cursor: Cursor) -> Self {
        self.named_bindings
            .insert(name.into(), GqlParameter::Cursor(cursor));
        self
    }

    /// Appends a 1-indexed positional (`@1`, `@2`, ...) [`Value`] parameter.
    pub fn add_positional(mut self, value: impl IntoValue) -> Self {
        self.positional_bindings
            .push(GqlParameter::Value(value.into_value()));
        self
    }

    /// Appends a 1-indexed positional (`@1`, `@2`, ...) [`Cursor`] parameter.
    pub fn add_positional_cursor(mut self, cursor: Cursor) -> Self {
        self.positional_bindings.push(GqlParameter::Cursor(cursor));
        self
    }

    /// Returns the GQL query string.
    pub fn query_string(&self) -> &str {
        &self.query_string
    }

    /// Returns `true` if `allow_literals` is enabled.
    pub fn is_allow_literals(&self) -> bool {
        self.allow_literals
    }

    pub(crate) fn to_proto(
        &self,
        default_partition: &PartitionId,
    ) -> std::result::Result<crate::model::GqlQuery, ValueError> {
        let mut named = HashMap::with_capacity(self.named_bindings.len());
        for (name, param) in &self.named_bindings {
            named.insert(name.clone(), param.to_proto(default_partition)?);
        }
        let mut positional = Vec::with_capacity(self.positional_bindings.len());
        for param in &self.positional_bindings {
            positional.push(param.to_proto(default_partition)?);
        }
        Ok(crate::model::GqlQuery::new()
            .set_query_string(self.query_string.clone())
            .set_allow_literals(self.allow_literals)
            .set_named_bindings(named)
            .set_positional_bindings(positional))
    }
}

/// Request builder for [`Datastore::run_gql_query`].
#[derive(Clone, Debug)]
pub struct RunGqlQueryBuilder {
    pub(crate) client: Datastore,
    pub(crate) gql_query: GqlQuery,
    pub(crate) namespace_id: Option<String>,
    pub(crate) read_options: ReadOptions,
    pub(crate) transaction_id: Option<Bytes>,
    pub(crate) explain_options: Option<ExplainOptions>,
    pub(crate) options: crate::RequestOptions,
}

impl RunGqlQueryBuilder {
    /// Overrides the namespace ID for this GQL query execution.
    pub fn with_namespace_id(mut self, namespace_id: impl Into<String>) -> Self {
        self.namespace_id = Some(namespace_id.into());
        self
    }

    /// Configures the [`ReadConsistency`] for this GQL query.
    pub fn with_consistency(mut self, consistency: ReadConsistency) -> Self {
        self.read_options = ReadOptions {
            consistency: Some(consistency),
            read_time: None,
        };
        self
    }

    /// Configures a historical snapshot `read_time` for this GQL query.
    pub fn with_read_time(mut self, read_time: wkt::Timestamp) -> Self {
        self.read_options = ReadOptions::at_read_time(read_time);
        self
    }

    /// Configures the [`ReadOptions`] for this GQL query.
    pub fn with_read_options(mut self, read_options: ReadOptions) -> Self {
        self.read_options = read_options;
        self
    }

    /// Requests query planning and/or execution profiling via [`ExplainOptions`].
    pub fn with_explain_options(mut self, explain_options: ExplainOptions) -> Self {
        self.explain_options = Some(explain_options);
        self
    }

    /// Overrides the per-request [RequestOptions][google_cloud_gax::options::RequestOptions].
    pub fn with_options(mut self, options: impl Into<crate::RequestOptions>) -> Self {
        self.options = options.into();
        self
    }

    /// Executes the initial `RunQuery` RPC with the GQL query and returns a [`QueryStream`]
    /// (automatically continuing with the server-compiled structured [`Query`][crate::query::Query] across batches).
    pub async fn send(self) -> Result<QueryStream> {
        let mut partition_id = self.client.partition_id();
        if let Some(ns) = self.namespace_id {
            partition_id = partition_id.with_namespace_id(ns);
        }
        let proto_gql = self
            .gql_query
            .to_proto(&partition_id)
            .map_err(|e| Error::ser(Box::new(e)))?;
        let read_mode = match self.transaction_id {
            Some(tx_id) => LookupReadMode::Transaction(tx_id),
            None => LookupReadMode::NonTransactional(self.read_options),
        };
        let (stream, _) = execute_initial_query(
            self.client,
            partition_id,
            InitialQuerySpec::Gql(proto_gql),
            read_mode,
            self.explain_options,
            self.options,
        )
        .await?;
        Ok(stream)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity::Entity;
    use crate::generated::gapic_dataplane::stub::Datastore as DatastoreStub;
    use crate::key::Key;
    use crate::model::{
        EntityResult, KindExpression, Query as ProtoQuery, QueryResultBatch, RunQueryRequest,
        RunQueryResponse, query_result_batch::MoreResultsType,
    };
    use google_cloud_gax::options::RequestOptions;
    use google_cloud_gax::response::Response;

    mockall::mock! {
        #[derive(Debug)]
        DatastoreMock {}
        impl DatastoreStub for DatastoreMock {
            async fn run_query(
                &self,
                req: RunQueryRequest,
                options: RequestOptions,
            ) -> Result<Response<RunQueryResponse>>;
        }
    }

    fn test_client(mock: MockDatastoreMock) -> Datastore {
        Datastore::from_stub("test-proj", "test-db", "test-ns", mock)
    }

    fn make_entity_result(id: i64, name: &str) -> EntityResult {
        let key = Key::with_id("User", id)
            .unwrap()
            .with_partition_id(PartitionId::new("test-proj"));
        let mut entity = Entity::with_key(key.clone());
        entity.set("name", name);
        EntityResult::new()
            .set_entity(entity.to_proto(key.partition_id()).unwrap())
            .set_version(1)
    }

    #[tokio::test]
    async fn gql_query_continues_with_compiled_structured_query_on_not_finished() {
        let mut mock = MockDatastoreMock::new();

        // Batch 1 receives a GqlQuery with named and positional bindings, returns NOT_FINISHED
        // along with the compiled structured `query`.
        mock.expect_run_query()
            .times(1)
            .withf(|req, _| {
                let Some(gql) = req.gql_query() else {
                    return false;
                };
                gql.query_string == "SELECT * FROM User WHERE role = @role AND level >= @1"
                    && !gql.allow_literals
                    && gql.named_bindings.contains_key("role")
                    && gql.positional_bindings.len() == 1
            })
            .returning(|_, _| {
                let compiled = ProtoQuery::new()
                    .set_kind([KindExpression::new().set_name("User")])
                    .set_limit(2);
                let batch = QueryResultBatch::new()
                    .set_entity_results([make_entity_result(1, "Alice")])
                    .set_end_cursor(Bytes::from_static(b"gql-cursor-1"))
                    .set_more_results(MoreResultsType::NotFinished);
                Ok(Response::from(
                    RunQueryResponse::new().set_query(compiled).set_batch(batch),
                ))
            });

        // Batch 2 automatically uses the compiled structured `Query` with `start_cursor = "gql-cursor-1"`
        // and adjusted `limit = 1` (2 - 1).
        mock.expect_run_query()
            .times(1)
            .withf(|req, _| {
                let Some(q) = req.query() else {
                    return false;
                };
                q.kind.len() == 1
                    && q.kind[0].name == "User"
                    && q.start_cursor == Bytes::from_static(b"gql-cursor-1")
                    && q.limit == Some(1)
            })
            .returning(|_, _| {
                let batch = QueryResultBatch::new()
                    .set_entity_results([make_entity_result(2, "Bob")])
                    .set_end_cursor(Bytes::from_static(b"gql-cursor-2"))
                    .set_more_results(MoreResultsType::NoMoreResults);
                Ok(Response::from(RunQueryResponse::new().set_batch(batch)))
            });

        let client = test_client(mock);
        let gql = GqlQuery::new("SELECT * FROM User WHERE role = @role AND level >= @1")
            .bind("role", "admin")
            .add_positional(5_i64);

        let mut stream = client
            .run_gql_query(gql)
            .send()
            .await
            .expect("run_gql_query");

        let mut names = Vec::new();
        while let Some(entity) = stream.next_entity().await.expect("next_entity") {
            names.push(entity.get::<String>("name").expect("name"));
        }

        assert_eq!(names, vec!["Alice", "Bob"]);
    }
}
