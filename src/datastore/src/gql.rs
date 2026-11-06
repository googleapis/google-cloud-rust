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

use crate::Result;
use crate::client::Datastore;
use crate::explain::ExplainOptions;
use crate::query::{Cursor, QueryStream};
use crate::read::{ReadConsistency, ReadOptions};
use crate::value::{IntoValue, Value};
use bytes::Bytes;
use std::collections::BTreeMap;

/// A bound parameter in a [`GqlQuery`] (either a [`Value`] or a pagination [`Cursor`]).
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum GqlParameter {
    /// A typed Datastore [`Value`] parameter.
    Value(Value),
    /// A query pagination [`Cursor`] parameter.
    Cursor(Cursor),
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
}

/// Request builder for [`Datastore::run_gql_query`].
#[derive(Clone, Debug)]
#[allow(dead_code)]
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
        unimplemented!()
    }
}
