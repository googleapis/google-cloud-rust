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

//! Aggregation queries (`COUNT`, `SUM`, `AVG`) and typed [`AggregationResults`].

use crate::Result;
use crate::client::Datastore;
use crate::error::EntityError;
use crate::explain::{ExplainMetrics, ExplainOptions};
use crate::gql::GqlQuery;
use crate::query::Query;
use crate::read::{ReadConsistency, ReadOptions};
use crate::value::{FromValue, Value};
use bytes::Bytes;
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum AggregationOp {
    Count { up_to: Option<i64> },
    Sum { property: String },
    Avg { property: String },
}

/// A single aggregation specification (`COUNT`, `SUM`, or `AVG`) within an [`AggregationQuery`].
#[derive(Clone, Debug, PartialEq, Eq)]
#[allow(dead_code)]
pub struct Aggregation {
    pub(crate) alias: String,
    pub(crate) op: AggregationOp,
}

impl Aggregation {
    /// Creates an unbounded `COUNT(*)` aggregation with the given result `alias`.
    pub fn count(alias: impl Into<String>) -> Self {
        Self {
            alias: alias.into(),
            op: AggregationOp::Count { up_to: None },
        }
    }

    /// Creates a bounded `COUNT_UP_TO(limit)` aggregation with the given result `alias`.
    pub fn count_up_to(alias: impl Into<String>, up_to: i64) -> Self {
        Self {
            alias: alias.into(),
            op: AggregationOp::Count { up_to: Some(up_to) },
        }
    }

    /// Creates a `SUM(property)` aggregation with the given result `alias`.
    pub fn sum(property: impl Into<String>, alias: impl Into<String>) -> Self {
        Self {
            alias: alias.into(),
            op: AggregationOp::Sum {
                property: property.into(),
            },
        }
    }

    /// Creates an `AVG(property)` aggregation with the given result `alias`.
    pub fn avg(property: impl Into<String>, alias: impl Into<String>) -> Self {
        Self {
            alias: alias.into(),
            op: AggregationOp::Avg {
                property: property.into(),
            },
        }
    }

    /// Returns the result alias of this aggregation.
    pub fn alias(&self) -> &str {
        &self.alias
    }
}

/// An aggregation query wrapping an underlying structured [`Query`].
#[derive(Clone, Debug, PartialEq)]
pub struct AggregationQuery {
    pub(crate) nested_query: Query,
    pub(crate) aggregations: Vec<Aggregation>,
}

impl AggregationQuery {
    /// Creates an [`AggregationQuery`] over the given structured [`Query`].
    pub fn over(query: Query) -> Self {
        Self {
            nested_query: query,
            aggregations: Vec::new(),
        }
    }

    /// Adds an unbounded `COUNT(*)` aggregation under `alias`.
    pub fn count(mut self, alias: impl Into<String>) -> Self {
        self.aggregations.push(Aggregation::count(alias));
        self
    }

    /// Adds a bounded `COUNT_UP_TO(up_to)` aggregation under `alias`.
    pub fn count_up_to(mut self, alias: impl Into<String>, up_to: i64) -> Self {
        self.aggregations
            .push(Aggregation::count_up_to(alias, up_to));
        self
    }

    /// Adds a `SUM(property)` aggregation under `alias`.
    pub fn sum(mut self, property: impl Into<String>, alias: impl Into<String>) -> Self {
        self.aggregations.push(Aggregation::sum(property, alias));
        self
    }

    /// Adds an `AVG(property)` aggregation under `alias`.
    pub fn avg(mut self, property: impl Into<String>, alias: impl Into<String>) -> Self {
        self.aggregations.push(Aggregation::avg(property, alias));
        self
    }

    /// Appends a pre-built [`Aggregation`] to this query.
    pub fn add_aggregation(mut self, aggregation: Aggregation) -> Self {
        self.aggregations.push(aggregation);
        self
    }

    /// Returns the aggregations configured on this query.
    pub fn aggregations(&self) -> &[Aggregation] {
        &self.aggregations
    }
}

/// Results returned by [`Datastore::run_aggregation_query`] or [`Datastore::run_aggregation_gql_query`].
#[derive(Clone, Debug, Default, PartialEq)]
#[allow(dead_code)]
pub struct AggregationResults {
    pub(crate) values: BTreeMap<String, Value>,
    pub(crate) read_time: Option<wkt::Timestamp>,
    pub(crate) explain_metrics: Option<ExplainMetrics>,
}

impl AggregationResults {
    /// Returns the raw [`Value`] for `alias`, if present.
    pub fn get_value(&self, alias: &str) -> Option<&Value> {
        self.values.get(alias)
    }

    /// Deserializes the aggregation result for `alias` into `T` via [`FromValue`].
    pub fn get<T: FromValue>(&self, _alias: &str) -> std::result::Result<T, EntityError> {
        unimplemented!()
    }

    /// Returns the aggregation result for `alias` as an `i64` (e.g. for `COUNT` or integer `SUM`).
    pub fn get_i64(&self, _alias: &str) -> std::result::Result<i64, EntityError> {
        unimplemented!()
    }

    /// Returns the aggregation result for `alias` as an `f64` (e.g. for `AVG` or floating-point `SUM`).
    pub fn get_f64(&self, _alias: &str) -> std::result::Result<f64, EntityError> {
        unimplemented!()
    }

    /// Returns the snapshot `read_time` at which the aggregation was evaluated, if reported by the server.
    pub fn read_time(&self) -> Option<&wkt::Timestamp> {
        self.read_time.as_ref()
    }

    /// Returns the [`ExplainMetrics`] for the aggregation query, if [`ExplainOptions`] was requested.
    pub fn explain_metrics(&self) -> Option<&ExplainMetrics> {
        self.explain_metrics.as_ref()
    }
}

/// Request builder for [`Datastore::run_aggregation_query`].
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct RunAggregationQueryBuilder {
    pub(crate) client: Datastore,
    pub(crate) query: AggregationQuery,
    pub(crate) namespace_id: Option<String>,
    pub(crate) read_options: ReadOptions,
    pub(crate) transaction_id: Option<Bytes>,
    pub(crate) explain_options: Option<ExplainOptions>,
    pub(crate) options: crate::RequestOptions,
}

impl RunAggregationQueryBuilder {
    /// Overrides the namespace ID for this aggregation query execution.
    pub fn with_namespace_id(mut self, namespace_id: impl Into<String>) -> Self {
        self.namespace_id = Some(namespace_id.into());
        self
    }

    /// Configures the [`ReadConsistency`] for this aggregation query.
    pub fn with_consistency(mut self, consistency: ReadConsistency) -> Self {
        self.read_options = ReadOptions {
            consistency: Some(consistency),
            read_time: None,
        };
        self
    }

    /// Configures a historical snapshot `read_time` for this aggregation query.
    pub fn with_read_time(mut self, read_time: wkt::Timestamp) -> Self {
        self.read_options = ReadOptions::at_read_time(read_time);
        self
    }

    /// Configures the [`ReadOptions`] for this aggregation query.
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

    /// Executes the `RunAggregationQuery` RPC and returns the [`AggregationResults`].
    pub async fn send(self) -> Result<AggregationResults> {
        unimplemented!()
    }
}

/// Request builder for [`Datastore::run_aggregation_gql_query`].
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct RunAggregationGqlQueryBuilder {
    pub(crate) client: Datastore,
    pub(crate) gql_query: GqlQuery,
    pub(crate) namespace_id: Option<String>,
    pub(crate) read_options: ReadOptions,
    pub(crate) transaction_id: Option<Bytes>,
    pub(crate) explain_options: Option<ExplainOptions>,
    pub(crate) options: crate::RequestOptions,
}

impl RunAggregationGqlQueryBuilder {
    /// Overrides the namespace ID for this GQL aggregation query execution.
    pub fn with_namespace_id(mut self, namespace_id: impl Into<String>) -> Self {
        self.namespace_id = Some(namespace_id.into());
        self
    }

    /// Configures the [`ReadConsistency`] for this GQL aggregation query.
    pub fn with_consistency(mut self, consistency: ReadConsistency) -> Self {
        self.read_options = ReadOptions {
            consistency: Some(consistency),
            read_time: None,
        };
        self
    }

    /// Configures a historical snapshot `read_time` for this GQL aggregation query.
    pub fn with_read_time(mut self, read_time: wkt::Timestamp) -> Self {
        self.read_options = ReadOptions::at_read_time(read_time);
        self
    }

    /// Configures the [`ReadOptions`] for this GQL aggregation query.
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

    /// Executes the `RunAggregationQuery` RPC with the GQL query and returns the [`AggregationResults`].
    pub async fn send(self) -> Result<AggregationResults> {
        unimplemented!()
    }
}
