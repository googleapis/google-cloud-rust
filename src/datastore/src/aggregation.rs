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

use crate::Error;
use crate::Result;
use crate::client::Datastore;
use crate::error::{EntityError, ValueError};
use crate::explain::{ExplainMetrics, ExplainOptions};
use crate::gql::GqlQuery;
use crate::key::PartitionId;
use crate::query::Query;
use crate::read::{LookupReadMode, ReadConsistency, ReadOptions};
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

    pub(crate) fn to_proto(&self) -> crate::model::aggregation_query::Aggregation {
        let agg = crate::model::aggregation_query::Aggregation::new().set_alias(&self.alias);
        match &self.op {
            AggregationOp::Count { up_to } => {
                let mut count = crate::model::aggregation_query::aggregation::Count::new();
                if let Some(u) = up_to {
                    count = count.set_up_to(*u);
                }
                agg.set_count(count)
            }
            AggregationOp::Sum { property } => {
                let sum = crate::model::aggregation_query::aggregation::Sum::new()
                    .set_property(crate::model::PropertyReference::new().set_name(property));
                agg.set_sum(sum)
            }
            AggregationOp::Avg { property } => {
                let avg = crate::model::aggregation_query::aggregation::Avg::new()
                    .set_property(crate::model::PropertyReference::new().set_name(property));
                agg.set_avg(avg)
            }
        }
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

    pub(crate) fn to_proto(
        &self,
        default_partition: &PartitionId,
    ) -> std::result::Result<crate::model::AggregationQuery, ValueError> {
        let nested = self.nested_query.to_proto(default_partition)?;
        let aggs: Vec<_> = self
            .aggregations
            .iter()
            .map(Aggregation::to_proto)
            .collect();
        Ok(crate::model::AggregationQuery::new()
            .set_nested_query(nested)
            .set_aggregations(aggs))
    }
}

/// Results returned by [`Datastore::run_aggregation_query`] or [`Datastore::run_aggregation_gql_query`].
#[derive(Clone, Debug, Default, PartialEq)]
pub struct AggregationResults {
    pub(crate) values: BTreeMap<String, Value>,
    pub(crate) read_time: Option<wkt::Timestamp>,
    pub(crate) explain_metrics: Option<ExplainMetrics>,
}

impl AggregationResults {
    /// Returns the raw map of aggregation aliases to [`Value`]s.
    pub fn values(&self) -> &BTreeMap<String, Value> {
        &self.values
    }

    /// Returns the raw [`Value`] for `alias`, if present.
    pub fn get_value(&self, alias: &str) -> Option<&Value> {
        self.values.get(alias)
    }

    /// Deserializes the aggregation result for `alias` into `T` via [`FromValue`].
    pub fn get<T: FromValue>(&self, alias: &str) -> std::result::Result<T, EntityError> {
        match self.values.get(alias) {
            Some(v) => T::from_value(v).map_err(|source| EntityError::InvalidProperty {
                name: alias.to_string(),
                source,
            }),
            None => {
                T::from_missing().ok_or_else(|| EntityError::MissingProperty(alias.to_string()))
            }
        }
    }

    /// Returns the aggregation result for `alias` as an `i64` (e.g. for `COUNT` or integer `SUM`).
    pub fn get_i64(&self, alias: &str) -> std::result::Result<i64, EntityError> {
        self.get::<i64>(alias)
    }

    /// Returns the aggregation result for `alias` as an `f64` (e.g. for `AVG` or floating-point `SUM`).
    pub fn get_f64(&self, alias: &str) -> std::result::Result<f64, EntityError> {
        self.get::<f64>(alias)
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

#[derive(Clone, Debug)]
pub(crate) enum InitialAggregationSpec {
    Structured(Box<crate::model::AggregationQuery>),
    Gql(crate::model::GqlQuery),
}

pub(crate) async fn execute_aggregation_query(
    client: Datastore,
    partition_id: PartitionId,
    spec: InitialAggregationSpec,
    read_mode: LookupReadMode,
    explain_options: Option<ExplainOptions>,
    options: crate::RequestOptions,
) -> Result<(AggregationResults, Option<Bytes>)> {
    let proto_partition = partition_id.to_proto();
    let mut req = client
        .inner
        .gapic
        .run_aggregation_query()
        .set_project_id(client.project_id())
        .set_database_id(client.database_id())
        .set_partition_id(proto_partition)
        .with_options(options);

    req = match spec {
        InitialAggregationSpec::Structured(q) => req.set_aggregation_query(*q),
        InitialAggregationSpec::Gql(gql) => req.set_gql_query(gql),
    };

    if let Some(ro) = read_mode.to_proto(None) {
        req = req.set_read_options(ro);
    }
    if let Some(eo) = explain_options {
        req = req.set_explain_options(eo.to_proto());
    }

    let response = req.send().await?;
    let started_tx = (!response.transaction.is_empty()).then(|| response.transaction.clone());
    let explain_metrics = response.explain_metrics.map(ExplainMetrics::from_proto);

    let mut values = BTreeMap::new();
    let mut read_time = None;
    if let Some(batch) = response.batch {
        read_time = batch.read_time;
        for agg_res in batch.aggregation_results {
            for (alias, proto_val) in agg_res.aggregate_properties {
                let val = Value::from_proto(proto_val).map_err(|e| Error::deser(Box::new(e)))?;
                values.insert(alias, val);
            }
        }
    }

    Ok((
        AggregationResults {
            values,
            read_time,
            explain_metrics,
        },
        started_tx,
    ))
}

/// Request builder for [`Datastore::run_aggregation_query`].
#[derive(Clone, Debug)]
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
        let mut partition_id = self.client.partition_id();
        if let Some(ns) = self
            .namespace_id
            .or_else(|| self.query.nested_query.namespace_id.clone())
        {
            partition_id = partition_id.with_namespace_id(ns);
        }
        let proto_agg = self
            .query
            .to_proto(&partition_id)
            .map_err(|e| Error::ser(Box::new(e)))?;
        let read_mode = match self.transaction_id {
            Some(tx_id) => LookupReadMode::Transaction(tx_id),
            None => LookupReadMode::NonTransactional(self.read_options),
        };
        let (results, _) = execute_aggregation_query(
            self.client,
            partition_id,
            InitialAggregationSpec::Structured(Box::new(proto_agg)),
            read_mode,
            self.explain_options,
            self.options,
        )
        .await?;
        Ok(results)
    }
}

/// Request builder for [`Datastore::run_aggregation_gql_query`].
#[derive(Clone, Debug)]
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
        let (results, _) = execute_aggregation_query(
            self.client,
            partition_id,
            InitialAggregationSpec::Gql(proto_gql),
            read_mode,
            self.explain_options,
            self.options,
        )
        .await?;
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generated::gapic_dataplane::stub::Datastore as DatastoreStub;
    use crate::model::{
        AggregationResult, AggregationResultBatch, ExecutionStats as ProtoExecutionStats,
        ExplainMetrics as ProtoExplainMetrics, RunAggregationQueryRequest,
        RunAggregationQueryResponse, Value as ProtoValue,
    };
    use crate::query::Filter;
    use google_cloud_gax::options::RequestOptions;
    use google_cloud_gax::response::Response;

    mockall::mock! {
        #[derive(Debug)]
        DatastoreMock {}
        impl DatastoreStub for DatastoreMock {
            async fn run_aggregation_query(
                &self,
                req: RunAggregationQueryRequest,
                options: RequestOptions,
            ) -> Result<Response<RunAggregationQueryResponse>>;
        }
    }

    fn test_client(mock: MockDatastoreMock) -> Datastore {
        Datastore::from_stub("test-proj", "test-db", "test-ns", mock)
    }

    #[tokio::test]
    async fn structured_aggregation_query_count_sum_avg_and_explain_metrics() {
        let mut mock = MockDatastoreMock::new();
        mock.expect_run_aggregation_query()
            .times(1)
            .withf(|req, _| {
                let Some(aq) = req.aggregation_query() else {
                    return false;
                };
                req.project_id == "test-proj"
                    && req.database_id == "test-db"
                    && aq.aggregations.len() == 4
                    && aq.aggregations[0].alias == "total"
                    && aq.aggregations[1].alias == "capped"
                    && aq.aggregations[1].count().and_then(|c| c.up_to) == Some(100)
                    && aq.aggregations[2].alias == "sum_points"
                    && aq.aggregations[3].alias == "avg_points"
                    && req.explain_options.as_ref().is_some_and(|eo| eo.analyze)
            })
            .returning(|_, _| {
                let agg_res = AggregationResult::new().set_aggregate_properties([
                    ("total", ProtoValue::new().set_integer_value(25)),
                    ("capped", ProtoValue::new().set_integer_value(25)),
                    ("sum_points", ProtoValue::new().set_integer_value(500)),
                    ("avg_points", ProtoValue::new().set_double_value(20.0)),
                ]);
                let batch = AggregationResultBatch::new()
                    .set_aggregation_results([agg_res])
                    .set_read_time(wkt::Timestamp::clamp(1_700_000_000, 0));
                let explain = ProtoExplainMetrics::new().set_execution_stats(
                    ProtoExecutionStats::new()
                        .set_results_returned(1)
                        .set_read_operations(25),
                );
                Ok(Response::from(
                    RunAggregationQueryResponse::new()
                        .set_batch(batch)
                        .set_explain_metrics(explain),
                ))
            });

        let client = test_client(mock);
        let base_query = Query::new("Task").filter(Filter::equal("done", true));
        let agg_query = AggregationQuery::over(base_query)
            .count("total")
            .count_up_to("capped", 100)
            .sum("points", "sum_points")
            .avg("points", "avg_points");

        let results = client
            .run_aggregation_query(agg_query)
            .with_explain_options(ExplainOptions::analyze())
            .send()
            .await
            .expect("run_aggregation_query");

        assert_eq!(results.get_i64("total").unwrap(), 25);
        assert_eq!(results.get_i64("capped").unwrap(), 25);
        assert_eq!(results.get_i64("sum_points").unwrap(), 500);
        assert_eq!(results.get_f64("avg_points").unwrap(), 20.0);
        assert_eq!(results.values().len(), 4);
        assert_eq!(
            results.read_time(),
            Some(&wkt::Timestamp::clamp(1_700_000_000, 0))
        );
        let metrics = results.explain_metrics().expect("explain_metrics");
        assert_eq!(
            metrics.execution_stats.as_ref().unwrap().read_operations,
            25
        );
    }

    #[tokio::test]
    async fn gql_aggregation_query_with_bindings() {
        let mut mock = MockDatastoreMock::new();
        mock.expect_run_aggregation_query()
            .times(1)
            .withf(|req, _| {
                let Some(gql) = req.gql_query() else {
                    return false;
                };
                gql.query_string == "AGGREGATE COUNT(*) AS total OVER (SELECT * FROM Task WHERE status = @status)"
                    && gql.named_bindings.contains_key("status")
            })
            .returning(|_, _| {
                let agg_res = AggregationResult::new()
                    .set_aggregate_properties([("total", ProtoValue::new().set_integer_value(7))]);
                let batch = AggregationResultBatch::new().set_aggregation_results([agg_res]);
                Ok(Response::from(
                    RunAggregationQueryResponse::new().set_batch(batch),
                ))
            });

        let client = test_client(mock);
        let gql = GqlQuery::new(
            "AGGREGATE COUNT(*) AS total OVER (SELECT * FROM Task WHERE status = @status)",
        )
        .bind("status", "open");

        let results = client
            .run_aggregation_gql_query(gql)
            .send()
            .await
            .expect("run_aggregation_gql_query");

        assert_eq!(results.get_i64("total").unwrap(), 7);
    }
}
