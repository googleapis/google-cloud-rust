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

//! Query profiling options (`ExplainOptions`) and metrics (`ExplainMetrics`).

use std::collections::BTreeMap;
use std::time::Duration;

/// Options for requesting query execution planning and profiling metrics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ExplainOptions {
    pub(crate) analyze: bool,
}

impl ExplainOptions {
    /// Plans the query without executing it (`analyze = false`), returning only [`PlanSummary`].
    pub fn plan_only() -> Self {
        Self { analyze: false }
    }

    /// Plans and executes the query (`analyze = true`), returning both [`PlanSummary`] and [`ExecutionStats`].
    pub fn analyze() -> Self {
        Self { analyze: true }
    }

    /// Returns `true` if `analyze` mode is enabled.
    pub fn is_analyze(&self) -> bool {
        self.analyze
    }

    pub(crate) fn to_proto(self) -> crate::model::ExplainOptions {
        crate::model::ExplainOptions::new().set_analyze(self.analyze)
    }
}

/// Summary of the query planner's index selection.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PlanSummary {
    /// The indexes selected by the query planner, each represented as a structured JSON object.
    pub indexes_used: Vec<BTreeMap<String, serde_json::Value>>,
}

/// Runtime execution statistics collected when [`ExplainOptions::analyze`] is enabled.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ExecutionStats {
    /// Total number of results returned (entities or aggregation rows).
    pub results_returned: i64,
    /// Total wall-clock time spent executing the query on the server.
    pub execution_duration: Option<Duration>,
    /// Total number of read operations billed for the query.
    pub read_operations: i64,
    /// Additional debugging statistics returned by the query engine.
    pub debug_stats: BTreeMap<String, serde_json::Value>,
}

/// Query planning and execution profiling metrics returned on the terminal batch of a query.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ExplainMetrics {
    /// The query planner's index summary, if present.
    pub plan_summary: Option<PlanSummary>,
    /// Runtime execution statistics (populated only when [`ExplainOptions::analyze`] was requested).
    pub execution_stats: Option<ExecutionStats>,
}

impl ExplainMetrics {
    pub(crate) fn from_proto(proto: crate::model::ExplainMetrics) -> Self {
        let plan_summary = proto.plan_summary.map(|ps| PlanSummary {
            indexes_used: ps
                .indexes_used
                .into_iter()
                .map(|s| s.into_iter().collect())
                .collect(),
        });
        let execution_stats = proto.execution_stats.map(|es| ExecutionStats {
            results_returned: es.results_returned,
            execution_duration: es.execution_duration.and_then(|d| {
                let secs = u64::try_from(d.seconds()).ok()?;
                let nanos = u32::try_from(d.nanos()).ok()?;
                Some(Duration::new(secs, nanos))
            }),
            read_operations: es.read_operations,
            debug_stats: es
                .debug_stats
                .map(|s| s.into_iter().collect())
                .unwrap_or_default(),
        });
        Self {
            plan_summary,
            execution_stats,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explain_options_and_metrics_conversion() {
        let plan_only = ExplainOptions::plan_only();
        assert!(!plan_only.is_analyze());
        assert!(!plan_only.to_proto().analyze);

        let analyze = ExplainOptions::analyze();
        assert!(analyze.is_analyze());
        assert!(analyze.to_proto().analyze);

        let mut idx = wkt::Struct::new();
        idx.insert(
            "query_scope".to_string(),
            serde_json::Value::String("Collection group".to_string()),
        );
        let mut debug = wkt::Struct::new();
        debug.insert(
            " billing_details".trim().to_string(),
            serde_json::Value::String("ok".to_string()),
        );

        let proto = crate::model::ExplainMetrics::new()
            .set_plan_summary(crate::model::PlanSummary::new().set_indexes_used([idx]))
            .set_execution_stats(
                crate::model::ExecutionStats::new()
                    .set_results_returned(42)
                    .set_read_operations(45)
                    .set_execution_duration(wkt::Duration::clamp(1, 500_000_000))
                    .set_debug_stats(debug),
            );

        let metrics = ExplainMetrics::from_proto(proto);
        let ps = metrics.plan_summary.expect("plan_summary");
        assert_eq!(ps.indexes_used.len(), 1);
        assert_eq!(
            ps.indexes_used[0].get("query_scope"),
            Some(&serde_json::Value::String("Collection group".to_string()))
        );

        let es = metrics.execution_stats.expect("execution_stats");
        assert_eq!(es.results_returned, 42);
        assert_eq!(es.read_operations, 45);
        assert_eq!(es.execution_duration, Some(Duration::new(1, 500_000_000)));
        assert_eq!(
            es.debug_stats.get("billing_details"),
            Some(&serde_json::Value::String("ok".to_string()))
        );
    }
}
