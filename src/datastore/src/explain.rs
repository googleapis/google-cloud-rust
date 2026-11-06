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
