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

use crate::args::{Args, ScenarioName};
use std::fs;

/// Represents a configured query benchmark scenario.
#[derive(Clone, Debug)]
pub struct Scenario {
    pub name: String,
    pub sql: String,
    pub description: &'static str,
}

impl Scenario {
    /// Resolves the query scenario based on the provided CLI arguments.
    pub fn resolve(args: &Args) -> anyhow::Result<Self> {
        match args.scenario {
            ScenarioName::Synthetic100k => Ok(Self {
                name: "synthetic-100k".to_string(),
                sql: concat!(
                    "SELECT ",
                    "  x AS row_id, ",
                    "  GENERATE_UUID() AS uuid, ",
                    "  REPEAT('abcdefghij', 10) AS payload ",
                    "FROM UNNEST(GENERATE_ARRAY(1, 100000)) AS x"
                )
                .to_string(),
                description: "Generates 100,000 structured rows in-flight with no external table dependency.",
            }),
            ScenarioName::Synthetic10k => Ok(Self {
                name: "synthetic-10k".to_string(),
                sql: concat!(
                    "SELECT ",
                    "  x AS row_id, ",
                    "  GENERATE_UUID() AS uuid, ",
                    "  REPEAT('abcdefghij', 10) AS payload ",
                    "FROM UNNEST(GENERATE_ARRAY(1, 10000)) AS x"
                )
                .to_string(),
                description: "Generates 10,000 structured rows in-flight with no external table dependency.",
            }),
            ScenarioName::UsaNamesScan => Ok(Self {
                name: "usa-names-scan".to_string(),
                sql: concat!(
                    "SELECT name, state, year, gender, number ",
                    "FROM `bigquery-public-data.usa_names.usa_1910_2013` ",
                    "WHERE year >= 2000 ",
                    "LIMIT 50000"
                )
                .to_string(),
                description: "Scans and retrieves 50,000 rows from the USA names public dataset.",
            }),
            ScenarioName::UsaNamesAgg => Ok(Self {
                name: "usa-names-agg".to_string(),
                sql: concat!(
                    "SELECT state, gender, SUM(number) AS total_count ",
                    "FROM `bigquery-public-data.usa_names.usa_1910_2013` ",
                    "GROUP BY state, gender ",
                    "ORDER BY total_count DESC"
                )
                .to_string(),
                description: "Aggregates 5.5M rows grouped by state and gender.",
            }),
            ScenarioName::WikipediaAgg => Ok(Self {
                name: "wikipedia-agg".to_string(),
                sql: concat!(
                    "SELECT title, SUM(views) AS total_views ",
                    "FROM `bigquery-public-data.samples.wikipedia` ",
                    "WHERE wp_namespace = 0 ",
                    "GROUP BY title ",
                    "ORDER BY total_views DESC ",
                    "LIMIT 1000"
                )
                .to_string(),
                description: "Aggregates top 1000 article views from Wikipedia public samples.",
            }),
            ScenarioName::Custom => {
                let sql = if let Some(sql) = &args.sql {
                    sql.clone()
                } else if let Some(sql_file) = &args.sql_file {
                    fs::read_to_string(sql_file).map_err(|e| {
                        anyhow::anyhow!(
                            "Failed to read custom SQL file {}: {}",
                            sql_file.display(),
                            e
                        )
                    })?
                } else {
                    anyhow::bail!("Custom scenario requires --sql or --sql-file");
                };

                Ok(Self {
                    name: "custom".to_string(),
                    sql,
                    description: "User-defined custom SQL query.",
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn test_synthetic_scenarios() {
        let args = Args::parse_from(["bigquery-benchmark-queries", "--scenario", "synthetic-100k"]);
        let s = Scenario::resolve(&args).unwrap();
        assert_eq!(s.name, "synthetic-100k");
        assert!(s.sql.contains("100000"));

        let args = Args::parse_from(["bigquery-benchmark-queries", "--scenario", "synthetic-10k"]);
        let s = Scenario::resolve(&args).unwrap();
        assert_eq!(s.name, "synthetic-10k");
        assert!(s.sql.contains("10000"));
    }

    #[test]
    fn test_custom_scenario_from_string() {
        let args = Args::parse_from([
            "bigquery-benchmark-queries",
            "--scenario",
            "custom",
            "--sql",
            "SELECT 42",
        ]);
        let s = Scenario::resolve(&args).unwrap();
        assert_eq!(s.name, "custom");
        assert_eq!(s.sql, "SELECT 42");
    }
}
