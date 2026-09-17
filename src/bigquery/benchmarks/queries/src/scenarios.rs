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
    pub name: &'static str,
    pub sql: String,
    pub description: &'static str,
}

impl Scenario {
    /// Resolves the query scenario based on the provided CLI arguments.
    pub fn resolve(args: &Args) -> anyhow::Result<Self> {
        let (name, sql, description) = match args.scenario {
            ScenarioName::Synthetic100k => (
                "synthetic-100k",
                synthetic_sql(100_000),
                "Generates 100,000 structured rows in-flight with no external table dependency.",
            ),
            ScenarioName::Synthetic10k => (
                "synthetic-10k",
                synthetic_sql(10_000),
                "Generates 10,000 structured rows in-flight with no external table dependency.",
            ),
            ScenarioName::UsaNamesScan => (
                "usa-names-scan",
                concat!(
                    "SELECT name, state, year, gender, number ",
                    "FROM `bigquery-public-data.usa_names.usa_1910_2013` ",
                    "WHERE year >= 2000 ",
                    "LIMIT 50000"
                )
                .to_string(),
                "Scans and retrieves 50,000 rows from the USA names public dataset.",
            ),
            ScenarioName::UsaNamesAgg => (
                "usa-names-agg",
                concat!(
                    "SELECT state, gender, SUM(number) AS total_count ",
                    "FROM `bigquery-public-data.usa_names.usa_1910_2013` ",
                    "GROUP BY state, gender ",
                    "ORDER BY total_count DESC"
                )
                .to_string(),
                "Aggregates 5.5M rows grouped by state and gender.",
            ),
            ScenarioName::WikipediaAgg => (
                "wikipedia-agg",
                concat!(
                    "SELECT title, SUM(views) AS total_views ",
                    "FROM `bigquery-public-data.samples.wikipedia` ",
                    "WHERE wp_namespace = 0 ",
                    "GROUP BY title ",
                    "ORDER BY total_views DESC ",
                    "LIMIT 1000"
                )
                .to_string(),
                "Aggregates top 1000 article views from Wikipedia public samples.",
            ),
            ScenarioName::Custom => (
                "custom",
                custom_sql(args)?,
                "User-defined custom SQL query.",
            ),
        };

        Ok(Self {
            name,
            sql,
            description,
        })
    }
}

/// Builds a zero-dependency query that generates `rows` structured rows in-flight.
fn synthetic_sql(rows: u64) -> String {
    format!(
        concat!(
            "SELECT ",
            "  x AS row_id, ",
            "  GENERATE_UUID() AS uuid, ",
            "  REPEAT('abcdefghij', 10) AS payload ",
            "FROM UNNEST(GENERATE_ARRAY(1, {})) AS x"
        ),
        rows
    )
}

/// Reads the user-supplied SQL from `--sql` or `--sql-file`.
fn custom_sql(args: &Args) -> anyhow::Result<String> {
    if let Some(sql) = &args.sql {
        return Ok(sql.clone());
    }
    let Some(sql_file) = &args.sql_file else {
        anyhow::bail!("Custom scenario requires --sql or --sql-file");
    };
    fs::read_to_string(sql_file).map_err(|e| {
        anyhow::anyhow!(
            "Failed to read custom SQL file {}: {}",
            sql_file.display(),
            e
        )
    })
}
