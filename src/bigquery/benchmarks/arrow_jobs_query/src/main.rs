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

use clap::{Parser, ValueEnum};
use google_cloud_bigquery::FromRow;
use google_cloud_bigquery::client::BigQuery;
use std::time::{Duration, Instant};

#[derive(Parser, Debug)]
#[command(
    name = "bigquery-benchmark-arrow-jobs-query",
    about = "BigQuery jobs.query Benchmark: Arrow results format vs standard JSON"
)]
struct Args {
    /// GCP Project ID (reads from GOOGLE_CLOUD_PROJECT if omitted).
    #[arg(long, env = "GOOGLE_CLOUD_PROJECT")]
    project_id: Option<String>,

    /// Scenario to execute.
    #[arg(long, value_enum, default_value = "synthetic-100k")]
    scenario: Scenario,

    /// Custom query to run when scenario is `custom`.
    #[arg(long)]
    query: Option<String>,

    /// Number of benchmark measurement iterations.
    #[arg(long, default_value_t = 5)]
    iterations: usize,

    /// Number of warmup iterations before measuring.
    #[arg(long, default_value_t = 1)]
    warmup: usize,

    /// Whether to enable BigQuery server-side query cache (default: false).
    #[arg(long, default_value_t = false)]
    use_query_cache: bool,

    /// Whether to deserialize rows into typed structs using FromRow.
    #[arg(long, default_value_t = true)]
    typed: bool,
}

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
enum Scenario {
    #[value(name = "synthetic-1")]
    Synthetic1,
    #[value(name = "synthetic-100")]
    Synthetic100,
    #[value(name = "synthetic-1k")]
    Synthetic1k,
    #[value(name = "synthetic-10k")]
    Synthetic10k,
    #[value(name = "synthetic-50k")]
    Synthetic50k,
    #[value(name = "synthetic-100k")]
    Synthetic100k,
    #[value(name = "synthetic-500k")]
    Synthetic500k,
    #[value(name = "wikipedia-10k")]
    Wikipedia10k,
    #[value(name = "wikipedia-100k")]
    Wikipedia100k,
    #[value(name = "custom")]
    Custom,
}

impl Scenario {
    fn query(&self, custom_query: Option<&str>) -> String {
        match self {
            Scenario::Synthetic1 => Self::synthetic_query(1),
            Scenario::Synthetic100 => Self::synthetic_query(100),
            Scenario::Synthetic1k => Self::synthetic_query(1_000),
            Scenario::Synthetic10k => Self::synthetic_query(10_000),
            Scenario::Synthetic50k => Self::synthetic_query(50_000),
            Scenario::Synthetic100k => Self::synthetic_query(100_000),
            Scenario::Synthetic500k => Self::synthetic_query(500_000),
            Scenario::Wikipedia10k => Self::wikipedia_query(10_000),
            Scenario::Wikipedia100k => Self::wikipedia_query(100_000),
            Scenario::Custom => custom_query
                .expect("custom query must be provided when scenario is 'custom'")
                .to_string(),
        }
    }

    fn synthetic_query(row_count: usize) -> String {
        format!(
            "SELECT \
                x AS id, \
                CONCAT('row_item_name_', CAST(x AS STRING)) AS name, \
                CAST(x AS FLOAT64) * 1.25 AS score, \
                (MOD(x, 2) = 0) AS is_even, \
                CURRENT_TIMESTAMP() AS created_at \
             FROM UNNEST(GENERATE_ARRAY(1, {row_count})) AS x"
        )
    }

    fn wikipedia_query(limit: usize) -> String {
        format!(
            "SELECT \
                title, \
                id, \
                language, \
                wp_namespace, \
                is_redirect, \
                revision_id, \
                timestamp, \
                contributor_ip, \
                contributor_id, \
                contributor_username, \
                comment, \
                num_characters \
             FROM `bigquery-public-data.samples.wikipedia` \
             LIMIT {limit}"
        )
    }

    fn is_wikipedia(&self) -> bool {
        matches!(self, Scenario::Wikipedia10k | Scenario::Wikipedia100k)
    }
}

#[derive(FromRow, Debug, PartialEq)]
#[allow(dead_code)]
struct SyntheticRow {
    id: i64,
    name: String,
    score: f64,
    is_even: bool,
    created_at: wkt::Timestamp,
}

#[derive(FromRow, Debug, PartialEq)]
#[allow(dead_code)]
struct WikipediaRow {
    title: Option<String>,
    id: Option<i64>,
    language: Option<String>,
    wp_namespace: Option<i64>,
    is_redirect: Option<bool>,
    revision_id: Option<i64>,
    timestamp: Option<i64>,
    contributor_ip: Option<String>,
    contributor_id: Option<i64>,
    contributor_username: Option<String>,
    comment: Option<String>,
    num_characters: Option<i64>,
}

struct IterationResult {
    query_duration: Duration,
    read_duration: Duration,
    total_duration: Duration,
    rows_count: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let project_id = args.project_id.ok_or_else(|| {
        anyhow::anyhow!(
            "Project ID must be provided via --project-id or GOOGLE_CLOUD_PROJECT env var"
        )
    })?;

    let sql_query = args.scenario.query(args.query.as_deref());

    println!("================================================================================");
    println!("                    BigQuery Query Benchmark");
    println!("================================================================================");
    #[cfg(google_cloud_unstable_bigquery_arrow)]
    println!("  Arrow Acceleration:  ENABLED (--cfg google_cloud_unstable_bigquery_arrow)");
    #[cfg(not(google_cloud_unstable_bigquery_arrow))]
    println!("  Arrow Acceleration:  DISABLED (Standard JSON mode)");
    println!("  Project ID:          {project_id}");
    println!("  Scenario:            {:?}", args.scenario);
    println!("  Warmup Iterations:   {}", args.warmup);
    println!("  Measured Runs:       {}", args.iterations);
    println!("  Use Query Cache:     {}", args.use_query_cache);
    println!("  Typed Deserialization: {}", args.typed);
    println!("================================================================================");
    println!();

    let client = BigQuery::builder()
        .with_project_id(&project_id)
        .build()
        .await?;

    // Warmup runs
    if args.warmup > 0 {
        println!("Running {} warmup iteration(s)...", args.warmup);
        for i in 1..=args.warmup {
            print!("  Warmup {i}/{}: ", args.warmup);
            let result = run_single_query(
                &client,
                &project_id,
                &sql_query,
                args.scenario,
                args.use_query_cache,
                args.typed,
            )
            .await?;
            println!(
                "done ({} rows, query: {:.2?}, read: {:.2?}, total: {:.2?})",
                result.rows_count,
                result.query_duration,
                result.read_duration,
                result.total_duration
            );
        }
        println!();
    }

    // Benchmark measured runs
    println!("Running {} benchmark measurement(s)...", args.iterations);
    println!("--------------------------------------------------------------------------------");
    println!(
        "{:<6} | {:<12} | {:<12} | {:<12} | {:<10} | {:<14}",
        "Run", "Query Time", "Read/Iter", "Total Time", "Rows", "Throughput"
    );
    println!("--------------------------------------------------------------------------------");

    let mut results = Vec::with_capacity(args.iterations);
    for i in 1..=args.iterations {
        let result = run_single_query(
            &client,
            &project_id,
            &sql_query,
            args.scenario,
            args.use_query_cache,
            args.typed,
        )
        .await?;
        let rps = if result.read_duration.as_secs_f64() > 0.0 {
            result.rows_count as f64 / result.read_duration.as_secs_f64()
        } else {
            0.0
        };

        println!(
            "{:<6} | {:<12.2?} | {:<12.2?} | {:<12.2?} | {:<10} | {:>10.0} rows/s",
            format!("#{i}"),
            result.query_duration,
            result.read_duration,
            result.total_duration,
            result.rows_count,
            rps
        );
        results.push(result);
    }
    println!("--------------------------------------------------------------------------------");

    // Print summary statistics
    print_summary(&results);

    println!();
    println!("Tip: Compare Arrow vs JSON by running:");
    println!(
        "  Arrow:  RUSTFLAGS=\"--cfg google_cloud_unstable_bigquery_arrow\" cargo run --release -p bigquery-benchmark-arrow-jobs-query -- --scenario {:?}",
        args.scenario
    );
    println!(
        "  JSON:   cargo run --release -p bigquery-benchmark-arrow-jobs-query -- --scenario {:?}",
        args.scenario
    );
    println!();

    Ok(())
}

async fn run_single_query(
    client: &BigQuery,
    project_id: &str,
    query_str: &str,
    scenario: Scenario,
    use_query_cache: bool,
    typed: bool,
) -> anyhow::Result<IterationResult> {
    let start_total = Instant::now();

    // 1. Submit query and wait until complete
    let start_query = Instant::now();
    let complete_query = client
        .query(query_str)
        .with_project_id(project_id)
        .set_use_query_cache(use_query_cache)
        .until_done()
        .await?;
    let query_duration = start_query.elapsed();

    // 2. Read and deserialize rows
    let start_read = Instant::now();
    let mut iter = complete_query.read();
    let mut rows_count = 0;

    if typed {
        if scenario.is_wikipedia() {
            while let Some(row_res) = iter.next().await {
                let row = row_res?;
                let _typed_row: WikipediaRow = row.try_into()?;
                rows_count += 1;
            }
        } else {
            while let Some(row_res) = iter.next().await {
                let row = row_res?;
                let _typed_row: SyntheticRow = row.try_into()?;
                rows_count += 1;
            }
        }
    } else {
        while let Some(row_res) = iter.next().await {
            let _row = row_res?;
            rows_count += 1;
        }
    }
    let read_duration = start_read.elapsed();
    let total_duration = start_total.elapsed();

    Ok(IterationResult {
        query_duration,
        read_duration,
        total_duration,
        rows_count,
    })
}

fn print_summary(results: &[IterationResult]) {
    if results.is_empty() {
        return;
    }

    let n = results.len() as f64;
    let query_times: Vec<f64> = results
        .iter()
        .map(|r| r.query_duration.as_secs_f64())
        .collect();
    let read_times: Vec<f64> = results
        .iter()
        .map(|r| r.read_duration.as_secs_f64())
        .collect();
    let total_times: Vec<f64> = results
        .iter()
        .map(|r| r.total_duration.as_secs_f64())
        .collect();
    let throughputs: Vec<f64> = results
        .iter()
        .map(|r| {
            if r.read_duration.as_secs_f64() > 0.0 {
                r.rows_count as f64 / r.read_duration.as_secs_f64()
            } else {
                0.0
            }
        })
        .collect();

    let avg = |v: &[f64]| v.iter().sum::<f64>() / n;
    let min = |v: &[f64]| v.iter().cloned().fold(f64::INFINITY, f64::min);
    let max = |v: &[f64]| v.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    let std_dev = |v: &[f64], mean: f64| {
        let variance = v.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n;
        variance.sqrt()
    };

    let q_avg = avg(&query_times);
    let r_avg = avg(&read_times);
    let t_avg = avg(&total_times);
    let tp_avg = avg(&throughputs);

    println!("Summary Statistics (over {} runs):", results.len());
    println!(
        "  Query Execution Time:   avg: {:.2?} (min: {:.2?}, max: {:.2?}, stddev: {:.2?})",
        Duration::from_secs_f64(q_avg),
        Duration::from_secs_f64(min(&query_times)),
        Duration::from_secs_f64(max(&query_times)),
        Duration::from_secs_f64(std_dev(&query_times, q_avg))
    );
    println!(
        "  Row Reading & Parsing:  avg: {:.2?} (min: {:.2?}, max: {:.2?}, stddev: {:.2?})",
        Duration::from_secs_f64(r_avg),
        Duration::from_secs_f64(min(&read_times)),
        Duration::from_secs_f64(max(&read_times)),
        Duration::from_secs_f64(std_dev(&read_times, r_avg))
    );
    println!(
        "  Total End-to-End Time:  avg: {:.2?} (min: {:.2?}, max: {:.2?}, stddev: {:.2?})",
        Duration::from_secs_f64(t_avg),
        Duration::from_secs_f64(min(&total_times)),
        Duration::from_secs_f64(max(&total_times)),
        Duration::from_secs_f64(std_dev(&total_times, t_avg))
    );
    println!(
        "  Row Throughput:         avg: {:.0} rows/s (min: {:.0}, max: {:.0})",
        tp_avg,
        min(&throughputs),
        max(&throughputs)
    );
}
