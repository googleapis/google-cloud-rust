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

//! BigQuery query benchmark and endurance test runner.

mod args;
mod metrics;
mod reporter;
mod runner;
mod sample;
mod scenarios;
mod telemetry;

use args::Args;
use clap::Parser;
use google_cloud_auth::credentials::Builder as CredentialsBuilder;
use google_cloud_bigquery::client::BigQuery;
use metrics::OtelMetrics;
use scenarios::Scenario;
use std::collections::BTreeMap;
use std::time::Instant;
use tokio::task::JoinSet;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_log::LogTracer::init()?;
    let args = Args::parse();
    args.validate()?;

    let scenario = Scenario::resolve(&args)?;
    let credentials = CredentialsBuilder::default().build()?;
    let telemetry_guard = telemetry::enable_telemetry(&args, &credentials).await?;

    tracing::info!(
        scenario = %scenario.name,
        description = %scenario.description,
        task_count = args.task_count,
        iterations = ?args.iterations,
        duration = ?args.duration,
        use_query_cache = args.use_query_cache,
        "Starting BigQuery benchmark"
    );

    // Spawn periodic runtime monitor and counter logger
    let handle = tokio::runtime::Handle::current();
    let runtime_monitor = tokio_metrics::RuntimeMonitor::new(&handle);
    let monitor_freq = std::time::Duration::from_secs(5);
    tokio::spawn(async move {
        for metrics in runtime_monitor.intervals() {
            let counters = BTreeMap::from_iter(metrics::get_counters());
            tracing::info!("Counters = {:?} RuntimeMetrics = {:?}", counters, metrics);
            tokio::time::sleep(monitor_freq).await;
        }
    });

    let mut client_builder = BigQuery::builder()
        .with_credentials(credentials.clone())
        .with_tracing();

    if let Some(project_id) = &args.project_id {
        client_builder = client_builder.with_project_id(project_id);
    }

    let client = client_builder.build().await?;
    let otel_metrics = OtelMetrics::new();
    otel_metrics.init_scenario(&scenario.name);

    let channel_capacity = (1024 * args.task_count).max(64);
    let (tx, rx) = tokio::sync::mpsc::channel(channel_capacity);

    // Spawn reporter in background to process samples as they arrive
    let reporter_scenario = scenario.clone();
    let reporter_args = args.clone();
    let reporter_handle = tokio::spawn(async move {
        reporter::collect_and_report(rx, &reporter_scenario, &reporter_args).await
    });

    let test_start = Instant::now();
    let mut tasks = JoinSet::new();

    for task_id in 0..args.task_count {
        let task_client = client.clone();
        let task_scenario = scenario.clone();
        let task_args = args.clone();
        let task_tx = tx.clone();
        let task_metrics = otel_metrics.clone();

        tasks.spawn(async move {
            let runner = runner::TaskRunner {
                task_id,
                test_start,
                client: &task_client,
                scenario: &task_scenario,
                args: &task_args,
                tx: &task_tx,
                metrics: &task_metrics,
            };
            let result = runner.run().await;
            (task_id, result)
        });
    }

    // Drop main sender so receiver terminates after all tasks complete
    drop(tx);

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            tracing::warn!("Ctrl+C received, stopping benchmark tasks and generating report...");
            tasks.abort_all();
            while (tasks.join_next().await).is_some() {}
        }
        _ = async {
            while let Some(res) = tasks.join_next().await {
                match res {
                    Ok((task_id, Ok(_))) => {
                        tracing::debug!(task_id, "Task worker completed successfully");
                    }
                    Ok((task_id, Err(err))) => {
                        tracing::error!(task_id, "Task worker encountered error: {err:?}");
                    }
                    Err(err) => {
                        tracing::error!("Failed to join task: {err:?}");
                    }
                }
            }
        } => {}
    }

    // Wait for reporter to finish outputting summary and files
    match reporter_handle.await {
        Ok(Ok(_)) => {}
        Ok(Err(err)) => tracing::error!("Reporter failed: {err:?}"),
        Err(err) => tracing::error!("Reporter task panicked: {err:?}"),
    }

    let final_counters = BTreeMap::from_iter(metrics::get_counters());
    tracing::info!("Final counters: {:?}", final_counters);

    telemetry_guard.shutdown();

    Ok(())
}
