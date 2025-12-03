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

//! Benchmark random-reads using the Cloud Storage client library for Rust.

mod args;
#[cfg(google_cloud_unstable_storage_bidi)]
mod bidi;
mod dataset;
mod experiment;
mod json;
mod names;
mod sample;

use anyhow::{Result, bail};
use args::Args;
use clap::Parser;
use google_cloud_auth::credentials::{Builder as CredentialsBuilder, Credentials};
use sample::Sample;
use std::time::Instant;
use tokio::sync::mpsc;

use crate::sample::Protocol;

const DESCRIPTION: &str = concat!(
    "This benchmark repeatedly reads ranges from a set of Cloud Storage objects.",
    " In each iteration of the benchmark the number of concurrent ranges,",
    " the size of the ranges, and the location of the ranges is selected at random.",
    " The API used for the download is also selected at random.",
    " The benchmark runs multiple tasks concurrently, all running identical loops."
);

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    args.validate()?;
    let _guard = enable_tracing(&args);
    tracing::info!("Configuration: {args:?}");

    let credentials = CredentialsBuilder::default().build()?;
    let objects = dataset::populate(&args, credentials.clone()).await?;
    if objects.is_empty() {
        bail!("no objects in the dataset for bucket {}", args.bucket_name);
    }
    let (tx, mut rx) = mpsc::channel(1024 * args.task_count);
    let test_start = Instant::now();
    let tasks = (0..args.task_count)
        .map(|task| {
            tokio::spawn(runner(
                task,
                test_start,
                credentials.clone(),
                tx.clone(),
                args.clone(),
                objects.clone(),
            ))
        })
        .collect::<Vec<_>>();
    drop(tx);
    println!("{}", Sample::HEADER);
    while let Some(sample) = rx.recv().await {
        println!("{}", sample.to_row());
    }

    for (id, t) in tasks.into_iter().enumerate() {
        match t.await {
            Err(e) => tracing::error!("cannot join task {id}: {e}"),
            Ok(Err(e)) => tracing::error!("error in task {id}: {e}"),
            Ok(Ok(_)) => {}
        }
    }
    tracing::info!("DONE");

    Ok(())
}

async fn runner(
    task: usize,
    test_start: Instant,
    credentials: Credentials,
    tx: mpsc::Sender<Sample>,
    args: Args,
    objects: Vec<String>,
) -> anyhow::Result<()> {
    let _guard = enable_tracing(&args);
    if task % 128 == 0 {
        tracing::info!("Task::run({})", task);
    }

    let json = json::Runner::new(credentials.clone()).await?;
    #[cfg(google_cloud_unstable_storage_bidi)]
    let bidi = bidi::Runner::new(&args, objects.clone(), credentials.clone()).await?;

    let generator = experiment::ExperimentGenerator::new(&args, objects)?;
    for iteration in 0..args.iterations {
        let experiment = generator.generate();
        let range_count = experiment.ranges.len();
        let protocol = experiment.protocol;

        let start = Instant::now();
        let attempts = match experiment.protocol {
            Protocol::Json => json.iteration(&experiment).await,
            #[cfg(google_cloud_unstable_storage_bidi)]
            Protocol::Bidi => bidi.iteration(&experiment).await,
        };
        let elapsed = Instant::now() - start;
        let relative_start = start - test_start;

        let samples =
            attempts
                .into_iter()
                .zip(experiment.ranges)
                .enumerate()
                .map(|(i, (result, range))| {
                    let (size, ttfb, ttlb, details) = match result {
                        Ok(a) => (a.size, a.ttfb, a.ttlb, "OK"),
                        Err(e) => {
                            tracing::error!("error on range {i}: {e:?}");
                            (0, elapsed, elapsed, "ERROR")
                        }
                    };
                    Sample {
                        protocol,
                        transfer_size: size,
                        ttfb,
                        ttlb,
                        details: details.to_string(),
                        task,
                        iteration,
                        range_id: i,
                        range_count,
                        start: relative_start,
                        range_offset: range.read_offset,
                        range_length: range.read_length,
                        object: range.object_name,
                    }
                });
        for s in samples {
            let _ = tx.send(s).await;
        }
    }

    Ok(())
}

fn enable_tracing(_args: &Args) -> tracing::dispatcher::DefaultGuard {
    use tracing_subscriber::fmt::format::FmtSpan;

    let subscriber = tracing_subscriber::fmt()
        .with_level(true)
        .with_thread_ids(true)
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .with_writer(std::io::stderr)
        .with_max_level(tracing::Level::INFO)
        .finish();

    tracing::subscriber::set_default(subscriber)
}
