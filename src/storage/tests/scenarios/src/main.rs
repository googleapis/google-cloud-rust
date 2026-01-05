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

//! Test several interesting scenarios for the Cloud Storage client library.

mod args;
mod dataset;
mod names;
mod read_resume_policy;
mod retry_policy;
mod sample;

use args::Args;
use clap::Parser;
use google_cloud_auth::credentials::Builder as CredentialsBuilder;
use google_cloud_gax::options::RequestOptionsBuilder;
use google_cloud_gax::retry_policy::RetryPolicyExt;
use google_cloud_storage::read_resume_policy::Recommended;
use google_cloud_storage::retry_policy::RetryableErrors;
use google_cloud_storage::{
    client::{Storage, StorageControl},
    model::Object,
};
use rand::seq::IndexedRandom;
use sample::Sample;
use sample::Scenario;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::mpsc::Sender;

const DESCRIPTION: &str = concat!(
    "This program repeatedly tests different use-cases for the",
    " Cloud Storage client library.",
    " The goal is to detect error conditions that are unknown at design time."
);

const KIB: usize = 1024;
const MIB: usize = 1024 * KIB;

static SAMPLE_COUNT: AtomicU64 = AtomicU64::new(0);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = Args::parse();
    args.validate()?;
    if args.scenarios.is_empty() {
        args.scenarios = vec![
            Scenario::Json,
            Scenario::Open,
            Scenario::OpenRead,
            Scenario::OpenReadDiscard,
            Scenario::OpenReadAfterDrop,
        ];
    }
    enable_tracing(&args);
    tracing::info!("Configuration: {args:?}");

    let credentials = CredentialsBuilder::default().build()?;
    let builder = Storage::builder()
        .with_credentials(credentials.clone())
        .with_read_resume_policy(read_resume_policy::Instrumented::new(Recommended))
        .with_retry_policy(retry_policy::Instrumented::new(
            RetryableErrors.with_time_limit(Duration::from_secs(300)),
        ));
    let builder = args
        .grpc_subchannel_count
        .iter()
        .fold(builder, |b, v| b.with_grpc_subchannel_count(*v));
    let client = builder.build().await?;

    let objects = dataset::populate(&args, credentials.clone()).await?;
    if objects.is_empty() {
        anyhow::bail!("empty dataset")
    }

    let (tx, mut rx) = tokio::sync::mpsc::channel(1024 * args.task_count);
    let test_start = Instant::now();
    let tasks = (0..args.task_count)
        .map(|task| {
            tokio::spawn(runner(
                task,
                test_start,
                client.clone(),
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

    if !args.use_existing_dataset {
        let control = StorageControl::builder()
            .with_credentials(credentials)
            .build()
            .await?;
        let _delete =
            futures::future::join_all(objects.into_iter().map(|o| remove(&control, o))).await;
    }
    tracing::info!("DONE");

    Ok(())
}

async fn runner(
    task: usize,
    test_start: Instant,
    client: Storage,
    tx: Sender<Sample>,
    args: Args,
    objects: Vec<Object>,
) -> anyhow::Result<()> {
    if task % 128 == 0 {
        tracing::info!("Task::run({})", task);
    }

    for iteration in 0..args.iterations {
        let relative_start = test_start.elapsed();
        let scenario = *args
            .scenarios
            .choose(&mut rand::rng())
            .expect("scenario list is never empty");
        let attempt = scenario.run(&args, &client, &objects).await;
        let details = match attempt.result {
            Ok(_) => "OK".to_string(),
            Err(e) => format!("ERROR={e:?}").replace(",", ";").replace("\n", ";"),
        };
        let sample = Sample {
            task,
            iteration,
            start: relative_start,
            scenario,
            open_latency: attempt.open_latency,
            uploadid: attempt.uploadid,
            object: attempt.object,
            details,
        };
        let _ = tx.send(sample).await;
        if SAMPLE_COUNT.fetch_add(1, Ordering::SeqCst) >= args.iterations {
            break;
        }
    }

    Ok(())
}

fn enable_tracing(_args: &Args) {
    use tracing_subscriber::fmt::format::FmtSpan;

    let subscriber = tracing_subscriber::fmt()
        .with_level(true)
        .with_thread_ids(true)
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .with_writer(std::io::stderr)
        .with_max_level(tracing::Level::INFO)
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("setting global subscriber succeeds");
}

async fn remove(control: &StorageControl, object: Object) {
    if let Err(e) = control
        .delete_object()
        .set_bucket(&object.bucket)
        .set_object(&object.name)
        .set_generation(object.generation)
        .with_idempotency(true)
        .send()
        .await
    {
        tracing::error!(
            "error deleting object {} in bucket {}: {e:?}",
            object.name,
            object.bucket
        );
    }
}
