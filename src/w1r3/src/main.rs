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

//! An implementation of the W1R3 benchmark for Rust.
//!
//! The W1R3 benchmark repeatedly uploads an object, then downloads the object
//! 3 times, and then deletes the object. In each iteration of the benchmark the
//! size and name of the object is selected at random. The benchmark runs
//! multiple tasks concurrently.

use clap::Parser;
use google_cloud_storage::client::{Storage, StorageControl};
use rand::{
    Rng,
    distr::{Alphanumeric, Uniform},
};
use std::time::{Duration, Instant};
use tokio::sync::mpsc::Sender;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _guard = enable_tracing();

    let args = Args::parse();
    tracing::info!("{args:?}");

    let client = Storage::builder().build().await?;
    let control = StorageControl::builder().build().await?;

    let (tx, mut rx) = tokio::sync::mpsc::channel(128);
    let test_start = Instant::now();
    let tasks = (0..args.task_count)
        .map(|task| {
            tokio::spawn(runner(
                task,
                client.clone(),
                control.clone(),
                test_start,
                args.clone(),
                tx.clone(),
            ))
        })
        .collect::<Vec<_>>();
    drop(tx);

    println!("{}", Sample::HEADER);
    let mut sample_count = 0_usize;
    while let Some(sample) = rx.recv().await {
        println!("{}", sample.to_row());
        sample_count += 1;
    }
    tracing::info!("Benchmark collected {sample_count} samples");

    for t in tasks {
        t.await??;
    }
    Ok(())
}

async fn runner(
    task: usize,
    client: Storage,
    control: StorageControl,
    test_start: Instant,
    args: Args,
    tx: Sender<Sample>,
) -> anyhow::Result<()> {
    let _guard = enable_tracing();
    let buffer = bytes::Bytes::from_owner(
        rand::rng()
            .sample_iter(Uniform::new_inclusive(u8::MIN, u8::MAX)?)
            .take(args.max_object_size as usize)
            .collect::<Vec<_>>(),
    );
    let size = Uniform::new_inclusive(args.min_object_size, args.max_object_size)?;

    for iteration in 0..args.min_sample_count {
        let size = rand::rng().sample(size) as usize;
        let name = random_object_name();
        let (write_op, threshold) = if rand::rng().random_bool(0.5) {
            (Operation::Resumable, 0_usize)
        } else {
            (Operation::SingleShot, size)
        };

        let write_start = Instant::now();
        let upload = client
            .write_object(
                format!("projects/_/buckets/{}", &args.bucket_name),
                &name,
                buffer.slice(0..size),
            )
            .with_resumable_upload_threshold(threshold)
            .send_unbuffered()
            .await;
        let upload = match upload {
            Ok(u) => u,
            Err(e) => {
                tracing::error!("# Error in upload {e}");
                continue;
            }
        };
        tx.send(Sample {
            task,
            iteration,
            size,
            transfer_size: size,
            op: write_op,
            op_start: write_start - test_start,
            elapsed: Instant::now() - write_start,
            object: upload.name.clone(),
            result: ExperimentResult::Success,
            details: "".to_string(),
        })
        .await?;
        for op in [Operation::Read0, Operation::Read1, Operation::Read2] {
            let read_start = Instant::now();
            let mut read = client
                .read_object(&upload.bucket, &upload.name)
                .set_generation(upload.generation)
                .send()
                .await?;
            let mut transfer_size = 0;
            while let Some(b) = read.next().await {
                if let Ok(b) = b {
                    transfer_size += b.len();
                }
            }
            tx.send(Sample {
                task,
                iteration,
                size,
                transfer_size,
                op,
                op_start: read_start - test_start,
                elapsed: Instant::now() - read_start,
                object: upload.name.clone(),
                result: if transfer_size == size {
                    ExperimentResult::Success
                } else {
                    ExperimentResult::Error
                },
                details: "".to_string(),
            })
            .await?;
        }
        let _ = control
            .delete_object()
            .set_bucket(upload.bucket)
            .set_object(upload.name)
            .set_generation(upload.generation)
            .send()
            .await;
    }
    Ok(())
}

fn random_object_name() -> String {
    rand::rng()
        .sample_iter(&Alphanumeric)
        .take(32)
        .map(char::from)
        .collect()
}

#[derive(Clone, Debug)]
struct Sample {
    task: usize,
    iteration: u64,
    op_start: Duration,
    op: Operation,
    size: usize,
    transfer_size: usize,
    elapsed: Duration,
    object: String,
    result: ExperimentResult,
    details: String,
}

impl Sample {
    const HEADER: &str = concat!(
        "Task,Iteration,IterationStart,Operation",
        ",Size,TransferSize,ElapsedMicroseconds,Object",
        ",Result,Details"
    );
    fn to_row(&self) -> String {
        format!(
            "{},{},{},{},{},{},{},{},{},{}",
            self.task,
            self.iteration,
            self.op_start.as_micros(),
            self.op.name(),
            self.size,
            self.transfer_size,
            self.elapsed.as_micros(),
            self.object,
            self.result.name(),
            self.details,
        )
    }
}

#[derive(Clone, Debug)]
enum Operation {
    Resumable,
    SingleShot,
    Read0,
    Read1,
    Read2,
}

impl Operation {
    fn name(&self) -> &str {
        match self {
            Self::Resumable => "RESUMABLE",
            Self::SingleShot => "SINGLE_SHOT",
            Self::Read0 => "READ[0]",
            Self::Read1 => "READ[1]",
            Self::Read2 => "READ[2]",
        }
    }
}

#[derive(Clone, Debug)]
enum ExperimentResult {
    Success,
    Error,
}

impl ExperimentResult {
    fn name(&self) -> &str {
        match self {
            Self::Success => "OK",
            Self::Error => "ERR",
        }
    }
}

fn enable_tracing() -> tracing::dispatcher::DefaultGuard {
    use tracing_subscriber::fmt::format::FmtSpan;
    let subscriber = tracing_subscriber::fmt()
        .with_level(true)
        .with_thread_ids(true)
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .with_writer(std::io::stderr)
        .finish();

    tracing::subscriber::set_default(subscriber)
}

#[derive(Clone, Debug, Parser)]
#[command(version, about)]
struct Args {
    #[arg(long)]
    bucket_name: String,

    #[arg(long, default_value_t = 0, value_parser = parse_size_arg)]
    min_object_size: u64,
    #[arg(long, default_value_t = 0, value_parser = parse_size_arg)]
    max_object_size: u64,

    #[arg(long, default_value_t = 1)]
    task_count: usize,

    #[arg(long, default_value_t = 1)]
    min_sample_count: u64,
}

fn parse_size_arg(arg: &str) -> anyhow::Result<u64> {
    let value = parse_size::parse_size(arg)?;
    Ok(value)
}
