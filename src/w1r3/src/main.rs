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

const DESCRIPTION: &str = concat!(
    "The W1R3 benchmark repeatedly uploads an object, then downloads the object",
    " 3 times. In each iteration of the benchmark the size and name of the",
    " object is selected at random.",
    " The type of upload (resumable vs. single-shot) is also selected at random.",
    " Every few iterations, the tasks delete a batch of objects. The size of the",
    " batch is selected at random, from a range specified in the commend line.",
    " The benchmark runs multiple tasks concurrently, all running identical loops."
);

use clap::Parser;
use google_cloud_auth::credentials::{Builder as CredentialsBuilder, Credentials};
use google_cloud_gax::options::RequestOptionsBuilder;
use google_cloud_gax::retry_policy::RetryPolicyExt;
use google_cloud_storage::ReadObjectResponse;
use google_cloud_storage::Result as StorageResult;
use google_cloud_storage::client::{Storage, StorageControl};
use google_cloud_storage::model::Object;
use google_cloud_storage::retry_policy::RetryableErrors;
use rand::{
    Rng,
    distr::{Alphanumeric, Uniform},
};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::mpsc::Sender;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _guard = enable_tracing();

    let args = Args::parse();
    if args.min_object_size > args.max_object_size {
        return Err(anyhow::Error::msg("invalid object size range"));
    }
    if args.min_delete_batch > args.max_delete_batch {
        return Err(anyhow::Error::msg("invalid delete batch size range"));
    }
    tracing::info!("{args:?}");

    let handle = tokio::runtime::Handle::current();
    let runtime_monitor = tokio_metrics::RuntimeMonitor::new(&handle);
    let frequency = std::time::Duration::from_millis(5000);
    tokio::spawn(async move {
        for metrics in runtime_monitor.intervals() {
            let counters = BTreeMap::from_iter(counters());
            tracing::info!("Counters = {:?} Metrics = {:?}", counters, metrics);
            tokio::time::sleep(frequency).await;
        }
    });

    let credentials = CredentialsBuilder::default().build()?;
    let client = Storage::builder()
        .with_credentials(credentials.clone())
        .build()
        .await?;

    // Use random data for the uploads. We could use a buffer full of zeroes,
    // but that compresses too well and may introduce artificially good results.
    tracing::info!("generating random data");
    let buffer = bytes::Bytes::from_owner(
        rand::rng()
            .sample_iter(Uniform::new_inclusive(u8::MIN, u8::MAX)?)
            .take(args.max_object_size as usize)
            .collect::<Vec<_>>(),
    );
    tracing::info!("random data ready");
    let (tx, mut rx) = tokio::sync::mpsc::channel(128);
    let test_start = Instant::now();
    let tasks = (0..args.task_count)
        .map(|task| {
            tokio::spawn(runner(
                task,
                client.clone(),
                credentials.clone(),
                buffer.clone(),
                test_start,
                args.clone(),
                tx.clone(),
            ))
        })
        .collect::<Vec<_>>();
    drop(tx);

    println!("{}", Sample::HEADER);
    while let Some(sample) = rx.recv().await {
        println!("{}", sample.to_row());
        sample_done();
    }
    let counters = BTreeMap::from_iter(counters());
    tracing::info!("Counters = {counters:?}");

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

#[derive(Clone)]
struct Task {
    start: Instant,
    id: usize,
    tx: Sender<Sample>,
}

async fn runner(
    id: usize,
    client: Storage,
    credentials: Credentials,
    buffer: bytes::Bytes,
    test_start: Instant,
    args: Args,
    tx: Sender<Sample>,
) -> anyhow::Result<()> {
    let _guard = enable_tracing();
    let control = StorageControl::builder()
        .with_credentials(credentials)
        .with_retry_policy(RetryableErrors.with_time_limit(Duration::from_secs(args.retry_seconds)))
        .with_backoff_policy(google_cloud_storage::backoff_policy::default())
        .build()
        .await?;
    let task = Task {
        id,
        start: test_start,
        tx,
    };

    let size_gen = Uniform::new_inclusive(args.min_object_size, args.max_object_size)?;
    let batch_size_gen =
        Uniform::new_inclusive(args.min_delete_batch, args.max_delete_batch).unwrap();

    let mut batch_size = rand::rng().sample(batch_size_gen);
    let mut deletes = Vec::new();
    for iteration in 0..args.iterations {
        let size = rand::rng().sample(size_gen) as usize;
        let name = random_object_name();
        let (write_op, threshold) = if rand::rng().random_bool(0.5) {
            (Operation::Resumable, 0_usize)
        } else {
            (Operation::SingleShot, size)
        };

        let builder = SampleBuilder::new(&task, iteration, write_op, size, name.clone());
        let upload = match upload(&client, &args, &name, buffer.slice(0..size), threshold).await {
            Ok(u) => {
                let _ = task.tx.send(builder.success()).await;
                write_done();
                u
            }
            Err(e) => {
                let _ = task.tx.send(builder.error(&e)).await;
                write_done();
                write_error();
                continue;
            }
        };
        for op in [Operation::Read0, Operation::Read1, Operation::Read2] {
            let builder = SampleBuilder::new(&task, iteration, op, size, upload.name.clone());
            let sample = match download(&client, &args, &upload).await {
                (_, Ok(_)) => builder.success(),
                (0, Err(e)) => builder.error(&e),
                (_partial, Err(e)) => builder.error(&e),
            };
            let _ = task.tx.send(sample).await;
        }
        deletes.push(delete(&control, &args, upload));
        if deletes.len() >= batch_size {
            batch_size = rand::rng().sample(batch_size_gen);
            batch_delete(
                &task,
                iteration,
                deletes.len(),
                deletes.drain(..),
                name.as_str(),
            )
            .await;
        }
    }
    batch_delete(
        &task,
        args.iterations,
        deletes.len(),
        deletes.into_iter(),
        "N/A",
    )
    .await;
    Ok(())
}

async fn upload(
    client: &Storage,
    args: &Args,
    name: &str,
    buffer: bytes::Bytes,
    threshold: usize,
) -> StorageResult<Object> {
    let duration = Duration::from_secs(args.retry_seconds);

    let future = client
        .write_object(
            format!("projects/_/buckets/{}", &args.bucket_name),
            name,
            buffer,
        )
        .set_if_generation_match(0)
        .with_resumable_upload_threshold(threshold)
        .send_unbuffered();

    match tokio::time::timeout(duration, future).await {
        Err(e) => Err(google_cloud_storage::Error::timeout(e)),
        Ok(r) => r,
    }
}

async fn download(
    client: &Storage,
    args: &Args,
    object: &google_cloud_storage::model::Object,
) -> (usize, StorageResult<()>) {
    let duration = Duration::from_secs(args.retry_seconds);

    let read = client
        .read_object(&object.bucket, &object.name)
        .set_generation(object.generation)
        .send();

    let mut read = match tokio::time::timeout(duration, read).await {
        Err(e) => {
            read_done();
            read_error();
            return (0, Err(google_cloud_gax::error::Error::timeout(e)));
        }
        Ok(Ok(r)) => r,
        Ok(Err(e)) => {
            read_done();
            read_error();
            return (0, Err(e));
        }
    };
    read_done();
    let duration = Duration::from_secs(args.retry_seconds);
    let mut transfer_size = 0;
    let mut read_data = async move || {
        while let Some(result) = read.next().await {
            match result {
                Ok(b) => transfer_size += b.len(),
                Err(e) => {
                    read_error();
                    return Err(e);
                }
            }
        }
        Ok(())
    };

    match tokio::time::timeout(duration, read_data()).await {
        Err(e) => (transfer_size, Err(google_cloud_storage::Error::timeout(e))),
        Ok(r) => (transfer_size, r),
    }
}

async fn batch_delete<I, F>(task: &Task, iteration: u64, size: usize, pending: I, name: &str)
where
    I: Iterator<Item = F>,
    F: Future<Output = StorageResult<()>>,
{
    let builder = SampleBuilder::new(task, iteration, Operation::Delete, size, name.to_string());
    let done = futures::future::join_all(pending)
        .await
        .into_iter()
        .collect::<StorageResult<Vec<_>>>();
    delete_done();
    match done {
        Ok(_) => {
            let _ = task.tx.send(builder.success()).await;
        }
        Err(e) => {
            tracing::error!("delete error: {e:?}");
            delete_error();
            let _ = task.tx.send(builder.error(&e)).await;
        }
    }
}

async fn delete(control: &StorageControl, args: &Args, object: Object) -> StorageResult<()> {
    use google_cloud_gax::error::rpc::Code;

    let result = control
        .delete_object()
        .set_bucket(object.bucket)
        .set_object(object.name)
        .set_generation(object.generation)
        .with_attempt_timeout(Duration::from_secs(args.attempt_seconds))
        .with_idempotency(true)
        .send()
        .await;
    if let Err(e) = result {
        // Ignore NotFound errors as they may be the result of a retry.
        if e.status().is_some_and(|s| s.code == Code::NotFound) {
            return Ok(());
        }
        return Err(e);
    };
    Ok(())
}

#[derive(Clone, Debug)]
struct SampleBuilder {
    task: usize,
    relative: Duration,
    iteration: u64,
    start: Instant,
    op: Operation,
    target_size: usize,
    object: String,
}

impl SampleBuilder {
    fn new(task: &Task, iteration: u64, op: Operation, target_size: usize, object: String) -> Self {
        Self {
            task: task.id,
            relative: Instant::now() - task.start,
            start: Instant::now(),
            iteration,
            op,
            target_size,
            object,
        }
    }

    fn error(self, error: &google_cloud_storage::Error) -> Sample {
        tracing::error!(
            "{} sample_builder = {self:?} error = {error:?}",
            self.op.name()
        );
        let details = counters()
            .map(|(name, value)| format!("{name}={value}"))
            .chain([format!("error={error:?}")])
            .collect::<Vec<_>>()
            .join(";");
        Sample {
            task: self.task,
            iteration: self.iteration,
            op_start: self.relative,
            size: self.target_size,
            transfer_size: 0,
            op: self.op,
            elapsed: Instant::now() - self.start,
            object: self.object,
            result: ExperimentResult::Error,
            details,
        }
    }

    fn success(self) -> Sample {
        Sample {
            task: self.task,
            iteration: self.iteration,
            op_start: self.relative,
            size: self.target_size,
            transfer_size: self.target_size,
            op: self.op,
            elapsed: Instant::now() - self.start,
            object: self.object.to_string(),
            result: ExperimentResult::Success,
            details: String::new(),
        }
    }
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
    Delete,
}

impl Operation {
    fn name(&self) -> &str {
        match self {
            Self::Resumable => "RESUMABLE",
            Self::SingleShot => "SINGLE_SHOT",
            Self::Read0 => "READ[0]",
            Self::Read1 => "READ[1]",
            Self::Read2 => "READ[2]",
            Self::Delete => "DELETE",
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

static DELETE_COUNT: AtomicU64 = AtomicU64::new(0);
static READ_COUNT: AtomicU64 = AtomicU64::new(0);
static SAMPLE_COUNT: AtomicU64 = AtomicU64::new(0);
static WRITE_COUNT: AtomicU64 = AtomicU64::new(0);
static DELETE_ERROR: AtomicU64 = AtomicU64::new(0);
static READ_ERROR: AtomicU64 = AtomicU64::new(0);
static WRITE_ERROR: AtomicU64 = AtomicU64::new(0);

#[inline]
fn delete_done() {
    DELETE_COUNT.fetch_add(1, Ordering::SeqCst);
}

#[inline]
fn read_done() {
    READ_COUNT.fetch_add(1, Ordering::SeqCst);
}

#[inline]
fn sample_done() {
    SAMPLE_COUNT.fetch_add(1, Ordering::SeqCst);
}

#[inline]
fn write_done() {
    WRITE_COUNT.fetch_add(1, Ordering::SeqCst);
}

#[inline]
fn delete_error() {
    DELETE_ERROR.fetch_add(1, Ordering::SeqCst);
}

#[inline]
fn read_error() {
    READ_ERROR.fetch_add(1, Ordering::SeqCst);
}

#[inline]
fn write_error() {
    WRITE_ERROR.fetch_add(1, Ordering::SeqCst);
}

fn counters() -> impl Iterator<Item = (&'static str, u64)> {
    [
        ("SAMPLE_COUNT", SAMPLE_COUNT.load(Ordering::SeqCst)),
        ("DELETE_COUNT", DELETE_COUNT.load(Ordering::SeqCst)),
        ("READ_COUNT", READ_COUNT.load(Ordering::SeqCst)),
        ("WRITE_COUNT", WRITE_COUNT.load(Ordering::SeqCst)),
        ("DELETE_ERROR", DELETE_ERROR.load(Ordering::SeqCst)),
        ("READ_ERROR", READ_ERROR.load(Ordering::Relaxed)),
        ("WRITE_ERROR", WRITE_ERROR.load(Ordering::Relaxed)),
    ]
    .into_iter()
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

/// Runs the W1R3 benchmark for the Rust client library.
#[derive(Clone, Debug, Parser)]
#[command(version, about, long_about = DESCRIPTION)]
struct Args {
    /// The name of the bucket used by the benchmark.
    ///
    /// You should use a regional bucket in the same region as the VM running
    /// the benchmark.
    #[arg(long)]
    bucket_name: String,

    /// The minimum object size.
    ///
    /// See `--maximum-object-size` for more details.
    #[arg(long, default_value_t = 0, value_parser = parse_size_arg)]
    min_object_size: u64,

    /// The maximum object size.
    ///
    /// In each iteration, the benchmark picks a size at random between
    /// `--minimum-object-size` and `--maximum-object-size`, both inclusive. The
    /// benchmark uploads an object of that size and then reads it back
    #[arg(long, default_value_t = 0, value_parser = parse_size_arg)]
    max_object_size: u64,

    /// The number of concurrent tasks running the benchmark.
    #[arg(long, default_value_t = 1)]
    task_count: usize,

    /// The number of iterations for each task.
    #[arg(long, default_value_t = 1)]
    iterations: u64,

    /// The minimum size for the delete batch.
    #[arg(long, default_value_t = 20)]
    min_delete_batch: usize,

    /// The maximum size for the delete batch.
    #[arg(long, default_value_t = 20)]
    max_delete_batch: usize,

    /// The maximum time for the retry loop.
    #[arg(long, default_value_t = 900)]
    retry_seconds: u64,

    /// The maximum time for each attempt.
    #[arg(long, default_value_t = 30)]
    attempt_seconds: u64,
}

fn parse_size_arg(arg: &str) -> anyhow::Result<u64> {
    let value = parse_size::parse_size(arg)?;
    Ok(value)
}
