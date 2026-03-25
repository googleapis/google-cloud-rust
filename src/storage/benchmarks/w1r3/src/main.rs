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

mod instrumented_future;
mod instrumented_retry;

use clap::Parser;
use google_cloud_auth::credentials::{Builder as CredentialsBuilder, Credentials};
use google_cloud_gax::error::rpc::Code;
use google_cloud_gax::options::RequestOptionsBuilder;
use google_cloud_gax::retry_policy::{RetryPolicy, RetryPolicyExt};
use google_cloud_storage::Result as StorageResult;
use google_cloud_storage::client::{Storage, StorageControl};
use google_cloud_storage::model::Object;
use google_cloud_storage::retry_policy::RetryableErrors;
use humantime::parse_duration;
use instrumented_future::Instrumented;
use instrumented_retry::DebugRetry;
use opentelemetry_sdk::trace::{SdkTracerProvider, TraceError};
use rand::{
    RngExt,
    distr::{Alphanumeric, Uniform},
};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::mpsc::Sender;
use tokio::task::JoinSet;
use tracing::Instrument;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(900);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    if args.min_object_size > args.max_object_size {
        return Err(anyhow::Error::msg("invalid object size range"));
    }
    if args.min_delete_batch > args.max_delete_batch {
        return Err(anyhow::Error::msg("invalid delete batch size range"));
    }
    tracing_log::LogTracer::init()?;
    let credentials = CredentialsBuilder::default().build()?;
    let tracer_provider = enable_tracing(&args, &credentials).await?;
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
    let client = Storage::builder()
        .with_credentials(credentials.clone())
        .with_tracing()
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

    let (tx, mut rx) = tokio::sync::mpsc::channel(1024 * args.task_count);
    let test_start = Instant::now();
    let mut tasks = JoinSet::new();
    for task in 0..args.task_count {
        let span = tracing::info_span!("w1r3.task", task_id = task);
        let client = client.clone();
        let credentials = credentials.clone();
        let buffer = buffer.clone();
        let tx = tx.clone();
        let args = args.clone();
        tasks.spawn(async move {
            let result = runner(task, test_start, client, credentials, buffer, tx, args)
                .instrument(span)
                .await;
            (task, result)
        });
    }
    drop(tx);

    println!("{}", Sample::HEADER);
    while let Some(sample) = rx.recv().await {
        println!("{}", sample.to_row());
        sample_done();
    }
    let counters = BTreeMap::from_iter(counters());
    tracing::info!("Counters = {counters:?}");

    while let Some(result) = tasks.join_next().await {
        match result {
            Ok((_, Ok(_))) => {}
            Ok((id, Err(e))) => tracing::error!("error in task {id}: {e}"),
            // If tracking which task failed to join ever becomes important, note that
            // AbortHandle returned by JoinSet::spawn has an id() method which returns
            // a tokio::task::Id. Since JoinError::id() also returns this exact same
            // tokio::task::Id, the task_id can be retrieved if a suitable map is created.
            Err(e) => tracing::error!("a task failed to join: {e}"),
        }
    }
    tracing::info!("DONE");

    if let Some(tracer_provider) = tracer_provider {
        if let Err(e) = tracer_provider.shutdown() {
            eprintln!("error shutting down trace provider: {e}");
        }
    }

    Ok(())
}

#[derive(Clone)]
struct Task {
    id: usize,
    start: Instant,
    tx: Sender<Sample>,
}

async fn runner(
    id: usize,
    start: Instant,
    client: Storage,
    credentials: Credentials,
    buffer: bytes::Bytes,
    tx: Sender<Sample>,
    args: Args,
) -> anyhow::Result<()> {
    tokio::time::sleep(args.rampup_period * id as u32).await;
    let task = Task { id, start, tx };
    if task.id % 128 == 0 {
        tracing::info!("Task::run({})", task.id);
    }
    let builder = StorageControl::builder().with_credentials(credentials);
    let builder = args
        .retry_timeout
        .iter()
        .fold(builder, |b, v| b.with_retry_policy(retry_policy(*v, &args)));
    let control = builder.build().await?;

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
            (Operation::SingleShot, size + 1)
        };

        let upload_span = tracing::info_span!("w1r3.upload", op = %write_op.name(), size);
        let builder = SampleBuilder::new(&task, iteration, write_op, size, name.clone());
        let upload = match upload(
            &client,
            &control,
            &args,
            &name,
            buffer.slice(0..size),
            threshold,
        )
        .instrument(upload_span)
        .await
        {
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
        for i in 0..(args.read_count) {
            let op = Operation::Read(i);
            let builder = SampleBuilder::new(&task, iteration, op, size, upload.name.clone());
            let download_span = tracing::info_span!("w1r3.download", read_index = i, size);
            let sample = match download(&client, &args, &upload)
                .instrument(download_span)
                .await
            {
                (_, Ok(_)) => builder.success(),
                (0, Err(e)) => builder.error(&e),
                (partial, Err(e)) => builder.interrupted(partial, &e),
            };
            let _ = task.tx.send(sample).await;
        }
        if !args.no_delete {
            deletes.push(delete(&control, &args, upload));
        }
        if deletes.len() >= batch_size {
            batch_size = rand::rng().sample(batch_size_gen);
            let delete_span = tracing::info_span!("w1r3.delete_batch", batch_size = deletes.len());
            batch_delete(
                &task,
                iteration,
                deletes.len(),
                deletes.drain(..),
                name.as_str(),
            )
            .instrument(delete_span)
            .await;
        }
    }
    let delete_span = tracing::info_span!("w1r3.delete_batch", batch_size = deletes.len());
    batch_delete(
        &task,
        args.iterations,
        deletes.len(),
        deletes.into_iter(),
        "N/A",
    )
    .instrument(delete_span)
    .await;
    Ok(())
}

async fn upload(
    client: &Storage,
    control: &StorageControl,
    args: &Args,
    name: &str,
    buffer: bytes::Bytes,
    threshold: usize,
) -> StorageResult<Object> {
    let timeout = args.retry_timeout.unwrap_or(DEFAULT_TIMEOUT);
    let builder = client
        .write_object(
            format!("projects/_/buckets/{}", &args.bucket_name),
            name,
            buffer,
        )
        .set_if_generation_match(0)
        .with_resumable_upload_threshold(threshold);
    let builder = args
        .retry_timeout
        .iter()
        .fold(builder, |b, v| b.with_retry_policy(retry_policy(*v, args)));
    let future = Instrumented::new(builder.send_unbuffered());

    match tokio::time::timeout(timeout, future).await {
        Err(e) => Err(google_cloud_storage::Error::timeout(e)),
        Ok(Err(e)) if e.http_status_code().is_some_and(|code| code == 412) => {
            tracing::info!("failed precondition, object may exist, fetching object details");
            get_object(control, args, name).await
        }
        Ok(Err(e)) => Err(e),
        Ok(Ok(r)) => Ok(r),
    }
}

async fn get_object(control: &StorageControl, args: &Args, name: &str) -> StorageResult<Object> {
    let timeout = args.retry_timeout.unwrap_or(DEFAULT_TIMEOUT);
    let future = control
        .get_object()
        .set_bucket(format!("projects/_/buckets/{}", &args.bucket_name))
        .set_object(name)
        .with_retry_policy(DebugRetry::new(RetryableErrors.with_time_limit(timeout)))
        .send();
    match tokio::time::timeout(timeout, Instrumented::new(future)).await {
        Err(e) => Err(google_cloud_storage::Error::timeout(e)),
        Ok(Err(e)) => Err(e),
        Ok(Ok(r)) => Ok(r),
    }
}

async fn download(
    client: &Storage,
    args: &Args,
    object: &google_cloud_storage::model::Object,
) -> (usize, StorageResult<()>) {
    let timeout = args.retry_timeout.unwrap_or(DEFAULT_TIMEOUT);
    let builder = client
        .read_object(&object.bucket, &object.name)
        .set_generation(object.generation);
    let builder = args
        .retry_timeout
        .iter()
        .fold(builder, |b, v| b.with_retry_policy(retry_policy(*v, args)));
    let read = builder.send();

    let mut read = match tokio::time::timeout(timeout, read).await {
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

    match tokio::time::timeout(timeout, Instrumented::new(read_data())).await {
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
    let builder = control
        .delete_object()
        .set_bucket(object.bucket)
        .set_object(object.name)
        .set_generation(object.generation)
        .with_attempt_timeout(args.attempt_timeout)
        .with_idempotency(true);
    let builder = args
        .retry_timeout
        .iter()
        .fold(builder, |b, v| b.with_retry_policy(retry_policy(*v, args)));
    let result = Instrumented::new(builder.send()).await;
    if let Err(e) = result {
        // Ignore NotFound errors as they may be the result of a retry.
        if e.status().is_some_and(|s| s.code == Code::NotFound) {
            return Ok(());
        }
        return Err(e);
    };
    Ok(())
}

use std::sync::Arc;
fn retry_policy(time_limit: Duration, args: &Args) -> Arc<dyn RetryPolicy> {
    if args.debug_retry {
        Arc::new(DebugRetry::new(RetryableErrors.with_time_limit(time_limit)))
    } else {
        Arc::new(RetryableErrors.with_time_limit(time_limit))
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
            relative: task.start.elapsed(),
            start: Instant::now(),
            iteration,
            op,
            target_size,
            object,
        }
    }

    fn error(self, error: &google_cloud_storage::Error) -> Sample {
        tracing::error!(
            "ERR {} sample_builder = {self:?} error = {error:?}",
            self.op.name()
        );
        let details = Self::error_details(error);
        Sample {
            task: self.task,
            iteration: self.iteration,
            op_start: self.relative,
            size: self.target_size,
            transfer_size: 0,
            op: self.op,
            elapsed: self.start.elapsed(),
            object: self.object,
            result: ExperimentResult::Error,
            details,
        }
    }

    fn interrupted(self, transfer_size: usize, error: &google_cloud_storage::Error) -> Sample {
        tracing::error!(
            "INT {} sample_builder = {self:?} error = {error:?}",
            self.op.name()
        );
        let details = Self::error_details(error);
        Sample {
            task: self.task,
            iteration: self.iteration,
            op_start: self.relative,
            size: self.target_size,
            transfer_size,
            op: self.op,
            elapsed: self.start.elapsed(),
            object: self.object.to_string(),
            result: ExperimentResult::Interrupted,
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
            elapsed: self.start.elapsed(),
            object: self.object.to_string(),
            result: ExperimentResult::Success,
            details: String::new(),
        }
    }

    fn error_details(error: &google_cloud_storage::Error) -> String {
        counters()
            .map(|(name, value)| format!("{name}={value}"))
            .chain([format!("error={error:?}")])
            .collect::<Vec<_>>()
            .join(";")
            .replace(",", ";")
    }
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
    Read(i32),
    Delete,
}

impl Operation {
    fn name(&self) -> std::borrow::Cow<'static, str> {
        match self {
            Self::Resumable => "RESUMABLE".into(),
            Self::SingleShot => "SINGLE_SHOT".into(),
            Self::Read(i) => format!("READ[{i}]").into(),
            Self::Delete => "DELETE".into(),
        }
    }
}

#[derive(Clone, Debug)]
enum ExperimentResult {
    Success,
    Error,
    Interrupted,
}

impl ExperimentResult {
    fn name(&self) -> &str {
        match self {
            Self::Success => "OK",
            Self::Error => "ERR",
            Self::Interrupted => "INT",
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

async fn enable_tracing(
    _args: &Args,
    _credentials: &Credentials,
) -> Result<Option<SdkTracerProvider>, TraceError> {
    use tracing_subscriber::fmt::format::{self, FmtSpan};
    use tracing_subscriber::prelude::*;

    let formatter = format::debug_fn(|writer, field, value| match field.name() {
        "message" => {
            let v = format!("{value:?}");
            let re = regex::Regex::new("authorization: Bearer [A-Z0-9a-z_\\-\\.]*").unwrap();
            let clean = re.replace(&v, "authorization: Bearer [censored]");
            if clean.contains(" read: b") {
                write!(
                    writer,
                    "{}: {}",
                    field,
                    &clean[..std::cmp::min(256, clean.len())]
                )
            } else if clean.contains(" write (vectored): b") {
                write!(
                    writer,
                    "{}: {}",
                    field,
                    &clean[..std::cmp::min(1024, clean.len())]
                )
            } else {
                write!(writer, "{}: {}", field, clean)
            }
        }
        _ => write!(writer, "{}: {:?}", field, value),
    })
    // Use the `tracing_subscriber::MakeFmtExt` trait to wrap the
    // formatter so that a delimiter is added between fields.
    .delimited("; ");

    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_level(true)
        .with_thread_ids(true)
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .with_writer(std::io::stderr)
        .fmt_fields(formatter)
        .with_filter(env_filter);

    let registry = tracing_subscriber::Registry::default().with(fmt_layer);

    #[cfg(google_cloud_unstable_tracing)]
    if let Some(project_id) = &_args.project_id {
        let tracer_provider =
            integration_tests_o11y::otlp::trace::Builder::new(project_id, "storage-w1r3")
                .with_credentials(_credentials.clone())
                .build()
                .await
                .inspect_err(|e| eprintln!("failed to create tracer provider: {e:?}"))?;
        // integration_tests_o11y::tracing::trace_layer already has an EnvFilter set on it.
        let otel_layer = integration_tests_o11y::tracing::trace_layer(tracer_provider.clone());
        tracing::subscriber::set_global_default(registry.with(otel_layer))
            .expect("setting global subscriber succeeds");
        return Ok(Some(tracer_provider));
    }

    tracing::subscriber::set_global_default(registry).expect("setting global subscriber succeeds");
    Ok(None)
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
    #[arg(long, value_parser = parse_duration)]
    retry_timeout: Option<Duration>,

    /// The maximum time for each attempt.
    #[arg(long, value_parser = parse_duration, default_value = "30s")]
    attempt_timeout: Duration,

    /// The rampup period between new tasks.
    #[arg(long, value_parser = parse_duration, default_value = "500ms")]
    rampup_period: Duration,

    /// Sets the number of reads on each object.
    #[arg(long, default_value_t = 3)]
    read_count: i32,

    /// Skip the deletes.
    #[arg(long)]
    no_delete: bool,

    /// Enable logs for the retry policies.
    #[arg(long)]
    debug_retry: bool,

    /// A Google Cloud project ID used to send tracing data.
    ///
    /// When set, enables OpenTelemetry export to Cloud Trace via
    /// telemetry.googleapis.com.
    #[cfg(google_cloud_unstable_tracing)]
    #[arg(long)]
    project_id: Option<String>,
}

fn parse_size_arg(arg: &str) -> anyhow::Result<u64> {
    let value = parse_size::parse_size(arg)?;
    Ok(value)
}
