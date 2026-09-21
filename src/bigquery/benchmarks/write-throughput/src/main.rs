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

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use google_cloud_bigquery::client::Write;
use google_cloud_bigquery::model::{ArrowRecordBatch, ArrowSchema};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::{Duration, Instant};

mod args;
mod table;

use table::BenchmarkEnvironment;

const CSV_HEADER: &str =
    "timestamp,elapsed(s),op,iteration,count,batches/s,bytes,MB/s,errors,errors/s";

#[derive(Default)]
struct Stats {
    send_count: AtomicI64,
    send_bytes: AtomicI64,
    recv_count: AtomicI64,
    recv_bytes: AtomicI64,
    error_count: AtomicI64,
    stop_flag: AtomicBool,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let config = crate::args::parse_args();
    if config.project.is_empty() {
        anyhow::bail!(
            "GOOGLE_CLOUD_PROJECT environment variable or --project argument must be set"
        );
    }
    if config.num_tables == 0 {
        anyhow::bail!("--num-tables must be at least 1");
    }
    if config.num_writers == 0 {
        anyhow::bail!("--num-writers must be at least 1");
    }

    println!(
        "# Running BigQuery Write throughput benchmark with config: {:?}",
        config
    );
    run_benchmark(config).await?;

    Ok(())
}

async fn run_benchmark(config: crate::args::Config) -> anyhow::Result<()> {
    let env =
        BenchmarkEnvironment::setup(&config.project, &config.dataset_id, config.num_tables).await?;

    let run_res = async {
        let client = Arc::new(
            Write::builder()
                .with_grpc_subchannel_count(config.grpc_channels)
                .build()
                .await?,
        );

        let schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Utf8,
            false,
        )]));

        let pool_size = 16;
        let raw_batches = generate_batches(
            &schema,
            config.rows_per_batch,
            config.row_size,
            pool_size,
        )?;

        let mut ipc_writer = StreamWriter::try_new(Vec::new(), &schema)?;
        let schema_buf = std::mem::take(ipc_writer.get_mut());
        let mut serialized_batches = Vec::with_capacity(raw_batches.len());
        for batch in &raw_batches {
            ipc_writer.write(batch)?;
            serialized_batches.push(bytes::Bytes::from(std::mem::take(ipc_writer.get_mut())));
        }
        let batches = Arc::new(serialized_batches);

        let logical_bytes_per_batch = (config.row_size * config.rows_per_batch) as i64;
        println!(
            "# Setup complete. Row size: {} bytes, Rows per batch: {}, Logical batch size: {} bytes, Pool size: {}",
            config.row_size,
            config.rows_per_batch,
            logical_bytes_per_batch,
            pool_size
        );

        let stats = Arc::new(Stats::default());
        // Limit outstanding requests to 1000 across all writers to prevent unbounded memory growth
        let semaphore = Arc::new(tokio::sync::Semaphore::new(1000));

        println!("# Spawning {} writer stream tasks...", config.num_writers);
        let mut writer_tasks = Vec::with_capacity(config.num_writers);
        for w in 0..config.num_writers {
            let table_id = &env.table_ids[w % env.table_ids.len()];
            let table_path = format!(
                "projects/{}/datasets/{}/tables/{}",
                env.project, env.dataset_id, table_id
            );

            let ctx = StreamTaskContext {
                task_id: w,
                client: client.clone(),
                table_path,
                schema_buf: schema_buf.clone(),
                batches: batches.clone(),
                stats: stats.clone(),
                semaphore: semaphore.clone(),
                logical_bytes_per_batch,
            };
            writer_tasks.push(tokio::spawn(run_stream_task(ctx)));
        }

        let start_time = Instant::now();
        run_reporter(stats.clone(), config.report_interval, config.duration).await;

        // Drain all writer loops and in-flight requests before generating summary
        drain_and_shutdown(&stats, writer_tasks, &semaphore, 1000).await?;

        print_summary(&stats, start_time.elapsed(), &config);

        Ok(())
    }
    .await;

    env.cleanup().await;
    run_res
}

struct StreamTaskContext {
    task_id: usize,
    client: Arc<Write>,
    table_path: String,
    schema_buf: Vec<u8>,
    batches: Arc<Vec<bytes::Bytes>>,
    stats: Arc<Stats>,
    semaphore: Arc<tokio::sync::Semaphore>,
    logical_bytes_per_batch: i64,
}

/// Represents an individual stream worker task.
async fn run_stream_task(ctx: StreamTaskContext) -> anyhow::Result<()> {
    let StreamTaskContext {
        task_id,
        client,
        table_path,
        schema_buf,
        batches,
        stats,
        semaphore,
        logical_bytes_per_batch,
    } = ctx;

    let arrow_schema = ArrowSchema::new().set_serialized_schema(schema_buf);
    let writer = Arc::new(
        client
            .open_default_stream(table_path)
            .build_arrow(arrow_schema)
            .await?,
    );

    let mut seq = 0usize;
    loop {
        if stats.stop_flag.load(Ordering::Relaxed) {
            break;
        }

        let permit = match semaphore.clone().acquire_owned().await {
            Ok(p) => p,
            Err(_) => break,
        };

        let batch_bytes = batches[seq % batches.len()].clone();
        seq = seq.wrapping_add(1);

        let rows = ArrowRecordBatch::new().set_serialized_record_batch(batch_bytes);
        let append = writer.append(rows);

        stats.send_count.fetch_add(1, Ordering::Relaxed);
        stats
            .send_bytes
            .fetch_add(logical_bytes_per_batch, Ordering::Relaxed);

        let stats = stats.clone();
        tokio::spawn(async move {
            let _permit = permit;
            match append.send().await {
                Ok(_) => {
                    stats.recv_count.fetch_add(1, Ordering::Relaxed);
                    stats
                        .recv_bytes
                        .fetch_add(logical_bytes_per_batch, Ordering::Relaxed);
                }
                Err(e) => {
                    eprintln!("Write error on writer {}: {:?}", task_id, e);
                    stats.error_count.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
    }

    Ok(())
}

fn generate_batches(
    schema: &Arc<Schema>,
    rows_per_batch: usize,
    row_size: usize,
    pool_size: usize,
) -> anyhow::Result<Vec<RecordBatch>> {
    println!("# Pre-generating batches with distinct rows...");
    let mut batches = Vec::with_capacity(pool_size);
    for b in 0..pool_size {
        let mut rows = Vec::with_capacity(rows_per_batch);
        for r in 0..rows_per_batch {
            let id = b * rows_per_batch + r;
            let mut row = format!("{:0width$}", id, width = row_size);
            if row.len() > row_size {
                row.truncate(row_size);
            }
            rows.push(row);
        }
        let row_slices: Vec<&str> = rows.iter().map(|s| s.as_str()).collect();
        let payload_array = StringArray::from(row_slices);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(payload_array)])?;
        batches.push(batch);
    }
    Ok(batches)
}

async fn run_reporter(stats: Arc<Stats>, report_interval: Duration, total_duration: Duration) {
    println!("{}", CSV_HEADER);
    let start_time = Instant::now();
    let mut iteration = 0;

    loop {
        let elapsed = start_time.elapsed();
        if elapsed >= total_duration || stats.stop_flag.load(Ordering::Relaxed) {
            break;
        }

        let interval_start = Instant::now();
        let start_send_count = stats.send_count.load(Ordering::Relaxed);
        let start_send_bytes = stats.send_bytes.load(Ordering::Relaxed);
        let start_recv_count = stats.recv_count.load(Ordering::Relaxed);
        let start_recv_bytes = stats.recv_bytes.load(Ordering::Relaxed);
        let start_error_count = stats.error_count.load(Ordering::Relaxed);

        tokio::time::sleep(report_interval).await;

        let elapsed_interval = interval_start.elapsed();
        let send_count_last = stats.send_count.load(Ordering::Relaxed) - start_send_count;
        let send_bytes_last = stats.send_bytes.load(Ordering::Relaxed) - start_send_bytes;
        let recv_count_last = stats.recv_count.load(Ordering::Relaxed) - start_recv_count;
        let recv_bytes_last = stats.recv_bytes.load(Ordering::Relaxed) - start_recv_bytes;
        let error_count_last = stats.error_count.load(Ordering::Relaxed) - start_error_count;

        print_result(
            "Send",
            iteration,
            send_count_last,
            send_bytes_last,
            0,
            elapsed_interval,
        );
        print_result(
            "Recv",
            iteration,
            recv_count_last,
            recv_bytes_last,
            error_count_last,
            elapsed_interval,
        );

        iteration += 1;
    }
}

async fn drain_and_shutdown(
    stats: &Stats,
    writer_tasks: Vec<tokio::task::JoinHandle<anyhow::Result<()>>>,
    semaphore: &tokio::sync::Semaphore,
    max_concurrency: u32,
) -> anyhow::Result<()> {
    // Set stop flag so all writer loops exit
    stats.stop_flag.store(true, Ordering::Relaxed);

    // Await all writer stream tasks to finish and collect any errors
    let mut result = Ok(());
    for task in writer_tasks {
        match task.await {
            Ok(Err(e)) => result = Err(e),
            Err(e) => result = Err(anyhow::anyhow!("Task join error: {:?}", e)),
            _ => {}
        }
    }

    // Wait for all outstanding in-flight requests to complete
    let _ = semaphore.acquire_many(max_concurrency).await;

    result
}

fn print_summary(stats: &Stats, total_elapsed: Duration, config: &crate::args::Config) {
    println!("# Benchmark finished.");
    println!("# Configuration: {:?}", config);
    let total_elapsed_s = total_elapsed.as_secs_f64();

    let total_send_count = stats.send_count.load(Ordering::Relaxed);
    let total_send_bytes = stats.send_bytes.load(Ordering::Relaxed);
    let total_recv_count = stats.recv_count.load(Ordering::Relaxed);
    let total_recv_bytes = stats.recv_bytes.load(Ordering::Relaxed);
    let total_errors = stats.error_count.load(Ordering::Relaxed);

    let send_rate = (total_send_count as f64) / total_elapsed_s;
    let send_mbs = (total_send_bytes as f64) / total_elapsed_s / 1_000_000.0;

    let recv_rate = (total_recv_count as f64) / total_elapsed_s;
    let recv_mbs = (total_recv_bytes as f64) / total_elapsed_s / 1_000_000.0;

    let error_rate = (total_errors as f64) / total_elapsed_s;
    let error_percentage = if total_recv_count + total_errors > 0 {
        (total_errors as f64) / ((total_recv_count + total_errors) as f64) * 100.0
    } else {
        0.0
    };

    println!("# Summary:");
    println!("# Elapsed time: {:.2}s", total_elapsed_s);
    println!("# Total batches sent: {}", total_send_count);
    println!(
        "# Total data sent: {:.2} MB (rate: {:.2} batches/s, {:.2} MB/s)",
        (total_send_bytes as f64) / 1_000_000.0,
        send_rate,
        send_mbs
    );
    println!("# Total batches completed: {}", total_recv_count);
    println!(
        "# Total data completed: {:.2} MB (rate: {:.2} batches/s, {:.2} MB/s)",
        (total_recv_bytes as f64) / 1_000_000.0,
        recv_rate,
        recv_mbs
    );
    println!(
        "# Total errors: {} (rate: {:.2} errors/s, percentage: {:.2}%)",
        total_errors, error_rate, error_percentage
    );
}

fn print_result(
    operation: &str,
    iteration: i64,
    count: i64,
    bytes: i64,
    errors: i64,
    elapsed: Duration,
) {
    let elapsed_s = elapsed.as_secs_f64();
    let mbs = (bytes as f64) / elapsed_s / 1_000_000.0;
    let msgs = (count as f64) / elapsed_s;
    let errs = (errors as f64) / elapsed_s;
    println!(
        "{},{},{},{},{},{:.2},{},{:.2},{},{:.2}",
        timestamp(),
        elapsed_s,
        operation,
        iteration,
        count,
        msgs,
        bytes,
        mbs,
        errors,
        errs
    );
}

fn timestamp() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or_default()
}
