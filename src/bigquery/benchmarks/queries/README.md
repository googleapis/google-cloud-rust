# BigQuery SDK Benchmark & Endurance Test Suite

Benchmarks the Rust BigQuery client library (`google-cloud-bigquery`), measuring
latency distributions (P50/P90/P99), query throughput, and endurance under
long-running workloads with OpenTelemetry tracking and under-the-hood job retry
detection.

## Features

- **Benchmark & Endurance Modes**: Run fixed iterations or continuous
  duration-based tests (e.g. 10m, 2h, 24h).
- **Under-the-Hood Retry Detection**: Detects BigQuery job retries by checking
  if the `job_id` changed between `Query::send()` and `Query::until_done()`.
- **OpenTelemetry & Cloud Observability**: Automatically exports distributed
  traces to **Google Cloud Trace** and metric instruments to **Google Cloud
  Monitoring** when `--project-id` is provided.
- **Configurable Scenarios**:
  - `synthetic-100k` (default): Zero-dependency query generating 100,000
    structured rows in-flight with `UNNEST(GENERATE_ARRAY(1, 100000))`.
  - `synthetic-10k`: Zero-dependency query generating 10,000 rows.
  - `usa-names-scan`: Scans and retrieves 50,000 rows from
    `bigquery-public-data.usa_names.usa_1910_2013`.
  - `usa-names-agg`: Aggregates 5.5M rows grouped by state and gender.
  - `wikipedia-agg`: Aggregates top 1000 page views from Wikipedia public
    dataset.
  - `custom`: Executes user-provided queries via `--sql` or `--sql-file`.

______________________________________________________________________

## Pre-requisites

1. **Authentication**: Ensure Application Default Credentials (ADC) are
   configured:

   ```shell
   gcloud auth application-default login
   ```

1. **Project ID**: Set the project ID environment variable:

   ```shell
   export GOOGLE_CLOUD_PROJECT="$(gcloud config get project)"
   ```

______________________________________________________________________

## Running Benchmarks

> [!NOTE]
> **Query Caching is disabled by default (`--use-query-cache false`)** to force
> queries to always execute against storage and provide accurate, repeatable
> latency measurements. You can pass `--use-query-cache true` if you wish to
> benchmark cache hit performance.
>
> **Indefinite Execution by Default:** If neither `--iterations` nor
> `--duration` is specified, the benchmark runs indefinitely until interrupted
> with `Ctrl+C`.

### Synthetic Benchmark

Runs 10 iterations per task with 4 concurrent tasks, generating and streaming
100,000 rows per query:

```shell
cargo run --release -p bigquery-benchmark-queries -- \
    --project-id ${GOOGLE_CLOUD_PROJECT} \
    --scenario synthetic-100k \
    --task-count 4 \
    --iterations 10 \
    --output-dir ./results
```

### Public Dataset Query Benchmark

Benchmark streaming 50,000 rows from the USA names public dataset:

```shell
cargo run --release -p bigquery-benchmark-queries -- \
    --project-id ${GOOGLE_CLOUD_PROJECT} \
    --scenario usa-names-scan \
    --task-count 2 \
    --iterations 20 \
    --output-dir ./results
```

### Custom SQL Query Benchmark

```shell
cargo run --release -p bigquery-benchmark-queries -- \
    --project-id ${GOOGLE_CLOUD_PROJECT} \
    --scenario custom \
    --sql "SELECT word, SUM(word_count) as total FROM \`bigquery-public-data.samples.shakespeare\` GROUP BY word ORDER BY total DESC LIMIT 100" \
    --task-count 4 \
    --iterations 25
```

______________________________________________________________________

## Running Endurance Tests

To test connection stability, memory leaks, and token refreshing over a
prolonged period (e.g. 1 hour):

```shell
cargo run --release -p bigquery-benchmark-queries -- \
    --project-id ${GOOGLE_CLOUD_PROJECT} \
    --scenario synthetic-100k \
    --task-count 4 \
    --duration 1h \
    --output-dir ./results
```

### Running as a Background Job

To run a long endurance test in the background (similar to the Pub/Sub benchmark
pattern) while capturing stdout reports to `.txt`, stderr tracing logs to
`.log`, and real-time CSV/JSON samples to `./results`:

```shell
TS=$(date +%s); RUSTFLAGS="-C target-cpu=native" \
  cargo run --release -p bigquery-benchmark-queries -- \
  --project-id "${GOOGLE_CLOUD_PROJECT:-$(gcloud config get project)}" \
  --scenario synthetic-100k \
  --task-count 4 \
  --duration 1h \
  --output-dir ./results \
  >bq-bm-${TS}.txt 2>bq-bm-${TS}.log </dev/null &
```

You can monitor the progress in real time with:

```shell
# Follow tracing & runtime logs
tail -f bq-bm-${TS}.log

# Follow raw per-query CSV rows as they arrive
tail -f results/samples-synthetic-100k-*.csv
```

### Running on a Google Compute Engine (GCE) VM & Viewing Cloud Console Logs

To execute endurance tests on a GCE VM and stream all logs directly into
**Google Cloud Console (Cloud Logging)**:

1. **Create the VM and install Ops Agent**:

   ```shell
   # Create a VM with full cloud-platform scope
   gcloud compute instances create bq-benchmark-vm \
     --zone=us-central1-a \
     --machine-type=c2-standard-4 \
     --scopes=cloud-platform \
     --image-family=debian-12 \
     --image-project=debian-cloud

   # 2. Install the Google Cloud Ops Agent (streams journald logs to Cloud Logging)
   gcloud compute ssh bq-benchmark-vm --zone=us-central1-a --command="
     curl -sSO https://dl.google.com/cloudagents/add-google-cloud-ops-agent-repo.sh
     sudo bash add-google-cloud-ops-agent-repo.sh --also-install
   "
   ```

1. **Run in the background via `systemd-cat`**: Using `systemd-cat` tags the
   output in the system journal so the Ops Agent automatically forwards stdout
   and stderr to Cloud Logging:

   ```shell
   gcloud compute ssh bq-benchmark-vm --zone=us-central1-a

   # Run benchmark in background: logs to local files AND streams to systemd-cat / Cloud Logging
   TS=$(date +%s); RUSTFLAGS="-C target-cpu=native" \
     cargo run --release -p bigquery-benchmark-queries -- \
     --project-id "aviebrantz-testing" \
     --scenario synthetic-100k \
     --task-count 16 \
     --duration 1h \
     --output-dir ./results \
     > >(tee bq-bm-${TS}.txt | systemd-cat -t bq-benchmark) \
     2> >(tee bq-bm-${TS}.log | systemd-cat -t bq-benchmark) </dev/null &
   ```

1. **View Logs in Google Cloud Console**:

   - Navigate to **Logging > Logs Explorer** in Google Cloud Console.
   - Filter by your identifier:
     ```log
     resource.type="gce_instance"
     jsonPayload.SYSLOG_IDENTIFIER="bq-benchmark"
     ```
   - Click **Stream logs** in the top right to watch real-time benchmark
     execution logs arrive in the console.

______________________________________________________________________

## OpenTelemetry & Cloud Observability

When `--project-id` is specified, the benchmark automatically connects to
`telemetry.googleapis.com` and records:

### Cloud Monitoring Metrics

- `bigquery.queries.total`: Total count of executed queries.
- `bigquery.queries.success`: Successful query executions.
- `bigquery.queries.error`: Query failures.
- `bigquery.queries.retries_detected`: Queries where an under-the-hood job retry
  was detected.
- `bigquery.queries.rows_read`: Cumulative rows streamed.
- `bigquery.queries.bytes_processed`: Total bytes processed.
- `bigquery.queries.duration_seconds`: Histogram of total end-to-end query
  latency.
- `bigquery.queries.send_duration_seconds`: Histogram of `Query::send()`
  latency.
- `bigquery.queries.poll_duration_seconds`: Histogram of `Query::until_done()`
  polling latency.
- `bigquery.queries.read_duration_seconds`: Histogram of `CompleteQuery::read()`
  row streaming latency.

### Cloud Trace Spans

- Root span: `bigquery.query_benchmark.iteration`
- Child spans:
  - `bigquery.send`
  - `bigquery.until_done`
  - `bigquery.read_rows`

### Monitoring with PromQL in Google Cloud Monitoring

In the Google Cloud Console, navigate to **Monitoring** > **Metrics Explorer**
and select the **PromQL** tab. You can use the following PromQL queries to
visualize the benchmark metrics:

#### 1. Query Throughput (QPS by Status and Scenario)

Tracks the rate of queries executed per second:

```promql
sum by (scenario, status) (rate(workload_googleapis_com:bigquery_queries_total[1m]))
```

#### 2. Under-the-Hood Job Retries (Retry Rate & Count)

Tracks how often queries triggered a backend job retry (where the job ID mutated
between `Query::send()` and `Query::until_done()` - needs to be improved to
detect when queries are retried on creation):

```promql
sum by (scenario) (rate(workload_googleapis_com:bigquery_queries_retries_detected[1m]))
```

To calculate the **Percentage of Queries Retried**:

```promql
100 * sum(rate(workload_googleapis_com:bigquery_queries_retries_detected[1m]))
  / sum(rate(workload_googleapis_com:bigquery_queries_total[1m]))
```

#### 3. Error Rate (%)

Tracks the percentage of query executions that failed:

```promql
100 * sum(rate(workload_googleapis_com:bigquery_queries_error[1m]))
  / sum(rate(workload_googleapis_com:bigquery_queries_total[1m]))
```

#### 4. End-to-End Query Latency Percentiles (P50, P90, P99)

Calculates the 50th, 90th, and 99th percentile query latencies from histogram
buckets:

```promql
# P99 Latency (seconds)
histogram_quantile(0.99, sum by (le, scenario) (rate(workload_googleapis_com:bigquery_queries_duration_seconds_bucket[1m])))

# P90 Latency (seconds)
histogram_quantile(0.90, sum by (le, scenario) (rate(workload_googleapis_com:bigquery_queries_duration_seconds_bucket[1m])))

# P50 (Median) Latency (seconds)
histogram_quantile(0.50, sum by (le, scenario) (rate(workload_googleapis_com:bigquery_queries_duration_seconds_bucket[1m])))
```

#### 5. Query Phase Latency Breakdown (P95 comparison)

Compare where time is spent across `send()`, `until_done()`, and `read()`:

```promql
# Query::send() P95 Latency
histogram_quantile(0.95, sum by (le, scenario) (rate(workload_googleapis_com:bigquery_queries_send_duration_seconds_bucket[1m])))

# Query::until_done() P95 Polling Latency
histogram_quantile(0.95, sum by (le, scenario) (rate(workload_googleapis_com:bigquery_queries_poll_duration_seconds_bucket[1m])))

# CompleteQuery::read() P95 Row Streaming Latency
histogram_quantile(0.95, sum by (le, scenario) (rate(workload_googleapis_com:bigquery_queries_read_duration_seconds_bucket[1m])))
```

#### 6. Row and Byte Streaming Throughput

Tracks data processing throughput (rows/sec and MiB/sec):

```promql
# Rows streamed per second
sum by (scenario) (rate(workload_googleapis_com:bigquery_queries_rows_read[1m]))

# MiB processed per second
sum by (scenario) (rate(workload_googleapis_com:bigquery_queries_bytes_processed[1m])) / (1024 * 1024)
```

______________________________________________________________________

## Uploading Results to BigQuery

If `--output-dir` is specified, the suite saves per-iteration raw samples to a
CSV file (e.g. `results/samples-synthetic-100k-*.csv`). You can upload the
samples to BigQuery for SQL analysis:

```shell
bq load --source_format=CSV --skip_leading_rows=1 \
    ${GOOGLE_CLOUD_PROJECT}:benchmark_dataset.bigquery_samples \
    ./results/samples-*.csv \
    Task:int64,Iteration:int64,StartOffsetMicros:int64,SendDurationMicros:int64,PollDurationMicros:int64,ReadDurationMicros:int64,TotalDurationMicros:int64,RowsCount:int64,BytesProcessed:int64,CacheHit:bool,InitialJobId:string,FinalJobId:string,RetryDetected:bool,Status:string,ErrorMessage:string
```
