# BigQuery Write Throughput Benchmark

A throughput benchmark for the BigQuery Storage Write API in the
`google-cloud-bigquery` Rust client library.

This tool measures the streaming ingestion performance of the default stream in
the BigQuery Storage Write API using Arrow data. It reports operation rates in
batches per second and megabytes per second.

## Usage

```bash
cargo run --release -p bigquery-write-throughput -- [OPTIONS]
```

To view all available options, run:

```bash
cargo run -p bigquery-write-throughput -- --help
```

## Options

- `--project <PROJECT>`: Google Cloud project ID (can also be set via `GOOGLE_CLOUD_PROJECT`).
- `--duration <DURATION>`: Benchmark runtime duration (e.g. `1m`, `5m`, `300s`). Default: `1m`.
- `--report-interval <REPORT_INTERVAL>`: Frequency of progress metrics reporting (e.g. `5s`, `10s`). Default: `5s`.
- `--row-size <ROW_SIZE>`: Size of each row payload in bytes. Default: `1024`.
- `--rows-per-batch <ROWS_PER_BATCH>`: Number of rows per serialized Arrow RecordBatch. Default: `1000`.
- `--num-tables <NUM_TABLES>`: Number of tables created/written to in the dataset. Default: `1`.
- `--num-writers <NUM_WRITERS>`: Number of concurrent writers. Default: `1`.
- `--grpc-channels <GRPC_CHANNELS>`: Number of gRPC subchannels configured on the client. Default: `1`.
- `--dataset-id <DATASET_ID>`: Target dataset ID. If not specified, a temporary dataset (`rust_bq_bench_dataset_<random>`) is created and automatically cleaned up upon completion.

## Output Format

The benchmark outputs progress data in CSV format:

- `timestamp`: Unix epoch timestamp in milliseconds.
- `elapsed(s)`: Elapsed time for the reported interval in seconds.
- `op`: Operation type (`Send` for dispatched batches, `Recv` for acknowledged batches).
- `iteration`: Current report iteration number.
- `count`: Number of batches processed in this interval.
- `batches/s`: Batches processed per second.
- `bytes`: Total bytes processed in this interval.
- `MB/s`: Throughput in megabytes per second.
- `errors`: Number of errors encountered in this interval.
- `errors/s`: Number of errors per second.

## Example

```bash
cargo run --release -p bigquery-write-throughput -- \
    --project ${GOOGLE_CLOUD_PROJECT} \
    --duration 1m \
    --report-interval 10s \
    --num-writers 2 \
    --grpc-channels 1
```
