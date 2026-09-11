# Cloud Storage WriteObject Upload Benchmark

Benchmarks and compares upload strategies and checksum modes for
[`write_object`](crate::client::Storage::write_object) in the Google Cloud
Storage (GCS) Rust client library.

## Motivation & Scenarios

When uploading seekable data (such as local disk files), SDKs face a protocol
trade-off regarding data integrity validation and throughput:

- **Option A (`Option_A_Unbuffered_Baseline`)**:
  - Code: `.send_unbuffered()`
  - Single continuous stream, 0 application RAM buffer.
  - Checksum calculated on the fly; verified client-side upon completion.
- **Option B (`Option_B_Unbuffered_2Pass`)**:
  - Code: `.precompute_checksums().await?.send_unbuffered()`
  - **Pass 1:** Scans the local file to compute SIMD CRC32C checksum (reads from
    physical disk if cold, populates page cache).
  - **Pass 2:** Streams data via a single continuous PUT request with
    server-side validation header (`x-goog-hash`). Reads from OS page cache.
  - 0 application RAM buffer.
- **Option C (`Option_C_Buffered_Chunked`)**:
  - Code: `.send_buffered()`
  - 1-pass chunked upload using sequential 8 MiB HTTP PUT requests.
  - Server-side validation enforced by attaching checksum to the final chunk.
  - Requires 8 MiB in-memory buffer per upload.

## Benchmark Matrix (5 Size Tiers)

1. **12 MiB (Single-Shot Multipart)**: Exercises single-shot multipart upload
   path (below default 16 MiB threshold).
1. **64 MiB (Small Resumable)**: 8 chunks in Option C vs. 1 continuous stream in
   Option B.
1. **512 MiB (Medium Resumable)**: 64 chunks in Option C vs. 1 continuous stream
   in Option B.
1. **2 GiB (Large Resumable)**: 256 chunks in Option C vs. 1 continuous stream
   in Option B.
1. **8 GiB (Stress Resumable)**: 1,024 chunks in Option C vs. 1 continuous
   stream in Option B.

## Pre-flight Check & 512 KiB Global Warmup

Before creating large test files or running measured iterations, the benchmark
performs a single **512 KiB pre-flight warmup check**:

- Validates Google Cloud authentication and bucket write/delete permissions. If
  authentication or permissions fail, the benchmark aborts immediately with
  remediation guidance.
- Primes DNS resolution and the TLS 1.3 keep-alive connection pool.
- Eliminates the need for redundant multi-gigabyte warmup uploads during actual
  scenario testing.

## Page Cache, Storage Medium & Disk Cleanup

- **Physical SSD Storage:** By default, test files are created under
  `/usr/local/google/tmp/rust-write-object-benchmarking-data` on physical SSD
  storage. This can be overridden via `--temp-dir=/path/to/dir`.
- **Cold Cache Eviction:** By default, `--cold-cache=true` is enabled. Before
  every measured iteration, the benchmark calls `posix_fadvise(DONTNEED)` once
  to evict the test file from the OS page cache (RAM), ensuring a cold physical
  disk read.
- **Disk Cleanup:** Upon benchmark completion, the temporary file is
  automatically removed from physical disk.

## Pre-requisites

- Authenticate with GCP credentials:
  ```bash
  gcloud auth application-default login
  ```
- Set required benchmark environment variables:
  ```bash
  # Target GCS bucket for uploads (required)
  export GOOGLE_CLOUD_RUST_BENCHMARKS_BUCKET="rust-write-object-benchmark-bucket"

  # Directory on physical SSD for temporary test file generation (required)
  export GOOGLE_CLOUD_RUST_BENCHMARKS_DATA_PATH="/usr/local/google/tmp/rust-write-object-benchmarking-data"

  # Directory for saving CSV latencies and JSON summary metrics (optional)
  # If omitted, metrics are printed to the terminal and file writing is skipped.
  export GOOGLE_CLOUD_RUST_BENCHMARKS_STATS_OUTPUT_PATH="/usr/local/google/tmp/rust-write-object-benchmarking-data/results"
  ```

## Running the Benchmark

### Run All 5 Tiers (12 MiB, 64 MiB, 512 MiB, 2 GiB, 8 GiB)

```bash
chmod +x run_all.sh
./run_all.sh
```

### Run a Single Configuration

```bash
cargo run --release -p storage-benchmark-write-object -- \
  --object-size 12582912 \
  --scenario all \
  --measured-iterations 5
```

### CLI Options Reference

| Flag                    | Env Variable                                     | Default             | Description                                          |
| :---------------------- | :----------------------------------------------- | :------------------ | :--------------------------------------------------- |
| `--bucket-name`         | `GOOGLE_CLOUD_RUST_BENCHMARKS_BUCKET`            | *(none, required)*  | Target bucket name                                   |
| `--temp-dir`            | `GOOGLE_CLOUD_RUST_BENCHMARKS_DATA_PATH`         | *(none, required)*  | Physical SSD directory for scratch file              |
| `--output-dir`          | `GOOGLE_CLOUD_RUST_BENCHMARKS_STATS_OUTPUT_PATH` | *(none, optional)*  | Output folder for CSV and JSON reports               |
| `--object-size`         |                                                  | `67108864` (64 MiB) | Object size in bytes                                 |
| `--scenario`            |                                                  | `all`               | Scenario (`option-a`, `option-b`, `option-c`, `all`) |
| `--cold-cache`          |                                                  | `true`              | Evict page cache between iterations                  |
| `--cleanup`             |                                                  | `true`              | Delete uploaded object after iteration               |
| `--measured-iterations` |                                                  | `5`                 | Measured iterations per scenario                     |
