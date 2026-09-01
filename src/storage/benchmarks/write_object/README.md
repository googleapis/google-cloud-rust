# Cloud Storage WriteObject Upload Benchmark

Benchmarks and compares upload strategies and checksum modes for [`write_object`](crate::client::Storage::write_object) in the Google Cloud Storage (GCS) Rust client library.

## Motivation & Scenarios

When uploading seekable data (such as local disk files), SDKs face a protocol trade-off regarding data integrity validation and throughput:

* **Option A (`Option_A_Unbuffered_Baseline`)**:
  * Code: `.send_unbuffered()`
  * Single continuous stream, 0 application RAM buffer.
  * Checksum calculated on the fly; verified client-side upon completion.
* **Option B (`Option_B_Unbuffered_2Pass`)**:
  * Code: `.precompute_checksums().await?.send_unbuffered()`
  * **Pass 1:** Scans the local file to compute SIMD CRC32C checksum (reads from physical disk if cold, populates page cache).
  * **Pass 2:** Streams data via a single continuous PUT request with server-side validation header (`x-goog-hash`). Reads from OS page cache.
  * 0 application RAM buffer.
* **Option C (`Option_C_Buffered_Chunked`)**:
  * Code: `.send_buffered()`
  * 1-pass chunked upload using sequential 8 MiB HTTP PUT requests.
  * Server-side validation enforced by attaching checksum to the final chunk.
  * Requires 8 MiB in-memory buffer per upload.

## Benchmark Matrix (5 Size Tiers)

1. **12 MiB (Single-Shot Multipart)**: Exercises single-shot multipart upload path (below default 16 MiB threshold).
2. **64 MiB (Small Resumable)**: 8 chunks in Option C vs. 1 continuous stream in Option B.
3. **512 MiB (Medium Resumable)**: 64 chunks in Option C vs. 1 continuous stream in Option B.
4. **2 GiB (Large Resumable)**: 256 chunks in Option C vs. 1 continuous stream in Option B.
5. **8 GiB (Stress Resumable)**: 1,024 chunks in Option C vs. 1 continuous stream in Option B.

## Page Cache & Cold Disk Simulation

By default, `--cold-cache=true` is enabled. Before every iteration, the benchmark calls `posix_fadvise(DONTNEED)` once to evict the test file from the OS page cache (RAM). This ensures that:
- **Option A** performs 1 cold disk read (streamed to network).
- **Option B** performs 1 cold disk read in Pass 1 (local SIMD checksumming), and Pass 2 naturally streams from the hot page cache populated by Pass 1.
- **Option C** performs 1 cold disk read (in 8 MiB chunks streamed to network).

To place the test file on a specific physical SSD/HDD mount instead of `/tmp`, pass `--temp-dir=/path/to/mount`.

## Pre-requisites

- Authenticate with GCP credentials:
  ```bash
  gcloud auth application-default login
  ```
- Set target bucket environment variable:
  ```bash
  export GOOGLE_CLOUD_RUST_BENCHMARKS_BUCKET="my-benchmark-bucket"
  ```

## Running the Benchmark

### Run All 5 Tiers
```bash
chmod +x run_all.sh
./run_all.sh
```

### Run a Single Configuration
```bash
cargo run --release -p storage-benchmark-write-object -- \
  --object-size 67108864 \
  --scenario all \
  --cold-cache true \
  --warmup-iterations 1 \
  --measured-iterations 5
```

### Save Output Metrics (CSV & JSON)
```bash
./run_all.sh --output-dir=/path/to/results
```
