# Cloud Storage Appendable Upload Benchmark

Benchmarks the Cloud Storage appendable upload performance.

## Pre-requisites

- **VM Provisioning**: Run on a Google Compute Engine (GCE) instance.
- **Zonal Bucket**: The target GCS bucket **must be a zonal bucket**. It is
  better if it is co-located in the exact same zone as your VM to isolate append
  overhead from inter-zonal routing latency.

## Bucket Setup

Provision a zonal testing bucket using the following `gcloud` snippet:

```bash
export ZONE="us-central1-a"
export GOOGLE_CLOUD_RUST_BENCHMARKS_BUCKET="storage-appendable-object-benchmark"

# Create a lifecycle configuration to automatically delete uploaded test objects after 1 day
echo '{ "lifecycle": { "rule": [ { "action": {"type": "Delete"}, "condition": {"age": 1} } ] } }' > lf.json

gcloud storage buckets create gs://${GOOGLE_CLOUD_RUST_BENCHMARKS_BUCKET} \
  --location=${ZONE} \
  --placement=zone \
  --default-storage-class=RAPID \
  --hierarchical-namespace \
  --uniform-bucket-level-access \
  --soft-delete-duration=0s \
  --lifecycle-file=lf.json
```

## Running the Benchmark

The suite uses an orchestrator script (`run_all.sh`) to sequentially test the
configurations mapping to our steady-state benchmark scenario. By default, each
scenario executes 105 runs total (5 warmup, 100 measured). This can be
overridden by appending `--warmup-iterations=X` and `--measured-iterations=Y` to
the script execution (note: at least 100 measured iterations are recommended so
percentiles like P99 have statistical significance).

1. **Authenticate**:
   ```bash
   gcloud auth application-default login
   ```
1. **Set Bucket Name**:
   ```bash
   export GOOGLE_CLOUD_RUST_BENCHMARKS_BUCKET=storage-appendable-object-benchmark
   ```
1. **Execute**:
   ```bash
   chmod +x run_all.sh
   ./run_all.sh
   ```

### Output Reporting

By default, the summarized percentile latency metrics are printed to standard
output.

To save per-iteration raw latency data (CSV) and the summary metrics (JSON) to
disk, pass `--output-dir=/path/to/dir`:

```bash
./run_all.sh --output-dir=/path/to/results
```

### Running a Single Configuration

You can also run a specific benchmark configuration directly via `cargo`:

```bash
# Example: --chunk-size 262144 represents a 256 KiB chunk (256 * 1024 bytes)
RUSTFLAGS="--cfg google_cloud_unstable_storage_bidi" \
  cargo run --release -p storage-benchmark-appendable-object -- \
  --chunk-size 262144 \
  --output-dir=/path/to/results
```
