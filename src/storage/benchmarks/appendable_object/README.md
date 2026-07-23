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
export BUCKET_NAME="storage-appendable-object-benchmark"

gcloud storage buckets create gs://${BUCKET_NAME} \
  --location=${ZONE} \
  --placement=zone \
  --default-storage-class=RAPID \
  --hierarchical-namespace \
  --uniform-bucket-level-access
```

## Running the Benchmark

The suite uses an orchestrator script (`run_all.sh`) to sequentially test the
configurations mapping to our design scenarios (chunk geometries, flushing,
concurrency). By default, each scenario executes 30 runs total (5 warmup,
25 measured). This can be completely overridden by appending
`--warmup-iterations=X` and `--measured-iterations=Y` to the script execution.

1. **Authenticate**:
   ```bash
   gcloud auth application-default login
   ```
2. **Set Bucket Name**:
   ```bash
   export GOOGLE_CLOUD_RUST_BENCHMARKS_BUCKET=storage-appendable-object-benchmark
   ```
3. **Execute**:
   ```bash
   chmod +x run_all.sh
   ./run_all.sh
   ```

By default, the summarized percentile latency metrics print to standard output,
and raw iteration-by-iteration latency data is written to local CSV files.
Use `--output-dir=/path/to/dir` to change the directory where the CSV files are
written.

Example:
```bash
./run_all.sh --output-dir=/path/to/results
```
