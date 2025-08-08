# W1R3 Benchmark

Benchmarks the Cloud Storage client library. The benchmark uploads an object and
reads it 3 times, reporting the single-stream upload and download bandwidth.

## Pre-requisites

Obtain a GCE instance. You should try to use a [Compute-optimized] instance
(e.g. the `c2d-*` family), with a large network bandwidth allocation. See the
[Network Bandwidth] guide.

## Bucket

Use a bucket in the same region as your VM. If you need to create a bucket,
these instructions may help:

- Create a configuration file to automatically delete objects after one day

  ```shell
  echo '{ "lifecycle": { "rule": [ { "action": {"type": "Delete"}, "condition": {"age": 1} } ] } }' > lf.json
  ```

- Create the bucket. Replace the `${REGION}` and `${BUCKET_NAME}` placeholders
  as needed:

  ```shell
  gcloud storage buckets create \
    --enable-hierarchical-namespace --uniform-bucket-level-access \
    --soft-delete-duration=0s --lifecycle-file=lf.json \
    --location=${REGION}  gs://${BUCKET_NAME}$
  ```

## Running

Start the program and use different files for `stdout` vs. `stderr`:

```shell
TS=$(date +%s); cargo run --release --package storage-w1r3 -- \
    --bucket-name ${BUCKET_NAME} --max-object-size 128KiB --task-count=4 \
    --min-sample-count=1000  >bm-${TS}.txt 2>bm-${TS}.log </dev/null &
```

Wait for the program to finish.

## Upload results to BigQuery

You can upload the results to BigQuery for analysis using your favorite
statistical packages:

```shell
bq load --source_format CSV --skip_leading_rows 1 \
    ${GOOGLE_CLOUD_PROJECT}:w1r3.small001 bm-${TS}$.txt \
    Experiment,Task:int64,Iteration:int64,IterationStart:int64,Operation,Size:int64,TransferSize:int64,ElapsedMicroseconds:int64,Object,Result,Details
```

[compute-optimized]: https://cloud.google.com/compute/docs/compute-optimized-machines
[network bandwidth]: https://cloud.google.com/compute/docs/network-bandwidth
