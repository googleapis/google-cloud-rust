# Stress test for bidi streaming reads

This directory contains a stress test for the client library. It runs multiple
valid use-cases for the client library. The expectation is that none of them
results in errors or panics.

## Pre-requisites

Obtain a GCE instance. You should try to use a [Compute-optimized] instance
(e.g. the `c4d-*` family), with a large network bandwidth allocation. See the
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
    --location=${REGION}  gs://${BUCKET_NAME}
  ```

## Running

Start the program and use different files for `stdout` vs. `stderr`:

```shell
TS=$(date +%s); RUSTFLAGS="-C target-cpu=native" \
    cargo run --release --package storage-scenarios -- \
    --bucket-name ${BUCKET_NAME} \
    --task-count=32 \
    --grpc-subchannel-count=64 \
    --iterations 3200 \
    > bm-${TS}.txt 2> bm-${TS}.log < /dev/null
```

Wait for the program to finish.

## Upload results to BigQuery

You can upload the results to BigQuery for analysis using your favorite
statistical packages.

If you have not done so already, make a dataset:

```shell
bq mk ${GOOGLE_CLOUD_PROJECT}:scenarios
```

Then upload the results of the experiment:

```shell
bq load --source_format CSV --skip_leading_rows 1 \
    ${GOOGLE_CLOUD_PROJECT}:scenarios.bm-${TS} bm-${TS}.txt \
    Task:int64,Iteration:int64,IterationStart:int64,Scenario,OpenLatencyMicroseconds:int64,UploadId,Object,Details
```

[compute-optimized]: https://cloud.google.com/compute/docs/compute-optimized-machines
[network bandwidth]: https://cloud.google.com/compute/docs/network-bandwidth
