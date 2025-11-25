# Random Reads Benchmark

Benchmarks the Cloud Storage client library. The benchmark uploads a number of
objects and then reads ranges from them at random, reporting the TTFB
(time-to-first byte) latency, TTLB (time-to-last byte) latency for each read.

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
TS=$(date +%s); cargo run --release --package storage-random -- \
    --bucket-name ${BUCKET_NAME} \
    --min-range-size 8KiB --max-range-size 8KiB \
    --min-batch-size 16 --max-batch-size 16 \
    --task-count=4 \
    --min-sample-count=1000  >bm-${TS}.txt 2>bm-${TS}.log </dev/null &
```

Wait for the program to finish.

## Upload results to BigQuery

You can upload the results to BigQuery for analysis using your favorite
statistical packages.

If you have not done so already, make a dataset:

```shell
bq mk ${GOOGLE_CLOUD_PROJECT}:random
```

Then upload the results of the experiment:

```shell
bq load --source_format CSV --skip_leading_rows 1 \
    ${GOOGLE_CLOUD_PROJECT}:random.small001 bm-${TS}.txt \
    Task:int64,Iteration:int64,IterationStart:int64,RangeId:int64,RangeCount:int64,RangeSize:int64,Protocol,TtfbMicroseconds:int64,TtlbMicroseconds:int64,Object,Details
```

[compute-optimized]: https://cloud.google.com/compute/docs/compute-optimized-machines
[network bandwidth]: https://cloud.google.com/compute/docs/network-bandwidth
