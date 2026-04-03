# Pub/Sub Throughput Benchmark

A throughput benchmark for the Cloud Pub/Sub Rust client library.

This tool measures the performance of publishing messages to a Google Cloud
Pub/Sub topic, or receiving messages from a subscription. It reports operation
rates in messages per second and megabytes per second.

## Usage

```bash
cargo run --release -p pubsub-throughput -- [COMMAND] [OPTIONS]
```

To see the commands and options use:

```bash
cargo run -p pubsub-throughput -- --help
```

## Output Format

The benchmark outputs data in CSV format with the following columns:

- `timestamp`: The Unix timestamp in milliseconds.
- `elapsed(s)`: The elapsed time for the operation in seconds.
- `op`: The operation being measured (`Pub`, `Ack`, or `Recv`).
- `iteration`: The current iteration number.
- `count`: The number of messages processed in the operation.
- `msgs/s`: The number of messages per second.
- `bytes`: The total number of bytes processed.
- `MB/s`: The throughput in megabytes per second.
- `errors`: The number of errors encountered (if any).
- `errors/s`: The number of errors per second (if any).

## Examples

### Setup

Create a topic and subscription:

```bash
gcloud pubsub topics create my-topic
gcloud pubsub subscriptions create my-subscription --topic=my-topic
```

### Publisher Run

```bash
cargo run --release -p pubsub-throughput -- publisher \
    --project my-gcp-project \
    --topic-id my-topic \
    --payload-size 2048 \
    --report-interval 10s \
    --duration 1m
```

### Subscriber Run

```bash
cargo run --release -p pubsub-throughput -- subscriber \
    --project my-gcp-project \
    --subscription-id my-subscription \
    --report-interval 10s \
    --duration 1m
```
