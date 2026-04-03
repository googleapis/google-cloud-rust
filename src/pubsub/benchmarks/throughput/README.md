# Pub/Sub Throughput Benchmark

A throughput benchmark for the Cloud Pub/Sub Rust client library.

This tool measures the performance of publishing messages to a Google Cloud
Pub/Sub topic, or receiving messages from a subscription. It reports operation
rates in messages per second and megabytes per second.

## Usage

```bash
cargo run --release -p pubsub-throughput -- [COMMAND] [OPTIONS]
```

### Commands

- `publisher`: Measure publishing throughput.
- `subscriber`: Measure subscribing throughput.

### Common Options (Global)

- `--project`: The Google Cloud project ID.
- `--report-interval`: The interval between progress reports (e.g., `5s`, `1m`;
  default: `5s`).
- `--duration`: The total duration to run the benchmark (e.g., `5m`, `1h`;
  default: `5m`).
- `--grpc-channels`: The number of gRPC channels to use (default: `1`).
- `--max-outstanding-messages`: The maximum number of unacknowledged messages
  held in memory (default: `100000`).

### Publisher Options

- `--topic-id`: The ID of the Pub/Sub topic to publish to (Required).
- `--payload-size`: The size of each message payload in bytes (default: `1024`).
- `--batch-size`: The maximum number of messages in a batch (default: `1000`).
- `--batch-bytes`: The maximum size of a batch in bytes (default: `10485760`
  which is 10 MB).
- `--batch-delay`: The maximum time to wait before sending a batch (default:
  `100ms`).

### Subscriber Options

- `--subscription-id`: The ID of the Pub/Sub subscription to receive from
  (Required).
- `--streams`: The number of subscriber streams to run (default: `1`).

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
