# Cloud Pub/Sub Publish Hedging Benchmark

This benchmark evaluates the latency distribution of Cloud Pub/Sub message
publishing with and without request hedging against an in-process simulated tail
latency server.

Request hedging automatically sends duplicate/hedged publish requests when an
in-flight publish exceeds a configured delay threshold (and tokens are available
in the hedging token bucket). This mitigates tail latency (p99/p99.9) caused by
network degradations or slow backend servers.

## Usage

```bash
cargo run --release -p pubsub-hedging -- [OPTIONS]
```

To see the options use:

```bash
cargo run -p pubsub-hedging -- --help
```
