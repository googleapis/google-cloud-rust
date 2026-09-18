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

## Benchmark Results

### Simulated Latency Profile

The in-process mock server introduces probabilistic latency degradations to simulate real-world tail latency and network anomalies:
- **Fast / Normal Responses (95% of requests):** 5ms latency
- **Degraded Responses (4% of requests):** 300ms latency
- **Tail Stalls / Outliers (1% of requests):** 4.0s latency

### Raw Benchmark Outputs
Here are sample results ran on September 17, 2026.

#### Without Hedging

```bash
cargo run --release -p pubsub-hedging
```

```text
================================================================================
             Starting Google Cloud Pub/Sub Hedging Benchmark                    
================================================================================
Parameters:
  Warmup:               5.00s
  Duration:             30.00s
  Message Rate:         200 msg/s
  Payload Size:         1024 bytes (unbatched: 1 msg/batch)
  Enable Hedging:       false
  Mock Server Endpoint: http://127.0.0.1:37489
================================================================================

Starting warmup phase for 5.00s...
Warmup complete. Starting benchmark...

Benchmark duration reached. Sent 6000 messages. Waiting for in-flight requests...

================================================================================
                         Pub/Sub Benchmark Results                              
================================================================================
Summary:
  Elapsed time:            33.93s
  Messages attempted:      6000
  Messages succeeded:      6000
  Errors:                  0
  Throughput:              176.82 msgs/s (0.18 MB/s)

Latency Distribution:
  Min:                     5.31ms
  p50 (Median):            6.69ms
  p90:                     6.94ms
  p95:                     10.30ms
  p99:                     306.05ms
  p99.9:                   4.01s
  p99.99:                  4.01s
  Max:                     4.01s
  Avg:                     55.77ms
  Top 10 slowest:          4.01s, 4.01s, 4.01s, 4.01s, 4.01s, 4.01s, 4.01s, 4.01s, 4.01s, 4.01s

Mock Server / RPC Hedging Stats:
  Total Publish RPCs:      6000
  Hedged Publish RPCs:     0 (0.00%)
  RPC Overhead from Hedge: +0.00% extra RPCs
================================================================================
```

#### With Hedging Enabled

```sh
cargo run --release -p pubsub-hedging -- --enable-hedging
```

```text
================================================================================
             Starting Google Cloud Pub/Sub Hedging Benchmark                    
================================================================================
Parameters:
  Warmup:               5.00s
  Duration:             30.00s
  Message Rate:         200 msg/s
  Payload Size:         1024 bytes (unbatched: 1 msg/batch)
  Enable Hedging:       true
  Hedge Delay:          100.00ms
  Hedge Max Tokens:     50
  Hedge Refill Ratio:   0.1
  Mock Server Endpoint: http://127.0.0.1:42565
================================================================================

Starting warmup phase for 5.00s...
Warmup complete. Starting benchmark...

Benchmark duration reached. Sent 6000 messages. Waiting for in-flight requests...

================================================================================
                         Pub/Sub Benchmark Results                              
================================================================================
Summary:
  Elapsed time:            30.04s
  Messages attempted:      6000
  Messages succeeded:      6000
  Errors:                  0
  Throughput:              199.73 msgs/s (0.20 MB/s)

Latency Distribution:
  Min:                     5.41ms
  p50 (Median):            6.71ms
  p90:                     7.04ms
  p95:                     10.24ms
  p99:                     110.54ms
  p99.9:                   209.60ms
  p99.99:                  211.05ms
  Max:                     211.05ms
  Avg:                     11.45ms
  Top 10 slowest:          206.48ms, 207.68ms, 208.12ms, 209.60ms, 209.87ms, 210.07ms, 210.33ms, 210.54ms, 210.76ms, 211.05ms

Mock Server / RPC Hedging Stats:
  Total Publish RPCs:      6293
  Hedged Publish RPCs:     293 (4.66%)
  RPC Overhead from Hedge: +4.88% extra RPCs
================================================================================
```
