// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

mod args;
mod publisher;
mod subscriber;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let config = crate::args::parse_args();
    /// The CSV header used for all throughput benchmark results.
    const CSV_HEADER: &str =
        "timestamp,elapsed(s),op,iteration,count,msgs/s,bytes,MB/s,errors,errors/s";

    match config.command {
        crate::args::Commands::Publisher(args) => {
            println!("# Running publish benchmark with config: {:?}", args);
            println!("{}", CSV_HEADER);
            publisher::run(args).await?;
        }
        crate::args::Commands::Subscriber(args) => {
            println!("# Running subscribe benchmark with config: {:?}", args);
            println!("{}", CSV_HEADER);
            subscriber::run(args).await?;
        }
    }

    Ok(())
}

/// Returns true if the benchmark has exceeded its maximum allowed runtime.
pub(crate) fn done(maximum_runtime: Duration, start: Instant) -> bool {
    start.elapsed() >= maximum_runtime
}

/// Returns the current Unix timestamp in milliseconds.
pub(crate) fn timestamp() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
}

/// Formats and prints a single measurement row to stdout in CSV format.
///
/// The output includes throughput metrics (msgs/s and MB/s) calculated based
/// on the elapsed time and the amount of data processed.
pub(crate) fn print_result(
    operation: &str,
    iteration: i64,
    count: i64,
    bytes: i64,
    errors: i64,
    elapsed: Duration,
) {
    let elapsed_s = elapsed.as_secs_f64();
    let mbs = (bytes as f64) / elapsed_s / 1_000_000.0;
    let msgs = (count as f64) / elapsed_s;
    let errs = (errors as f64) / elapsed_s;
    println!(
        "{},{},{},{},{},{:.2},{},{:.2},{},{:.2}",
        timestamp(),
        elapsed_s,
        operation,
        iteration,
        count,
        msgs,
        bytes,
        mbs,
        errors,
        errs
    );
}
