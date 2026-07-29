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

//! Appendable object benchmark binary.

#[cfg(google_cloud_unstable_storage_bidi)]
#[path = "."]
mod app {
    mod args;
    mod metrics;
    mod reporter;
    mod scenarios;
    mod source;

    use args::Args;
    use clap::Parser;
    use google_cloud_storage::client::Storage;
    use uuid::Uuid;

    pub async fn run() -> anyhow::Result<()> {
        tracing_subscriber::fmt::init();
        let args = Args::parse();
        if args.measured_iterations == 0 {
            anyhow::bail!("Measured iterations must be greater than 0");
        }
        if args.measured_iterations < 100 {
            eprintln!(
                "WARNING: Running with fewer than 100 measured iterations ({}); percentiles (P50/P90/P99) may lack statistical significance.",
                args.measured_iterations
            );
        }

        let credentials = google_cloud_auth::credentials::Builder::default().build()?;
        let client = Storage::builder()
            .with_credentials(credentials)
            .build()
            .await?;

        run_scenario(&client, &args).await
    }

    async fn run_scenario(client: &Storage, args: &Args) -> anyhow::Result<()> {
        println!("Running Scenario 1: Steady-state append");
        println!(
            "Object Size: {} bytes, Chunk Size: {} bytes",
            args.object_size, args.chunk_size
        );
        println!(
            "Warmup iterations: {}, Measured iterations: {}",
            args.warmup_iterations, args.measured_iterations
        );

        let mut latencies = Vec::new();
        let mut errors = 0;
        let total_iterations = args.warmup_iterations + args.measured_iterations;

        let formatted_bucket = format!("projects/_/buckets/{}", args.bucket_name);
        for i in 0..total_iterations {
            let object_name = format!("benchmark-appendable-object-{}", Uuid::new_v4());
            match scenarios::scenario_1_basic_steady_state(
                client,
                &formatted_bucket,
                &object_name,
                args.object_size,
                args.chunk_size,
            )
            .await
            {
                Ok(elapsed) => {
                    if i < args.warmup_iterations {
                        println!("Warmup {:>2}: {:?}", i + 1, elapsed);
                    } else {
                        println!(
                            "Measured {:>2}: {:?}",
                            i - args.warmup_iterations + 1,
                            elapsed
                        );
                        latencies.push(elapsed);
                    }
                }
                Err(err) => {
                    eprintln!("Error during iteration {}: {err:#}", i + 1);
                    errors += 1;
                }
            }
        }

        let metrics = metrics::compute_metrics(&latencies);
        reporter::report(metrics, &latencies, errors, args)?;

        Ok(())
    }
}

#[cfg(google_cloud_unstable_storage_bidi)]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    app::run().await
}

#[cfg(not(google_cloud_unstable_storage_bidi))]
fn main() {
    println!("This benchmark requires the 'google_cloud_unstable_storage_bidi' cfg flag.");
}
