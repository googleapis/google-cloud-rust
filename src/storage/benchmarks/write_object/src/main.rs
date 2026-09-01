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

//! `write_object` benchmark binary.

mod args;
mod metrics;
mod reporter;
mod scenarios;
mod source;

use args::{Args, UploadScenario};
use clap::Parser;
use google_cloud_storage::client::{Storage, StorageControl};
use std::path::Path;
use uuid::Uuid;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    if args.measured_iterations == 0 {
        anyhow::bail!("Measured iterations must be greater than 0");
    }

    let credentials = google_cloud_auth::credentials::Builder::default().build()?;
    let client = Storage::builder()
        .with_credentials(credentials.clone())
        .build()
        .await?;
    let control = StorageControl::builder()
        .with_credentials(credentials)
        .build()
        .await?;

    println!("============================================================");
    println!("GCS write_object Benchmark Suite");
    println!("Target Bucket:       {}", args.bucket_name);
    println!(
        "Object Size:         {} bytes ({:.2} MiB)",
        args.object_size,
        args.object_size as f64 / (1024.0 * 1024.0)
    );
    println!("Cold Cache Eviction: {}", args.cold_cache);
    println!("Temp Directory:      {}", args.temp_dir);
    println!("Output Directory:    {}", args.output_dir);
    println!("Measured Iterations: {}", args.measured_iterations);
    println!("============================================================");

    let formatted_bucket = format!("projects/_/buckets/{}", args.bucket_name);

    // Pre-flight check: 512 KiB global warmup to verify auth & prime TLS connection pool
    println!("\n[1/3] Running pre-flight warmup check (512 KiB payload)...");
    source::perform_global_warmup(&client, &control, &formatted_bucket).await?;
    println!("Pre-flight warmup check succeeded: Authentication verified & TLS pool primed.");

    // Generate local test file on physical SSD
    println!("\n[2/3] Generating local test file on physical SSD...");
    let (temp_handle, temp_file_path) =
        source::create_temp_test_file(args.object_size, &args.temp_dir).await?;
    println!("Test file created at: {}", temp_file_path.display());

    // Execute upload benchmark scenarios
    println!("\n[3/3] Executing benchmark scenarios...");
    match args.scenario {
        UploadScenario::OptionA => {
            run_single_scenario(
                &client,
                &control,
                &formatted_bucket,
                &temp_file_path,
                &args,
                UploadScenario::OptionA,
            )
            .await?;
        }
        UploadScenario::OptionB => {
            run_single_scenario(
                &client,
                &control,
                &formatted_bucket,
                &temp_file_path,
                &args,
                UploadScenario::OptionB,
            )
            .await?;
        }
        UploadScenario::OptionC => {
            run_single_scenario(
                &client,
                &control,
                &formatted_bucket,
                &temp_file_path,
                &args,
                UploadScenario::OptionC,
            )
            .await?;
        }
        UploadScenario::All => {
            run_single_scenario(
                &client,
                &control,
                &formatted_bucket,
                &temp_file_path,
                &args,
                UploadScenario::OptionA,
            )
            .await?;

            run_single_scenario(
                &client,
                &control,
                &formatted_bucket,
                &temp_file_path,
                &args,
                UploadScenario::OptionB,
            )
            .await?;

            run_single_scenario(
                &client,
                &control,
                &formatted_bucket,
                &temp_file_path,
                &args,
                UploadScenario::OptionC,
            )
            .await?;
        }
    }

    // Clean up local physical disk file
    println!(
        "\nCleaning up local test file on disk: {}",
        temp_file_path.display()
    );
    drop(temp_handle);
    println!("Local test file successfully deleted.");

    Ok(())
}

async fn run_single_scenario(
    client: &Storage,
    control: &StorageControl,
    bucket: &str,
    file_path: &Path,
    args: &Args,
    scenario: UploadScenario,
) -> anyhow::Result<()> {
    let scenario_name = match scenario {
        UploadScenario::OptionA => "Option_A_Unbuffered_Baseline",
        UploadScenario::OptionB => "Option_B_Unbuffered_2Pass",
        UploadScenario::OptionC => "Option_C_Buffered_Chunked",
        UploadScenario::All => unreachable!(),
    };

    println!("\n>>> Running Scenario: {} <<<", scenario_name);
    let mut results = Vec::new();
    let mut errors = 0;

    for i in 0..args.measured_iterations {
        // If cold_cache is enabled, evict the test file from OS page cache once before the
        // iteration starts to ensure a cold physical disk read.
        if args.cold_cache
            && let Err(e) = source::drop_file_from_page_cache(file_path)
        {
            eprintln!("Warning: Failed to drop file from page cache: {e}");
        }

        let object_name = format!("bench-write-object-{}-{}", scenario_name, Uuid::new_v4());
        let res = match scenario {
            UploadScenario::OptionA => {
                scenarios::scenario_option_a(
                    client,
                    bucket,
                    &object_name,
                    file_path,
                    args.object_size,
                )
                .await
            }
            UploadScenario::OptionB => {
                scenarios::scenario_option_b(
                    client,
                    bucket,
                    &object_name,
                    file_path,
                    args.object_size,
                )
                .await
            }
            UploadScenario::OptionC => {
                scenarios::scenario_option_c(
                    client,
                    bucket,
                    &object_name,
                    file_path,
                    args.object_size,
                )
                .await
            }
            UploadScenario::All => unreachable!(),
        };

        match res {
            Ok(r) => {
                println!(
                    "Measured {:>2}: {:?}{}",
                    i + 1,
                    r.total_elapsed,
                    r.precompute_duration
                        .map(|d| format!(" (Pass 1 Hash: {:?})", d))
                        .unwrap_or_default()
                );
                results.push(r);
            }
            Err(err) => {
                eprintln!("Error during iteration {}: {err:#}", i + 1);
                errors += 1;
            }
        }

        if args.cleanup {
            let _ = scenarios::cleanup_object(control, bucket, &object_name).await;
        }
    }

    let latencies: Vec<_> = results.iter().map(|r| r.total_elapsed).collect();
    let metrics = metrics::compute_metrics(&latencies, args.object_size);
    reporter::report(scenario_name, metrics, &results, errors, args)?;

    Ok(())
}
