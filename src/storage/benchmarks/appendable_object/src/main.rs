mod args;
mod metrics;
mod reporter;
mod scenarios;
mod source;

use args::Args;
use clap::Parser;
use google_cloud_storage::client::Storage;
use uuid::Uuid;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    if args.measured_iterations < 100 {
        anyhow::bail!("Minimum number of measured iterations is 100");
    }

    let credentials = google_cloud_auth::credentials::Builder::default().build()?;
    let client = Storage::builder()
        .with_credentials(credentials)
        .build()
        .await?;

    run_scenario(&client, &args).await?;

    Ok(())
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
        let object_name = format!("bench-append-{}", Uuid::new_v4());
        let result = scenarios::scenario_1_basic_steady_state(
            client,
            &formatted_bucket,
            &object_name,
            args.object_size,
            args.chunk_size,
        )
        .await;

        match result {
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
            Err(e) => {
                eprintln!("Error during iteration {}: {:?}", i + 1, e);
                errors += 1;
            }
        }
    }

    let metrics = metrics::compute_metrics(latencies.clone());
    reporter::report(metrics, &latencies, errors, args)?;
    Ok(())
}
