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
    mod scenarios;
    mod source;

    use args::Args;
    use clap::Parser;
    use google_cloud_storage::client::Storage;

    pub async fn run() -> anyhow::Result<()> {
        let args = Args::parse();
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

        let formatted_bucket = format!("projects/_/buckets/{}", args.bucket_name);
        let object_name = "bench-append-single";
        match scenarios::scenario_1_basic_steady_state(
            client,
            &formatted_bucket,
            object_name,
            args.object_size,
            args.chunk_size,
        )
        .await
        {
            Ok(elapsed) => {
                println!("Single run elapsed time: {:?}", elapsed);
                Ok(())
            }
            Err(err) => {
                eprintln!("Scenario 1 failed: {err:#}");
                Err(err)
            }
        }
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
