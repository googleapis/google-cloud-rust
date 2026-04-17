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

// [START rust_observability_logging] ANCHOR: rust_observability_logging
use google_cloud_secretmanager_v1::client::SecretManagerService;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

pub async fn sample() -> anyhow::Result<()> {
    // Output `WARN` logs for failed logical client requests, and `DEBUG` logs
    // for failed low-level RPC attempts from the client library crate.
    let filter = tracing_subscriber::EnvFilter::new("warn,google_cloud_secretmanager=debug");

    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().json())
        .init();

    let _client = SecretManagerService::builder()
        .with_tracing()
        .build()
        .await?;

    Ok(())
}
// [END rust_observability_logging] ANCHOR_END: rust_observability_logging
