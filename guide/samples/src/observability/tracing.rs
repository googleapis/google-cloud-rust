// Copyright 2025 Google LLC
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

// [START rust_observability_tracing] ANCHOR: rust_observability_tracing
use google_cloud_secretmanager_v1::client::SecretManagerService;
use tracing_subscriber::Registry;
use tracing_subscriber::layer::SubscriberExt;

pub async fn sample() -> anyhow::Result<()> {
    use opentelemetry::trace::TracerProvider as _;
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .build()?;
    let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .build();
    let tracer = provider.tracer("example");

    // Create a tracing layer that sends data to an OpenTelemetry Collector running on localhost.
    let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);

    // Register the subscriber globally
    let subscriber = Registry::default().with(telemetry);
    tracing::subscriber::set_global_default(subscriber)?;

    let _client = SecretManagerService::builder()
        .with_tracing()
        .build()
        .await?;

    Ok(())
}
// [END rust_observability_tracing] ANCHOR_END: rust_observability_tracing
