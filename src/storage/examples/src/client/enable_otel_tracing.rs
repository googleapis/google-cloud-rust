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

// [START storage_enable_otel_tracing]
use google_cloud_storage::client::Storage;
use opentelemetry::trace::TracerProvider as _;
use tracing_subscriber::Registry;
use tracing_subscriber::layer::SubscriberExt;

pub async fn sample(bucket_id: &str) -> anyhow::Result<()> {
    const NAME: &str = "enable-otel-tracing.txt";

    // Create an OTLP exporter to send spans to an OpenTelemetry Collector
    // running on localhost.
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .build()?;
    let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .build();
    let tracer = provider.tracer("storage-quickstart");

    // Create a tracing layer and register the subscriber for the current scope.
    // In an application's `main`, use `tracing::subscriber::set_global_default` instead.
    let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
    let subscriber = Registry::default().with(telemetry);
    let _guard = tracing::subscriber::set_default(subscriber);

    // Build a Storage client with client-side tracing enabled.
    let client = Storage::builder().with_tracing().build().await?;

    let object = client
        .write_object(
            format!("projects/_/buckets/{bucket_id}"),
            NAME,
            "Hello, world!",
        )
        .set_if_generation_match(0)
        .send_unbuffered()
        .await?;
    println!("Uploaded object {} to bucket {bucket_id}.", object.name);

    let mut reader = client
        .read_object(format!("projects/_/buckets/{bucket_id}"), NAME)
        .send()
        .await?;
    let mut contents = Vec::new();
    while let Some(chunk) = reader.next().await.transpose()? {
        contents.extend_from_slice(&chunk);
    }
    println!(
        "Downloaded {} bytes from object {NAME} in bucket {bucket_id}.",
        contents.len()
    );

    Ok(())
}
// [END storage_enable_otel_tracing]
