// Copyright 2024 Google LLC
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

use auth::credentials::Builder;
use http::Extensions;
use opentelemetry::{trace::{TracerProvider as _}, KeyValue};
use opentelemetry_sdk::{trace as sdktrace, Resource};
use opentelemetry_otlp::WithExportConfig;
use std::collections::HashMap;
use std::env;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Registry;
use std::sync::Once;
use tracing::Instrument;

static INIT: Once = Once::new();

#[tokio::test]
#[cfg(all(google_cloud_unstable_tracing, feature = "run-integration-tests"))]
async fn test_e2e_http_spans() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    INIT.call_once(|| {
        // Ignore error if already set
        let _ = opentelemetry::global::set_error_handler(|err| {
            eprintln!("OpenTelemetry Error: {:?}", err);
        });
    });

    let project_id = env::var("PROJECT_ID").expect("PROJECT_ID must be set for this test");

    // 1. Setup OTel
    let scopes = ["https://www.googleapis.com/auth/cloud-platform"];
    let creds = Builder::default().with_scopes(scopes).build()?;
    let header_map_resource = creds.headers(Extensions::new()).await?;
    let header_map = match header_map_resource {
        auth::credentials::CacheableResource::New { data, .. } => data,
        _ => panic!("Unexpected CacheableResource::NotModified"),
    };
    let auth_header_val = header_map.get(http::header::AUTHORIZATION)
        .expect("Missing Authorization header")
        .to_str()?
        .to_string();

    let mut headers = HashMap::new();
    headers.insert("Authorization".to_string(), auth_header_val);
    headers.insert("x-goog-user-project".to_string(), project_id.clone());

    let exporter = opentelemetry_otlp::new_exporter()
        .http()
        .with_endpoint("https://telemetry.googleapis.com/v1/traces")
        .with_headers(headers);

    let resource = Resource::new(vec![
        KeyValue::new("service.name", "e2e-test-rust"),
        KeyValue::new("gcp.project_id", project_id.clone()),
    ]);

    let tracer_provider = sdktrace::TracerProvider::builder()
        .with_config(sdktrace::Config::default().with_resource(resource))
        .with_batch_exporter(exporter.build_span_exporter()?, opentelemetry_sdk::runtime::Tokio)
        .build();

    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer_provider.tracer("e2e-test"));
    let subscriber = Registry::default().with(otel_layer);

    // Use global default to ensure all threads pick it up.
    let _ = tracing::subscriber::set_global_default(subscriber);

    // 2. Execution
    let root = tracing::info_span!("e2e_test_root");
    async {
        // Use Secret Manager to make a real GCP call
        let client = sm::client::SecretManagerService::builder()
            .with_tracing()
            .build().await?;
        let parent = format!("projects/{}", project_id);
        println!("Listing secrets for {}", parent);
        match client.list_secrets().set_parent(parent).send().await {
            Ok(_) => println!("ListSecrets succeeded"),
            Err(e) => println!("ListSecrets failed (expected if no permissions, but span should still be generated): {:?}", e),
        }
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    }
    .instrument(root)
    .await?;

    // 3. Teardown
    println!("Calling tracer_provider.shutdown() to flush spans...");
    
    // Wrap blocking shutdown in a timeout
    let shutdown_result = tokio::time::timeout(std::time::Duration::from_secs(5), tokio::task::spawn_blocking(move || {
        tracer_provider.shutdown()
    })).await;

    match shutdown_result {
        Ok(Ok(Ok(_))) => println!("tracer_provider.shutdown() completed successfully."),
        Ok(Ok(Err(e))) => println!("tracer_provider.shutdown() failed with error: {:?}", e),
        Ok(Err(join_err)) => println!("tracer_provider.shutdown() task failed to join: {:?}", join_err),
        Err(_) => println!("tracer_provider.shutdown() timed out! Spans may not have been flushed."),
    }

    println!("Please verify in Cloud Console for project {}", project_id);

    Ok(())
}
