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

use super::Args;
use super::logs::EventFormatter;
use google_cloud_auth::credentials::Credentials;
use integration_tests_o11y::detector::GoogleCloudResourceDetector;
use integration_tests_o11y::otlp::metrics::Builder as MeterProviderBuilder;
use integration_tests_o11y::otlp::trace::Builder as TracerProviderBuilder;
use integration_tests_o11y::tracing::trace_layer;
use opentelemetry::KeyValue;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::resource::ResourceDetector;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::{EnvFilter, Registry};
use uuid::Uuid;

/// Configure exporters for traces, logs, and metrics.
pub async fn exporters(args: &Args, credentials: Credentials) -> anyhow::Result<()> {
    use tracing_subscriber::prelude::*;

    let logging_layer = tracing_subscriber::fmt::layer()
        .with_span_events(FmtSpan::NONE)
        .with_level(true)
        .with_thread_ids(true)
        .event_format(EventFormatter::new(args.project_id.clone()));

    let node = GenericNodeDetector::new();
    let detector = GoogleCloudResourceDetector::builder()
        .with_fallback(node.detect())
        .build()
        .await?;
    if args.project_id.is_empty() || args.service_name.is_empty() {
        tracing::subscriber::set_global_default(
            Registry::default().with(logging_layer.with_filter(EnvFilter::from_default_env())),
        )?;
        tracing::warn!("observability disabled");
        return Ok(());
    }
    let project_id = &args.project_id;
    let service = &args.service_name;
    let tracer_provider = TracerProviderBuilder::new(project_id, service)
        .with_credentials(credentials.clone())
        .with_detector(detector.clone())
        .build()
        .await?;
    let meter_provider = MeterProviderBuilder::new(project_id, service)
        .with_credentials(credentials.clone())
        .with_detector(node.clone())
        .build()
        .await?;

    tracing::subscriber::set_global_default(
        Registry::default()
            .with(logging_layer.with_filter(EnvFilter::from_default_env()))
            .with(trace_layer(tracer_provider.clone())), // Also log to stdout,
    )?;
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
    opentelemetry::global::set_meter_provider(meter_provider.clone());

    tracing::info!("Detected resource: {:?}", detector.detect());
    tracing::info!("Detected node: {:?}", node.detect());
    Ok(())
}

#[derive(Clone, Debug)]
struct GenericNodeDetector {
    id: String,
    location: String,
    namespace: String,
}

impl GenericNodeDetector {
    pub fn new() -> Self {
        let id = Uuid::new_v4().to_string();
        Self {
            id,
            location: "us-central1".to_string(),
            namespace: "google-cloud-rust".to_string(),
        }
    }
}

impl ResourceDetector for GenericNodeDetector {
    fn detect(&self) -> Resource {
        Resource::builder_empty()
            .with_attributes([
                KeyValue::new("location", self.location.clone()),
                KeyValue::new("namespace", self.namespace.clone()),
                KeyValue::new("node_id", self.id.clone()),
            ])
            .build()
    }
}
