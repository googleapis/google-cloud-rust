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

use crate::args::Args;
use google_cloud_auth::credentials::Credentials;
use integration_tests_o11y::detector::GoogleCloudResourceDetector;
use integration_tests_o11y::otlp::Uri;
use opentelemetry::KeyValue;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::resource::ResourceDetector;
use opentelemetry_sdk::trace::SdkTracerProvider;
use std::str::FromStr;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::prelude::*;
use uuid::Uuid;

const SERVICE_NAME: &str = "bigquery-benchmark-queries";

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
            namespace: "bigquery-benchmark-queries".to_string(),
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

/// Holds providers that need graceful flush and shutdown upon completion.
pub struct TelemetryGuard {
    tracer_provider: Option<SdkTracerProvider>,
    meter_provider: Option<SdkMeterProvider>,
}

impl TelemetryGuard {
    /// Flushes and shuts down telemetry providers.
    pub fn shutdown(self) {
        if let Some(tp) = self.tracer_provider {
            let _ = tp.force_flush();
            if let Err(e) = tp.shutdown() {
                eprintln!("Error shutting down trace provider: {e:?}");
            }
        }
        if let Some(mp) = self.meter_provider {
            let _ = mp.force_flush();
            if let Err(e) = mp.shutdown() {
                eprintln!("Error shutting down meter provider: {e:?}");
            }
        }
    }
}

/// Initializes tracing subscriber, OpenTelemetry distributed tracing, and metrics export.
pub async fn enable_telemetry(
    args: &Args,
    credentials: &Credentials,
) -> anyhow::Result<TelemetryGuard> {
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_level(true)
        .with_thread_ids(true)
        .with_span_events(FmtSpan::NONE)
        .with_writer(std::io::stderr)
        .with_filter(env_filter);

    let error_file_layer = if let Some(output_dir) = &args.output_dir {
        let _ = std::fs::create_dir_all(output_dir);
        let error_log_path = output_dir.join("tracing-errors.log");
        match std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&error_log_path)
        {
            Ok(file) => {
                let layer = tracing_subscriber::fmt::layer()
                    .with_level(true)
                    .with_thread_ids(true)
                    .with_ansi(false)
                    .with_writer(std::sync::Mutex::new(file))
                    .with_filter(tracing_subscriber::filter::LevelFilter::ERROR);
                Some(layer)
            }
            Err(e) => {
                eprintln!("Could not open tracing error log file: {e:?}");
                None
            }
        }
    } else {
        None
    };

    let registry = tracing_subscriber::Registry::default()
        .with(fmt_layer)
        .with(error_file_layer);

    if let Some(project_id) = &args.project_id {
        tracing::info!("Enabling OpenTelemetry Cloud Trace & Monitoring for project {project_id}");

        let node = GenericNodeDetector::new();
        let detector = GoogleCloudResourceDetector::builder()
            .with_fallback(node.detect())
            .build()
            .await?;

        let mut trace_builder =
            integration_tests_o11y::otlp::trace::Builder::new(project_id, SERVICE_NAME)
                .with_credentials(credentials.clone())
                .with_detector(detector);

        let mut meter_builder =
            integration_tests_o11y::otlp::metrics::Builder::new(project_id, SERVICE_NAME)
                .with_credentials(credentials.clone())
                .with_detector(node);

        if let Some(endpoint_str) = &args.otlp_endpoint {
            let uri = Uri::from_str(endpoint_str)?;
            trace_builder = trace_builder.with_endpoint(endpoint_str.clone());
            meter_builder = meter_builder.with_endpoint(uri);
        }

        let tracer_provider = trace_builder
            .build()
            .await
            .inspect_err(|e| eprintln!("Failed to create tracer provider: {e:?}"))?;

        let meter_provider = meter_builder
            .build()
            .await
            .inspect_err(|e| eprintln!("Failed to create meter provider: {e:?}"))?;

        opentelemetry::global::set_meter_provider(meter_provider.clone());

        let otel_layer = integration_tests_o11y::tracing::trace_layer(tracer_provider.clone());
        tracing::subscriber::set_global_default(registry.with(otel_layer))
            .expect("Setting global subscriber succeeds");

        return Ok(TelemetryGuard {
            tracer_provider: Some(tracer_provider),
            meter_provider: Some(meter_provider),
        });
    }

    tracing::subscriber::set_global_default(registry).expect("Setting global subscriber succeeds");

    Ok(TelemetryGuard {
        tracer_provider: None,
        meter_provider: None,
    })
}
