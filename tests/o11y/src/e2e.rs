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

pub mod metrics;
pub mod resource_detector;
pub mod showcase;
pub mod signals;
pub mod storage;

use crate::e2e::resource_detector::TestResourceDetector;
use crate::otlp::logs::EventFormatter;
use std::io::Write;
use std::sync::{Arc, Mutex};
use tracing_subscriber::Layer;

use super::otlp::metrics::Builder as MeterProviderBuilder;
use super::otlp::trace::Builder as TracerProviderBuilder;
use google_cloud_auth::credentials::{Builder as CredentialsBuilder, Credentials};
use google_cloud_gax::error::rpc::Code;
use google_cloud_monitoring_v3::client::MetricService;
use google_cloud_monitoring_v3::model::{ListTimeSeriesResponse, TimeInterval};
use google_cloud_trace_v1::client::TraceService;
use google_cloud_trace_v1::model::Trace;
use google_cloud_wkt::Timestamp;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::trace::SdkTracerProvider;
use std::sync::OnceLock;
use std::time::{Duration, SystemTime};
use tracing::level_filters::LevelFilter;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::layer::SubscriberExt;
use uuid::Uuid;

pub const SERVICE_NAME: &str = "e2e-telemetry-test";
static PROVIDERS_SET: OnceLock<Uuid> = OnceLock::new();

/// Waits for a trace to appear in Cloud Trace.
///
/// Traces may take a few minutes to propagate from the collector endpoints to
/// the service. This function retrieves the trace, polling if the trace is
/// not found.
pub async fn wait_for_trace(
    project_id: &str,
    trace_id: &str,
    required_spans: &std::collections::BTreeSet<&str>,
) -> anyhow::Result<Trace> {
    let client = TraceService::builder().build().await?;

    // Because we are limited by quota, start with a backoff.
    // Traces can take several minutes to propagate after they have been written.
    // Implement a generous retry loop to account for this.
    let backoff_delays = [10, 60, 120, 120, 120];
    let mut trace = None;

    for delay in backoff_delays {
        tokio::time::sleep(std::time::Duration::from_secs(delay)).await;

        match client
            .get_trace()
            .set_project_id(project_id)
            .set_trace_id(trace_id)
            .send()
            .await
        {
            Ok(t) => {
                let span_names = t
                    .spans
                    .iter()
                    .map(|s| s.name.as_str())
                    .collect::<std::collections::BTreeSet<_>>();
                let missing = required_spans.difference(&span_names).collect::<Vec<_>>();
                if missing.is_empty() {
                    trace = Some(t);
                    break;
                } else {
                    println!(
                        "Trace found but is missing {} required spans: {:?}",
                        missing.len(),
                        missing
                    );
                }
            }
            Err(e) => {
                if let Some(status) = e.status() {
                    if status.code == Code::NotFound || status.code == Code::Internal {
                        println!(
                            "Trace not found yet (or internal error), retrying... Error: {:?}",
                            e
                        );
                        continue;
                    }
                }
                return Err(e.into());
            }
        }
    }

    let trace = trace.ok_or_else(|| anyhow::anyhow!("Timed out waiting for trace"))?;
    Ok(trace)
}

pub async fn try_get_metric(
    client: &MetricService,
    project_id: &str,
    metric_name: &str,
    label: (&str, &str),
) -> anyhow::Result<Option<ListTimeSeriesResponse>> {
    let end = Timestamp::try_from(SystemTime::now())?;
    let start = Timestamp::try_from(SystemTime::now() - Duration::from_secs(300))?;
    let (key, value) = label;
    let response = client
        .list_time_series()
        .set_name(format!("projects/{project_id}"))
        .set_interval(TimeInterval::new().set_end_time(end).set_start_time(start))
        .set_filter(format!(
            r#"metric.type = "{metric_name}" AND metric.label.{key} = "{value}""#
        ))
        .send()
        .await?;
    Ok(Some(response))
}

pub async fn set_up_providers(
    project_id: &str,
    service_name: &'static str,
    test_id: String,
    credentials: Credentials,
) -> anyhow::Result<(SdkTracerProvider, SdkMeterProvider, Buffer)> {
    let id = Uuid::new_v4();
    if PROVIDERS_SET.get_or_init(|| id) != &id {
        // This function necessarily uses `tracing::global`. Running more than one test per
        // process introduces flakes. Even if we use some kind of "Once*" to initialize the
        // globals, we still need to call `flush_provider()` and that fails if called too often.
        panic!("Only one test per process can use `set_up_providers()`.");
    }
    let detector = TestResourceDetector::new(test_id.to_string().as_str());
    let tracer_provider = TracerProviderBuilder::new(project_id, service_name)
        .with_credentials(credentials.clone())
        .with_detector(detector.clone())
        .build()
        .await?;
    let meter_provider = MeterProviderBuilder::new(project_id, service_name)
        .with_credentials(credentials.clone())
        .with_detector(detector.clone())
        .build()
        .await?;
    let formatter = EventFormatter::new(project_id);
    // Creates a buffer to capture the error messages.
    // - `tracing_subscriber::fmt::layer().with_writer()` requests `MakeWriter`.
    // - Any function that returns a `std::io::Writer` is a `MakeWriter`.
    // - `Buffer` implements the `std::io::Write` trait.
    let buffer = Buffer::default();
    let make_writer = {
        let shared = buffer.clone();
        move || shared.clone()
    };
    tracing::subscriber::set_global_default(
        tracing_subscriber::Registry::default()
            .with(
                tracing_subscriber::fmt::layer()
                    .with_span_events(FmtSpan::NONE)
                    .with_level(true)
                    .with_thread_ids(true)
                    .with_writer(make_writer)
                    .event_format(formatter.clone())
                    .with_filter(LevelFilter::WARN),
            )
            .with(crate::tracing::trace_layer(tracer_provider.clone())),
    )?;
    opentelemetry::global::set_tracer_provider(tracer_provider.clone());
    opentelemetry::global::set_meter_provider(meter_provider.clone());
    Ok((tracer_provider, meter_provider, buffer))
}

#[derive(Clone, Debug, Default)]
pub struct Buffer(Arc<Mutex<Vec<u8>>>);

impl Buffer {
    pub fn captured(&self) -> Vec<u8> {
        let guard = self.0.lock().expect("never poisoned");
        guard.clone()
    }
}

impl Write for Buffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut guard = self.0.lock().expect("never poisoned");
        guard.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

async fn new_credentials(project_id: &str) -> anyhow::Result<Credentials> {
    let credentials = CredentialsBuilder::default().build()?;
    let credentials = if format!("{credentials:?}").contains("UserCredentials") {
        CredentialsBuilder::default()
            .with_quota_project_id(project_id)
            .build()?
    } else {
        credentials
    };
    Ok(credentials)
}
