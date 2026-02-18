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

pub mod showcase;

use super::otlp::CloudTelemetryTracerProviderBuilder;
use google_cloud_auth::credentials::Builder as CredentialsBuilder;
use google_cloud_gax::error::rpc::Code;
use google_cloud_trace_v1::client::TraceService;
use google_cloud_trace_v1::model::Trace;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tokio::sync::OnceCell;
use tracing_subscriber::layer::SubscriberExt;

pub const SERVICE_NAME: &str = "e2e-telemetry-test";
static PROVIDER: OnceCell<anyhow::Result<SdkTracerProvider>> = OnceCell::const_new();

/// Waits for a trace to appear in Cloud Trace.
///
/// Traces may take a few minutes to propagate from the collector endpoints to
/// the service. This function retrieves the trace, polling if the trace is
/// not found.
pub async fn wait_for_trace(project_id: &str, trace_id: &str) -> anyhow::Result<Trace> {
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
                trace = Some(t);
                break;
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

/// Sets up a OTLP tracing provider to use with Cloud Trace.
///
/// This function configures a global OpenTelemetry provider that sends traces
/// to Cloud Trace via the OTLP endpoint (telemetry.googleapis.com). Only the
/// first call creates a provider. All the tests will use the same provider.
pub async fn set_up_otel_provider(project_id: &str) -> anyhow::Result<&SdkTracerProvider> {
    PROVIDER
        .get_or_init(|| self::new_provider(project_id))
        .await
        // `get_or_init()` returns a `&Result<T>` so we need some mapping.
        .as_ref()
        // Cannot clone anyhow::Error, so do this instead:
        .map_err(|e| anyhow::anyhow!("badly initialized provider: {e:?}"))
}

/// Creates a new provider for the tests.
///
/// This uses ADC, and configure a quota project for user credentials because
/// telemetry endpoint rejects user credentials without the quota user project.
///
/// Note that some other services reject requests *with* a quota user project.
/// Therefore, we cannot require that the credentials have a quota user prorject
/// set.
async fn new_provider(project_id: &str) -> anyhow::Result<SdkTracerProvider> {
    let credentials = CredentialsBuilder::default().build()?;
    let credentials = if format!("{credentials:?}").contains("UserCredentials") {
        CredentialsBuilder::default()
            .with_quota_project_id(project_id)
            .build()?
    } else {
        credentials
    };
    let provider = CloudTelemetryTracerProviderBuilder::new(project_id, SERVICE_NAME)
        .with_credentials(credentials)
        .build()
        .await?;

    // Install subscriber, ignore any other subscriber already installed.
    let _ = tracing::subscriber::set_global_default(
        tracing_subscriber::Registry::default().with(super::tracing::layer(provider.clone())),
    );

    Ok(provider)
}
