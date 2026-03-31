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

use super::{new_credentials, set_up_providers, wait_for_trace};
use crate::Anonymous;
use google_cloud_auth::credentials::Credentials;
use google_cloud_gax::retry_policy::NeverRetry;
use google_cloud_monitoring_v3::client::MetricService;
use google_cloud_monitoring_v3::model::TimeInterval;
use google_cloud_showcase_v1beta1::client::Echo;
use google_cloud_test_utils::runtime_config::project_id;
pub use google_cloud_test_utils::tracing::Buffer;
use google_cloud_wkt::Timestamp;
use httptest::responders::{delay_and_then, status_code};
use httptest::{Expectation, Server, matchers::*};
use opentelemetry::TraceId;
use opentelemetry::trace::TraceContextExt;
use pretty_assertions::assert_eq;
use rand::RngExt;
use std::time::SystemTime;
use std::{collections::BTreeSet, time::Duration};
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

const ROOT_SPAN_NAME: &str = "e2e-showcase-test";

pub async fn run() -> anyhow::Result<()> {
    let project_id = project_id()?;
    let id = Uuid::new_v4();
    let credentials = new_credentials(&project_id).await?;

    let (tracer_provider, meter_provider, buffer) = set_up_providers(
        &project_id,
        ROOT_SPAN_NAME,
        id.to_string(),
        credentials.clone(),
    )
    .await?;

    // Setup a Mock Server so the showcase client can work. In this test we do a fizzbuzz-type thing:
    // - About 3% of the calls fail with 404.
    // - Another 3% of the calls get a delay and then return 200.
    // - About 3% of 3% of the calls get a delay and then fail with 404.
    let echo_server = configure_mock_server();

    // Initialize showcase client pointing to local mock server.
    let client = Echo::builder()
        .with_endpoint(format!("http://{}", echo_server.addr()))
        .with_credentials(Anonymous::new().build())
        .with_retry_policy(NeverRetry)
        .with_tracing()
        .build()
        .await?;

    // Run multiple iterations where the test calls this mock server.
    let trace_id = run_test_iterations(&project_id, client).await?;

    // Force flush to ensure spans are sent.
    tracer_provider.force_flush()?;
    meter_provider.force_flush()?;

    // Verify the traces, logs and metrics match the expected values.
    check_traces(&project_id, trace_id).await?;
    check_logs(&project_id, buffer, trace_id)?;
    check_metrics(&project_id, id, credentials).await?;

    Ok(())
}

fn configure_mock_server() -> Server {
    let echo_server = Server::run();
    let respond = || {
        const DELAY: Duration = Duration::from_millis(50);
        match rand::rng().random_range(0..1000) {
            n if n % 29 == 0 && n % 31 == 0 => delay_and_then(DELAY, status_code(404)),
            n if n % 31 == 0 => {
                delay_and_then(DELAY, status_code(200).body(r#"{"content": "test"}"#))
            }
            n if n % 29 == 0 => delay_and_then(Duration::ZERO, status_code(404)),
            _ => delay_and_then(
                Duration::ZERO,
                status_code(200).body(r#"{"content": "test"}"#),
            ),
        }
    };
    echo_server.expect(
        Expectation::matching(all_of![
            request::method("POST"),
            request::path("/v1beta1/echo:echo"),
        ])
        .times(1..)
        .respond_with(respond),
    );
    echo_server
}

async fn run_test_iterations(project_id: &str, client: Echo) -> anyhow::Result<TraceId> {
    let mut trace_id = None;
    for _ in 0..2_000 {
        let root_span = tracing::info_span!("e2e_root", "otel.name" = ROOT_SPAN_NAME);
        let tid = root_span.context().span().span_context().trace_id();

        let result = client
            .echo()
            .set_content("test")
            .send()
            .instrument(root_span)
            .await;

        match (result, &trace_id) {
            (Err(_), None) => {
                tracing::info!(
                    "View generated trace in Console: https://console.cloud.google.com/traces/explorer;traceId={}?project={}",
                    tid.to_string(),
                    project_id
                );
                trace_id = Some(tid);
            }
            // Only error traces are interesting
            (Ok(_), _) => (),
            // Already have a trace
            (Err(_), Some(_)) => (),
        }
    }
    trace_id.ok_or_else(|| anyhow::anyhow!("no trace id created during test"))
}

async fn check_traces(project_id: &str, trace_id: TraceId) -> anyhow::Result<()> {
    const T3_SPAN_NAME: &str = "google_cloud_showcase_v1beta1::client::Echo::echo";
    const T4_SPAN_NAME: &str = "POST /v1beta1/echo:echo";
    let required = BTreeSet::from_iter([ROOT_SPAN_NAME, T3_SPAN_NAME, T4_SPAN_NAME]);
    let trace = wait_for_trace(project_id, &trace_id.to_string(), &required).await?;
    let span_names = trace
        .spans
        .iter()
        .map(|s| s.name.as_str())
        .collect::<BTreeSet<_>>();
    let missing = required.difference(&span_names).collect::<Vec<_>>();
    assert!(missing.is_empty(), "missing={missing:?}\n\n{trace:?}",);
    Ok(())
}

fn check_logs(project_id: &str, buffer: Buffer, trace_id: TraceId) -> anyhow::Result<()> {
    let contents = String::from_utf8(buffer.captured())?;
    let needle = format!("projects/{project_id}/traces/{trace_id}");
    let lines = contents.split('\n').collect::<Vec<_>>();
    let parsed = lines
        .iter()
        .filter(|s| !s.is_empty())
        .map(|line| serde_json::from_str::<serde_json::Value>(line))
        .collect::<Result<Vec<_>, _>>()
        .inspect_err(|e| println!("error parsing JSON log output: {e:?}\n{lines:?}"))?;
    let value = parsed
        .iter()
        .find(|v| {
            v.get("logging.googleapis.com/trace")
                .and_then(|v| v.as_str())
                .is_some_and(|v| needle == v)
        })
        .and_then(|v| v.as_object())
        .unwrap_or_else(|| panic!("missing trace in JSON logs: {lines:#?}"));
    // We will check the contents `fields` and `timestamp` later.
    let mut got = value.clone();
    let fields = got.remove("fields");
    let timestamp = got.remove("timestamp");
    // This needs to be exist and be a string, the contents are hard/impossible to verify.
    assert!(
        got.remove("logging.googleapis.com/spanId")
            .is_some_and(|v| v.as_str().is_some()),
        "{value:?}"
    );
    let want = serde_json::json!({
        "logging.googleapis.com/trace": needle,
        "logging.googleapis.com/trace_sampled": true,
        "severity": "WARN",
        "target": "experimental.client.request",
    });
    assert_eq!(Some(&got), want.as_object(), "{value:?}");

    let mut fields = fields
        .and_then(|v| v.as_object().cloned())
        .unwrap_or_else(|| panic!("fields should be an object: {value:?}"));
    // Just want to make sure it exists and is a string.
    assert!(
        fields
            .remove("message")
            .is_some_and(|v| v.as_str().is_some()),
        "{value:?}"
    );
    assert!(fields.remove("gcp.client.version").is_some(), "{value:?}");
    assert!(fields.remove("server.address").is_some(), "{value:?}");
    assert!(fields.remove("server.port").is_some(), "{value:?}");
    assert!(fields.remove("url.full").is_some(), "{value:?}");
    let want = serde_json::json!({
        "gcp.client.artifact": "google-cloud-showcase-v1beta1",
        "gcp.schema.url": "https://opentelemetry.io/schemas/1.39.0",
        "gcp.client.repo": "googleapis/google-cloud-rust",
        "gcp.client.service": "showcase",
        "error.type": "404",
        "http.request.method": "POST",
        "rpc.method": "google.showcase.v1beta1.Echo/Echo",
        "rpc.service": "showcase",
        "rpc.system.name": "http",
        "url.domain": "localhost:7469", // the showcase domain...
        "url.template": "/v1beta1/echo:echo",
    });
    assert_eq!(Some(&fields), want.as_object(), "{value:?}");

    let ts = timestamp
        .as_ref()
        .and_then(|v| v.as_str())
        .unwrap_or_else(|| panic!("timestamp field should be a string: {value:?}"));
    let ts = google_cloud_wkt::Timestamp::try_from(ts)
        .unwrap_or_else(|e| panic!("timestamp ({ts}) should be parseable as RFC 3339: {e:?}"));
    let deadline = google_cloud_wkt::Timestamp::try_from(
        std::time::SystemTime::now() - Duration::from_secs(600),
    )?;
    assert!(
        ts >= deadline,
        "timestamp ({ts:?}) should be >= now-600s ({deadline:?})"
    );
    Ok(())
}

async fn check_metrics(
    project_id: &str,
    test_id: Uuid,
    credentials: Credentials,
) -> anyhow::Result<()> {
    let metric_client = MetricService::builder()
        .with_credentials(credentials.clone())
        .build()
        .await?;
    let node_id = test_id.to_string();
    let mut found = None;
    for delay in [0, 10, 60, 120, 120, 120].map(Duration::from_secs) {
        tokio::time::sleep(delay).await;
        let end = Timestamp::try_from(SystemTime::now())?;
        let start = Timestamp::try_from(SystemTime::now() - Duration::from_secs(600))?;
        let response = metric_client
            .list_time_series()
            .set_name(format!("projects/{project_id}"))
            .set_interval(TimeInterval::new().set_end_time(end).set_start_time(start))
            .set_filter(
                format!(r#"metric.type = "workload.googleapis.com/test.client.duration" AND resource.labels.node_id = "{node_id}""#),
            )
            .set_order_by("timestamp desc")
            .send()
            .await
            .inspect_err(|e| tracing::info!("error fetching metric: {e:?}"));

        found = response.ok();
        if found.is_some() {
            break;
        }
    }

    let found = found.expect("metric not exported after timeout");
    let metrics = found
        .time_series
        .iter()
        .filter_map(|ts| ts.metric.as_ref())
        .collect::<Vec<_>>();

    let _code_404 = metrics
        .iter()
        .find(|m| {
            m.labels
                .get("http.response.status_code")
                .map(String::as_str)
                == Some("404")
        })
        .unwrap_or_else(|| panic!("missing metrics with 404 errors: {metrics:?}"));

    let _code_200 = metrics
        .iter()
        .find(|m| {
            m.labels
                .get("http.response.status_code")
                .map(String::as_str)
                == Some("200")
        })
        .unwrap_or_else(|| panic!("missing metrics with 200 status: {metrics:?}"));
    Ok(())
}
