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

use super::resource_detector::TestResourceDetector;
use super::{set_up_meter_provider, set_up_tracer_provider, wait_for_trace};
use crate::Anonymous;
use google_cloud_gax::retry_policy::NeverRetry;
use google_cloud_showcase_v1beta1::client::Echo;
use google_cloud_test_utils::runtime_config::project_id;
use httptest::responders::{delay_and_then, status_code};
use httptest::{Expectation, Server, matchers::*};
use opentelemetry::trace::TraceContextExt;
use rand::RngExt;
use std::{collections::BTreeSet, time::Duration};
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

const ROOT_SPAN_NAME: &str = "e2e-showcase-test";

pub async fn run() -> anyhow::Result<()> {
    // tracing_subscriber::fmt::init();
    tracing::info!("start ...");
    // 1. Setup Mock Server (Traffic Destination)
    let echo_server = Server::run();
    let respond = || {
        const DELAY: Duration = Duration::from_millis(200);
        match rand::rng().random_range(0..1000) {
            n if n == 37 * 31 => delay_and_then(DELAY, status_code(404)),
            n if n % 37 == 0 => {
                delay_and_then(DELAY, status_code(200).body(r#"{"content": "test"}"#))
            }
            n if n % 31 == 0 => delay_and_then(Duration::ZERO, status_code(404)),
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

    // 2. Setup Telemetry (Real Google Cloud Destination)
    // This requires GOOGLE_CLOUD_PROJECT to be set.
    let project_id = project_id()?;
    let tracer_provider = set_up_tracer_provider(&project_id).await?;
    let meter_provider =
        set_up_meter_provider(&project_id, TestResourceDetector::new(&project_id)).await?;
    opentelemetry::global::set_meter_provider(meter_provider.clone());

    tracing::warn!("exporters created");
    // Initialize showcase client pointing to local mock server
    let client = Echo::builder()
        .with_endpoint(format!("http://{}", echo_server.addr()))
        .with_credentials(Anonymous::new().build())
        .with_retry_policy(NeverRetry)
        .with_tracing()
        .build()
        .await?;

    let mut first_trace_id = None;
    for iteration in 0..1_000 {
        // 3. Generate Trace
        // Start a root span
        let root_span = tracing::info_span!("e2e_root", "otel.name" = ROOT_SPAN_NAME);
        let trace_id = root_span
            .context()
            .span()
            .span_context()
            .trace_id()
            .to_string();

        // Make the API call
        // This will generate child spans within the library
        let _result = client
            .echo()
            .set_content("test")
            .send()
            .instrument(root_span)
            .await;

        match iteration {
            0 => {
                first_trace_id = Some(trace_id.clone());
                println!(
                    "View generated trace in Console: https://console.cloud.google.com/traces/explorer;traceId={}?project={}",
                    trace_id, project_id
                );
                print!("RUNNING ");
            }
            n if n % 100 == 0 => print!("."),
            _ => {}
        };
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    println!(" DONE");
    // 4. Force flush to ensure spans are sent.
    tracer_provider.force_flush()?;
    meter_provider.force_flush()?;

    // 5. Verify (Poll Cloud Trace API)
    let trace_id = first_trace_id.unwrap();
    let required = BTreeSet::from_iter([
        ROOT_SPAN_NAME,
        "google_cloud_showcase_v1beta1::client::Echo::echo",
        "POST /v1beta1/echo:echo",
    ]);
    let trace = wait_for_trace(&project_id, &trace_id, required.len()).await?;

    // Verify the expected spans appear in the trace:
    let span_names = trace
        .spans
        .iter()
        .map(|s| s.name.as_str())
        .collect::<BTreeSet<_>>();
    let missing = required.difference(&span_names).collect::<Vec<_>>();
    assert!(missing.is_empty(), "missing={missing:?}\n\n{trace:?}",);

    Ok(())
}
