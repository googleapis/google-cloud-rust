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

use super::{set_up_otel_provider, wait_for_trace};
use crate::Anonymous;
use google_cloud_showcase_v1beta1::client::Echo;
use google_cloud_test_utils::runtime_config::project_id;
use httptest::{Expectation, Server, matchers::*, responders::status_code};
use opentelemetry::trace::TraceContextExt;
use std::collections::BTreeSet;
use tracing_opentelemetry::OpenTelemetrySpanExt;

const ROOT_SPAN_NAME: &str = "e2e-showcase-test";

pub async fn run() -> anyhow::Result<()> {
    // 1. Setup Mock Server (Traffic Destination)
    let echo_server = Server::run();
    echo_server.expect(
        Expectation::matching(all_of![
            request::method("POST"),
            request::path("/v1beta1/echo:echo"),
        ])
        .respond_with(status_code(200).body(r#"{"content": "test"}"#)),
    );

    // 2. Setup Telemetry (Real Google Cloud Destination)
    // This requires GOOGLE_CLOUD_PROJECT to be set.
    let project_id = project_id()?;
    let provider = set_up_otel_provider(&project_id).await?;

    // 3. Generate Trace
    // Start a root span
    let root_span = tracing::info_span!("e2e_root", "otel.name" = ROOT_SPAN_NAME);
    let trace_id = {
        let _enter = root_span.enter();
        let trace_id = root_span
            .context()
            .span()
            .span_context()
            .trace_id()
            .to_string();

        // Initialize showcase client pointing to local mock server
        let client = Echo::builder()
            .with_endpoint(format!("http://{}", echo_server.addr()))
            .with_credentials(Anonymous::new().build())
            .with_tracing()
            .build()
            .await?;

        // Make the API call
        // This will generate child spans within the library
        let _ = client.echo().set_content("test").send().await?;

        trace_id
    };
    // explicitly drop the span to end it
    drop(root_span);

    println!(
        "View generated trace in Console: https://console.cloud.google.com/traces/explorer;traceId={}?project={}",
        trace_id, project_id
    );

    // 4. Force flush to ensure spans are sent.
    provider.force_flush()?;

    // 5. Verify (Poll Cloud Trace API)
    let trace = wait_for_trace(&project_id, &trace_id).await?;

    // Verify the expected spans appear in the trace:
    let span_names = trace
        .spans
        .iter()
        .map(|s| s.name.as_str())
        .collect::<BTreeSet<_>>();
    let required = BTreeSet::from_iter([
        ROOT_SPAN_NAME,
        "google-cloud-showcase-v1beta1::client::Echo::echo",
    ]);
    let missing = required.difference(&span_names).collect::<Vec<_>>();
    assert!(missing.is_empty(), "missing={missing:?}\n\n{trace:?}",);

    Ok(())
}
