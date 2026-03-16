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

use crate::e2e::wait_for_trace;
use google_cloud_spanner::client::Spanner;
use google_cloud_test_utils::runtime_config::project_id;
use opentelemetry::trace::TraceContextExt;
use std::collections::BTreeSet;
use tracing_opentelemetry::OpenTelemetrySpanExt;

const ROOT_SPAN_NAME: &str = "e2e-spanner-test";

pub async fn spanner_e2e_tracing() -> anyhow::Result<()> {
    let project_id = project_id()?;
    // Create a trace with a number of interesting spans from the
    // `google-cloud-spanner` client.
    let trace_id = send_trace(&project_id).await?;
    let required = BTreeSet::from_iter([
        ROOT_SPAN_NAME,
        "google.spanner.v1.Spanner/CreateSession",
        "google.spanner.v1.Spanner/BeginTransaction",
        "Spanner.CreateSession",
        "Spanner.BeginTransaction",
    ]);
    let trace = wait_for_trace(&project_id, &trace_id, &required).await?;

    println!("TRACE SPANS DUMP:");
    for span in &trace.spans {
        println!("Span: {:?}", span);
    }

    // Verify the expected spans appear in the trace:
    let span_names = trace
        .spans
        .iter()
        .map(|s| s.name.as_str())
        .collect::<BTreeSet<_>>();
    let missing = required.difference(&span_names).collect::<Vec<_>>();
    assert!(missing.is_empty(), "missing={missing:?}\n\n{trace:?}");

    Ok(())
}

async fn send_trace(project_id: &str) -> anyhow::Result<String> {
    // 1. Setup Telemetry (Google Cloud Destination)
    let creds = google_cloud_auth::credentials::Builder::default().build()?;
    let (provider, _, _) = crate::e2e::set_up_providers(
        project_id,
        "e2e-telemetry-test",
        "spanner-test".to_string(),
        creds,
    )
    .await?;

    // 2. Generate Trace
    // Start a root span
    let root_span = tracing::info_span!("e2e_root", { "otel.name" } = ROOT_SPAN_NAME);
    let trace_id = root_span
        .context()
        .span()
        .span_context()
        .trace_id()
        .to_string();

    use tracing::Instrument;
    let _ = client_library_operations(project_id)
        .instrument(root_span)
        .await;

    println!(
        "View generated trace in Console: https://console.cloud.google.com/traces/explorer;traceId={}?project={}",
        trace_id, project_id
    );

    // 4. Force flush to ensure spans are sent.
    if let Err(e) = provider.force_flush() {
        tracing::error!("error flushing provider: {e:}");
    }
    Ok(trace_id)
}

async fn client_library_operations(project: &str) -> anyhow::Result<()> {
    // Explicitly opt-in to E2E tracing headers for the test
    unsafe {
        std::env::set_var(
            "GOOGLE_CLOUD_TEST_EXTRA_HEADERS",
            "x-goog-spanner-end-to-end-tracing=true",
        );
    }
    let instance = std::env::var("GOOGLE_CLOUD_SPANNER_TEST_INSTANCE")
        .unwrap_or_else(|_| "trace-propagation-test-instance".to_string());
    let db_id = std::env::var("GOOGLE_CLOUD_SPANNER_TEST_DATABASE")
        .unwrap_or_else(|_| "test-database".to_string());

    let db_path = format!(
        "projects/{}/instances/{}/databases/{}",
        project, instance, db_id
    );

    use google_cloud_auth::credentials::Builder as CredentialsBuilder;
    let creds = CredentialsBuilder::default().build()?;
    let spanner_client = Spanner::builder()
        .with_credentials(creds.clone())
        .with_tracing()
        .build()
        .await?;

    // Calling `build()` on the database client triggers a `CreateSession` RPC
    let db_client = spanner_client.database_client(db_path).build().await?;

    use google_cloud_spanner::model::{
        BeginTransactionRequest, TransactionOptions, transaction_options,
    };
    let mut req = BeginTransactionRequest::default();
    req.session = db_client.session.name.clone();

    let mut options = TransactionOptions::default();
    options.mode = Some(transaction_options::Mode::ReadOnly(Box::default()));
    req.options = Some(options);

    let _ = db_client
        .spanner
        .begin_transaction(req, google_cloud_gax::options::RequestOptions::default())
        .await;

    Ok(())
}
