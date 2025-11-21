// Copyright 2025 Google LLC
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

#[cfg(all(test, feature = "run-integration-tests", google_cloud_unstable_tracing))]
mod telemetry {
    use google_cloud_trace_v1::client::TraceService;
    use httptest::{Expectation, Server, matchers::*, responders::status_code};
    use integration_tests::observability::otlp::CloudTelemetryTracerProviderBuilder;
    use opentelemetry::trace::TraceContextExt;
    use tracing_opentelemetry::OpenTelemetrySpanExt;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_telemetry_e2e() -> integration_tests::Result<()> {
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
        let project_id = integration_tests::project_id()?;
        let service_name = "e2e-telemetry-test";
        println!("Project ID: {}", project_id);

        // Configure OTLP provider (sends to telemetry.googleapis.com)
        // This uses ADC automatically from the environment.
        let provider = CloudTelemetryTracerProviderBuilder::new(&project_id, service_name)
            .build()
            .await?;

        // Install subscriber
        let _guard = tracing_subscriber::Registry::default()
            .with(integration_tests::observability::tracing::layer(
                provider.clone(),
            ))
            .set_default();

        // Wait for credentials to be refreshed in the background task
        println!("Waiting for credentials to initialize...");
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;

        // 3. Generate Trace
        let span_name = "e2e-showcase-test";

        // Start a root span
        let root_span = tracing::info_span!("e2e_root", "otel.name" = span_name);
        let trace_id = {
            let _enter = root_span.enter();
            let otel_ctx = root_span.context();
            let otel_span_ref = otel_ctx.span();
            let span_context = otel_span_ref.span_context();
            let trace_id = span_context.trace_id().to_string();

            // Initialize showcase client pointing to local mock server
            // We use anonymous credentials for the *client* because it's talking to httptest
            let endpoint = format!("http://{}", echo_server.addr());

            let client = showcase::client::Echo::builder()
                .with_endpoint(endpoint)
                .with_credentials(auth::credentials::anonymous::Builder::new().build())
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

        println!("Generated Trace ID: {}", trace_id);
        println!("Span Name: {}", span_name);
        println!(
            "View in Console: https://console.cloud.google.com/traces/explorer;traceId={}?project={}",
            trace_id, project_id
        );

        // 4. Flush
        // Force flush
        for _ in 0..5 {
            let _ = provider.force_flush();
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
        println!("Spans flushed.");

        // 5. Verify (Poll Cloud Trace API)
        let client = TraceService::builder().build().await?;

        println!("Polling for trace...");
        let backoff_delays = [10, 60, 120, 120, 120];
        let mut trace = None;

        for delay in backoff_delays {
            println!("Waiting {}s before polling...", delay);
            tokio::time::sleep(std::time::Duration::from_secs(delay)).await;

            match client
                .get_trace()
                .set_project_id(&project_id)
                .set_trace_id(&trace_id)
                .send()
                .await
            {
                Ok(t) => {
                    trace = Some(t);
                    break;
                }
                Err(e) => {
                    if let Some(status) = e.status() {
                        if status.code == gax::error::rpc::Code::NotFound
                            || status.code == gax::error::rpc::Code::Internal
                        {
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

        println!("Trace found!");
        println!("Response: {:?}", trace);

        // 6. Assertions
        // Check for root span
        let root_found = trace.spans.iter().any(|s| s.name == span_name);
        assert!(root_found, "Root span '{}' not found in trace", span_name);

        // Check for showcase client span
        let client_span_name = "google-cloud-showcase-v1beta1::client::Echo::echo";
        let client_found = trace.spans.iter().any(|s| s.name == client_span_name);
        assert!(
            client_found,
            "Client library span '{}' not found in trace",
            client_span_name
        );

        Ok(())
    }
}
