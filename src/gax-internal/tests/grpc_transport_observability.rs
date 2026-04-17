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

#[cfg(all(test, feature = "_internal-grpc-client"))]
mod tests {
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
    use google_cloud_gax::error::Error;
    use google_cloud_gax_internal::observability::{
        ClientRequestAttributes, DurationMetric, RequestRecorder,
    };
    use google_cloud_gax_internal::options::InstrumentationClientInfo;
    use grpc_server::{google, start_echo_server};
    use opentelemetry::trace::{Status as SpanStatus, TracerProvider};
    use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
    use opentelemetry_sdk::logs::{BatchLogProcessor, InMemoryLogExporter, SdkLoggerProvider};
    use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};
    use opentelemetry_sdk::trace::{BatchSpanProcessor, InMemorySpanExporter, SdkTracerProvider};

    use std::sync::LazyLock;
    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::Registry;
    use tracing_subscriber::layer::SubscriberExt;

    const TEST_METHOD: &str = "test.method";
    const TEST_URL_TEMPLATE: &str = "https://example.com/test-only";

    static TEST_INFO: LazyLock<InstrumentationClientInfo> = LazyLock::new(|| {
        let mut info = InstrumentationClientInfo::default();
        info.client_artifact = "test-artifact";
        info.client_version = "1.2.3";
        info.service_name = "test-service";
        info.default_host = "example.com";
        info
    });

    pub struct SignalProviders {
        pub trace_exporter: InMemorySpanExporter,
        pub trace_provider: SdkTracerProvider,
        pub logs_exporter: InMemoryLogExporter,
        pub logs_provider: SdkLoggerProvider,
        pub metric_exporter: InMemoryMetricExporter,
        pub metric_provider: SdkMeterProvider,
        pub _guard: DefaultGuard,
    }

    impl SignalProviders {
        pub fn new() -> Self {
            let trace_exporter = InMemorySpanExporter::default();
            let trace_provider = SdkTracerProvider::builder()
                .with_span_processor(BatchSpanProcessor::builder(trace_exporter.clone()).build())
                .build();

            let logs_exporter = InMemoryLogExporter::default();
            let logs_provider = SdkLoggerProvider::builder()
                .with_log_processor(BatchLogProcessor::builder(logs_exporter.clone()).build())
                .build();

            let guard = tracing::subscriber::set_default(
                Registry::default()
                    .with(OpenTelemetryTracingBridge::new(&logs_provider))
                    .with(
                        tracing_opentelemetry::layer()
                            .with_tracer(trace_provider.tracer("test-only")),
                    ),
            );

            let metric_exporter = InMemoryMetricExporter::default();
            let metric_provider = SdkMeterProvider::builder()
                .with_reader(PeriodicReader::builder(metric_exporter.clone()).build())
                .build();

            // Set the global meter provider for this process.
            opentelemetry::global::set_meter_provider(metric_provider.clone());

            SignalProviders {
                trace_exporter,
                trace_provider,
                logs_exporter,
                logs_provider,
                metric_exporter,
                metric_provider,
                _guard: guard,
            }
        }

        pub fn force_flush(&self) -> anyhow::Result<()> {
            let _ = self.trace_provider.force_flush();
            let _ = self.logs_provider.force_flush();
            self.metric_provider.force_flush()?;
            Ok(())
        }
    }

    pub async fn recorded_request_grpc_stub(url: &str) -> Result<String, Error> {
        let recorder = RequestRecorder::current().expect("current recorder should be available");
        recorder.on_client_request(
            ClientRequestAttributes::default()
                .set_rpc_method(TEST_METHOD)
                .set_url_template(TEST_URL_TEMPLATE)
                .set_resource_name("//test.googleapis.com/test-only".to_string()),
        );

        let mut config = google_cloud_gax_internal::options::ClientConfig::default();
        config.tracing = true;
        config.retry_policy = Some(std::sync::Arc::new(
            google_cloud_gax::retry_policy::NeverRetry,
        ));
        config.cred = Some(Anonymous::new().build());

        let client = google_cloud_gax_internal::grpc::Client::new_with_instrumentation(
            config, url, &TEST_INFO,
        )
        .await
        .map_err(|e| Error::io(e.to_string()))?;

        let extensions = {
            let mut e = tonic::Extensions::new();
            e.insert(tonic::GrpcMethod::new("google.test.v1.EchoService", "Echo"));
            e
        };
        let request = google::test::v1::EchoRequest::default();
        // We expect an error (InvalidArgument) because EchoRequest::default() is empty,
        // which triggers error handling and asserts on error signals.
        let response = client
            .execute::<_, google::test::v1::EchoResponse>(
                extensions,
                http::uri::PathAndQuery::from_static("/google.test.v1.EchoService/Echo"),
                request,
                google_cloud_gax::options::RequestOptions::default(),
                "test-client",
                "",
            )
            .await;

        match response {
            Ok(_) => Ok("success".to_string()),
            Err(e) => Err(e),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn grpc_transport_signals() -> anyhow::Result<()> {
        let (endpoint, _server) = start_echo_server().await?;
        let signals = SignalProviders::new();

        // DurationMetric::new uses the global provider, which we just set in SignalProviders::new()
        let metric = DurationMetric::new(&TEST_INFO);

        let (span, pending) = google_cloud_gax_internal::client_request_signals!(
            metric: metric.clone(),
            info: *TEST_INFO,
            method: "FakeGrpcClient::some_rust_function",
            recorded_request_grpc_stub(&endpoint)
        );
        let result = pending.await;
        assert!(result.is_err(), "{result:?}");
        drop(span);

        signals.force_flush()?;

        let metrics = signals.metric_exporter.get_finished_metrics()?;
        let spans = signals.trace_exporter.get_finished_spans()?;
        let captured = signals.logs_exporter.get_emitted_logs()?;

        let client_span = spans
            .iter()
            .find(|s| s.name.contains("FakeGrpcClient::some_rust_function"))
            .expect("client span should exist");
        let trace_id = client_span.span_context.trace_id();

        let mut failures = Vec::new();

        // 1. Check Metric
        let found = metrics
            .iter()
            .flat_map(|s| s.scope_metrics())
            .flat_map(|r| r.metrics())
            .find(|m| m.name() == "gcp.client.attempt.duration");

        if found.is_none() {
            failures.push("Missing metric: gcp.client.attempt.duration".to_string());
        }

        // 2. Check Span
        let transport_span = spans
            .iter()
            .find(|s| s.name == "google.test.v1.EchoService/Echo");

        if let Some(span) = transport_span {
            if !matches!(span.status, SpanStatus::Error { .. }) {
                failures.push("Span status is not Error".to_string());
            }
            if span.span_context.trace_id() != trace_id {
                failures.push("Span trace_id does not match client span".to_string());
            }
        } else {
            failures.push("Missing span: google.test.v1.EchoService/Echo".to_string());
        }

        // 3. Check Log
        let transport_target = "experimental.transport.request";
        let transport_name = "experimental.transport.request.error";
        let transport_record = captured
            .iter()
            .find(|r| r.record.target().is_some_and(|v| v == transport_target));

        if let Some(record) = transport_record {
            if record.record.event_name() != Some(transport_name) {
                failures.push(format!("Log event name is not {transport_name}"));
            }
            if record.record.severity_text() != Some("DEBUG") {
                failures.push("Log severity is not DEBUG".to_string());
            }
            if record.record.trace_context().map(|c| c.trace_id) != Some(trace_id) {
                failures.push("Log trace_id does not match client span".to_string());
            }
        } else {
            failures.push("Missing log for target: experimental.transport.request".to_string());
        }

        assert!(
            failures.is_empty(),
            "Expected all signals to be present, but found failures:\n{}",
            failures.join("\n")
        );

        Ok(())
    }
}
