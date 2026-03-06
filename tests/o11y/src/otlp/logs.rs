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

use opentelemetry::{SpanId, TraceId};
use serde::Serializer;
use serde::ser::SerializeMap;
use serde_json::Serializer as JsonSerializer;
use std::fmt::Result as FmtResult;
use tracing::{Event, Subscriber};
use tracing_opentelemetry::OtelData;
use tracing_serde::AsSerde;
use tracing_serde::fields::AsMap;
use tracing_subscriber::fmt::format::{FormatEvent, Writer};
use tracing_subscriber::fmt::{FmtContext, FormatFields};
use tracing_subscriber::registry::LookupSpan;

/// Format [tracing] events into the format consumed by the Google Cloud [Ops Agent].
///
/// # Example
/// ```
/// use integration_tests_o11y::otlp::logs::EventFormatter;
/// use tracing::subscriber;
/// use tracing_subscriber::fmt;
/// use tracing_subscriber::fmt::format::FmtSpan;
/// use tracing_subscriber::layer::SubscriberExt;
/// use tracing_subscriber::{EnvFilter, Layer, Registry};
///
/// let formatter = EventFormatter::new("my-project-id");
/// tracing::subscriber::set_global_default(
///     Registry::default().with(
///         fmt::layer()
///             .with_span_events(FmtSpan::NONE)
///             .with_level(true)
///             .with_thread_ids(true)
///             .event_format(formatter)
///             .with_filter(EnvFilter::from_default_env())),
/// );
/// ```
///
/// When deploying applications to Google Cloud environments, such as [GKE], [Cloud Run], or [GCE],
/// applications can use the Ops Agent to forward their logs to [Cloud Logging]. If the logs are
/// formatted as JSON objects, the Ops Agent can extract annotations to correctly tag the severity
/// of the message, and link the message to the span active when the message was generated.
///
/// This formatter creates structured logs for [tracing] events. The structured logs connect the
/// log entries to the corresponding spans and traces, though these must be uploaded separately.
///
/// [GCE]: https://cloud.google.com/compute
/// [GKE]: https://cloud.google.com/gke
/// [Cloud Run]: https://cloud.google.com/run
/// [tracing]: https://docs.rs/tracing
/// [Ops Agent]: https://docs.cloud.google.com/logging/docs/agent/ops-agent
#[derive(Clone, Debug)]
pub struct EventFormatter {
    project_id: String,
}

impl EventFormatter {
    /// Creates a new instance, assuming all spans and traces are sent to `project_id`.
    pub fn new<V>(project_id: V) -> Self
    where
        V: Into<String>,
    {
        Self {
            project_id: project_id.into(),
        }
    }

    pub fn trace_info<S, N>(
        &self,
        ctx: &FmtContext<'_, S, N>,
        event: &Event<'_>,
    ) -> (Option<(TraceId, SpanId)>, bool)
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
        N: for<'a> FormatFields<'a> + 'static,
    {
        use opentelemetry::trace::TraceContextExt;
        use tracing_opentelemetry::OpenTelemetrySpanExt;
        if let Some((Some(tid), Some(sid))) = ctx.lookup_current().and_then(|span| {
            span.extensions()
                .get::<OtelData>()
                .map(|data| (data.trace_id(), data.span_id()))
        }) {
            return (Some((tid, sid)), tid != TraceId::INVALID);
        }
        if event.is_contextual() {
            let current = tracing::Span::current();
            let tid = current.context().span().span_context().trace_id();
            let sid = current.context().span().span_context().span_id();
            return (Some((tid, sid)), tid != TraceId::INVALID);
        }
        (None, false)
    }
}

impl<S, N> FormatEvent<S, N> for EventFormatter
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> FmtResult
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
        N: for<'a> FormatFields<'a> + 'static,
    {
        let meta = event.metadata();

        let mut visit = || {
            let mut serializer = JsonSerializer::new(WriteAdaptor::new(&mut writer));
            let mut serializer = serializer.serialize_map(None)?;
            serializer.serialize_entry("timestamp", &chrono::Utc::now().to_rfc3339())?;
            serializer.serialize_entry("severity", &meta.level().as_serde())?;
            serializer.serialize_entry("fields", &event.field_map())?;
            serializer.serialize_entry("target", meta.target())?;
            match self.trace_info(ctx, event) {
                (Some((tid, sid)), sampled) => {
                    serializer.serialize_entry(
                        "logging.googleapis.com/trace",
                        &format!("projects/{}/traces/{tid}", self.project_id),
                    )?;
                    serializer.serialize_entry("logging.googleapis.com/span", &sid.to_string())?;
                    serializer.serialize_entry("logging.googleapis.com/trace_sampled", &sampled)?;
                }
                (None, sampled) => {
                    serializer.serialize_entry("logging.googleapis.com/trace_sampled", &sampled)?;
                }
            };
            serializer.end()
        };
        visit().map_err(|_| std::fmt::Error)?;
        writeln!(writer)
    }
}

/// Make a `std::fmt::write` look like a `std::io::Write` so we can use it as the destination of a
/// `serde_json::Serializer`.
struct WriteAdaptor<'a> {
    fmt_write: &'a mut dyn std::fmt::Write,
}

impl<'a> WriteAdaptor<'a> {
    pub fn new(fmt_write: &'a mut dyn std::fmt::Write) -> Self {
        Self { fmt_write }
    }
}

impl<'a> std::io::Write for WriteAdaptor<'a> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let s = std::str::from_utf8(buf)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        self.fmt_write.write_str(s).map_err(std::io::Error::other)?;
        Ok(s.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
