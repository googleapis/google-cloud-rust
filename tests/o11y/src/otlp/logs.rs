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

//! This module contains types to export OpenTelemetry logs to Google Cloud Logging.
//!
//! When your application is deployed to an environment running the Google Cloud [Ops Agent] we
//! recommend that you configure the [tracing] framework to use [EventFormatter]. This will output
//! your log messages in the format that Ops Agent requires, and Ops Agent will forward them to
//! Cloud Logging. Ops Agent is available on [Cloud Run], [GKE], and [GCE].
//!
//! If Ops Agent is not available, consider using [Builder] and configure the [tracing] framework
//! and [opentelemetry] to directly send your logs to Cloud Logging.
//!
//! # Example: use the Ops Agent
//! ```
//! use integration_tests_o11y::otlp::logs::EventFormatter;
//! use tracing::subscriber;
//! use tracing_subscriber::fmt;
//! use tracing_subscriber::fmt::format::FmtSpan;
//! use tracing_subscriber::layer::SubscriberExt;
//! use tracing_subscriber::{EnvFilter, Layer, Registry};
//!
//! let formatter = EventFormatter::new("my-project-id");
//! tracing::subscriber::set_global_default(
//!     Registry::default().with(
//!         fmt::layer()
//!             .with_span_events(FmtSpan::NONE)
//!             .with_level(true)
//!             .with_thread_ids(true)
//!             .event_format(formatter)
//!             .with_filter(EnvFilter::from_default_env())),
//! );
//! ```
//!
//! # Example: without an Ops Agent running
//! ```
//! use integration_tests_o11y::otlp::logs::Builder;
//! use opentelemetry_sdk::logs::SdkLoggerProvider;
//! use opentelemetry::global;
//! use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
//! use tracing_subscriber::prelude::*;
//! # async fn example() -> anyhow::Result<()> {
//! // Near the beginning of your `main()` function
//! let provider: SdkLoggerProvider = Builder::new("my-project", "my-service")
//!     .build()
//!     .await?;
//! let otel_layer = OpenTelemetryTracingBridge::new(&provider);
//! tracing_subscriber::registry()
//!     .with(otel_layer)
//!     // maybe add other layers
//!     // .with(...)
//!     .init();
//! # Ok(()) }
//! ```
//!
//! [GCE]: https://cloud.google.com/compute
//! [GKE]: https://cloud.google.com/gke
//! [Cloud Run]: https://cloud.google.com/run
//! [Ops Agent]: https://docs.cloud.google.com/logging/docs/agent/ops-agent

mod builder;
mod event_formatter;

pub use builder::Builder;
pub use event_formatter::EventFormatter;
