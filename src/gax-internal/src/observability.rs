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

//! Observability-related components for HTTP and gRPC clients.
//!
//! This module and its sub-modules contain types and functions for emitting
//! tracing spans and metrics.

pub(crate) mod propagation;

pub(crate) mod attributes;

#[cfg(any(feature = "_internal-http-client", feature = "_internal-grpc-client"))]
mod errors;

#[cfg(feature = "_internal-http-client")]
pub(crate) mod http_tracing;

#[cfg(feature = "_internal-http-client")]
pub(crate) use http_tracing::{ResultExt as HttpResultExt, create_http_attempt_span};

#[cfg(feature = "_internal-grpc-client")]
pub(crate) mod grpc_tracing;

mod client_signals;

pub use client_signals::{
    ClientRequestAttributes, DurationMetric, RequestRecorder, TransportMetric, WithClientLogging,
    WithClientMetric, WithClientSpan, WithTransportMetric,
};

#[cfg(feature = "_internal-http-client")]
pub use client_signals::{WithTransportLogging, WithTransportSpan};

#[doc(hidden)]
pub use attributes::{GCP_CLIENT_REPO_GOOGLEAPIS, SCHEMA_URL_VALUE};
