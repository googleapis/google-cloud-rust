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

mod duration_metric;
mod request_start;
mod with_client_signals;

pub use duration_metric::DurationMetric;
pub use request_start::RequestStart;
#[allow(unused_imports)]
pub use with_client_signals::WithClientSignals;

/// Creates a new tracing span for a client request.
///
/// This span represents the logical request operation and is used to track
/// the overall duration and status of the request, including retries.
///
/// # Example
///
/// ```
/// let span = client_request_span!("client::Client", "upload_chunk", &HIDDEN_DETAIL);
/// # use google_cloud_gax_internal::client_request_span;
/// # use google_cloud_gax_internal::options::InstrumentationClientInfo;
/// # lazy_static::lazy_static! { static ref HIDDEN_DETAIL: InstrumentationClientInfo = {
/// #     InstrumentationClientInfo::default()
/// # };
/// # }
/// ```
#[macro_export]
macro_rules! client_request_span {
    ($client:expr, $method:expr, $info:expr) => {{
        use $crate::observability::attributes::keys::*;
        use $crate::observability::attributes::{
            GCP_CLIENT_LANGUAGE_RUST, GCP_CLIENT_REPO_GOOGLEAPIS, OTEL_KIND_INTERNAL,
            RPC_SYSTEM_HTTP, otel_status_codes::UNSET,
        };
        tracing::info_span!(
            "client_request",
            "gax.client.span" = true, // Marker field
            { OTEL_NAME } = concat!(env!("CARGO_CRATE_NAME"), "::", $client, "::", $method),
            { OTEL_KIND } = OTEL_KIND_INTERNAL,
            { RPC_SYSTEM } = RPC_SYSTEM_HTTP, // Default to HTTP, can be overridden
            { RPC_SERVICE } = $info.service_name,
            { RPC_METHOD } = $method,
            { GCP_CLIENT_SERVICE } = $info.service_name,
            { GCP_CLIENT_VERSION } = $info.client_version,
            { GCP_CLIENT_REPO } = GCP_CLIENT_REPO_GOOGLEAPIS,
            { GCP_CLIENT_ARTIFACT } = $info.client_artifact,
            { GCP_CLIENT_LANGUAGE } = GCP_CLIENT_LANGUAGE_RUST,
            // Fields to be recorded later
            { OTEL_STATUS_CODE } = UNSET,
            { OTEL_STATUS_DESCRIPTION } = ::tracing::field::Empty,
            { ERROR_TYPE } = ::tracing::field::Empty,
            { SERVER_ADDRESS } = ::tracing::field::Empty,
            { SERVER_PORT } = ::tracing::field::Empty,
            { URL_FULL } = ::tracing::field::Empty,
            { HTTP_REQUEST_METHOD } = ::tracing::field::Empty,
            { HTTP_RESPONSE_STATUS_CODE } = ::tracing::field::Empty,
            { HTTP_REQUEST_RESEND_COUNT } = ::tracing::field::Empty,
        )
    }};
}

#[cfg(test)]
mod tests {
    use crate::options::InstrumentationClientInfo;

    pub(crate) static TEST_INFO: InstrumentationClientInfo = InstrumentationClientInfo {
        service_name: "test-service",
        client_version: "1.2.3",
        client_artifact: "test-artifact",
        default_host: "example.com",
    };
    pub(crate) static URL_TEMPLATE: &str = "/v1/projects/{}:test_method";
    pub(crate) static METHOD: &str = "test-method";
}
