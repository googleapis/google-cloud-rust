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

use crate::options::InstrumentationClientInfo;
use gax::options::RequestOptions;

/// Holds information extracted from HTTP requests and responses,
/// formatted to align with OpenTelemetry semantic conventions for tracing.
/// This struct is used to populate attributes on tracing spans.
#[derive(Debug, Clone)]
#[allow(dead_code)] // TODO(#3239): Remove once used in http.rs
pub(crate) struct HttpSpanInfo {
    // Attributes for OpenTelemetry SDK interop
    otel_kind: String,   // "Client"
    otel_name: String,   // "{METHOD} {url.template}" or "{METHOD}"
    otel_status: String, // "Unset", "Ok", "Error"

    // OpenTelemetry Semantic Conventions
    rpc_system: String, // "http"
    http_request_method: String,
    server_address: Option<String>, // Host from URL
    server_port: Option<i64>,       // Port from URL
    url_full: String,
    url_scheme: Option<String>,
    url_template: Option<String>, // From RequestOptions.path_template
    url_domain: Option<String>,   // Host from generator

    http_response_status_code: Option<i64>,
    error_type: Option<String>,
    http_request_resend_count: Option<i64>,

    // Custom GCP Attributes
    gcp_client_service: Option<String>,
    gcp_client_version: Option<String>,
    gcp_client_repo: String, // "googleapis/google-cloud-rust"
    gcp_client_artifact: Option<String>,
}

impl HttpSpanInfo {
    // TODO(#3239): Remove once used in http.rs
    #[allow(dead_code)]
    pub(crate) fn from_request(
        request: &reqwest::Request,
        options: &RequestOptions,
        instrumentation: Option<&InstrumentationClientInfo>,
        prior_attempt_count: u32,
    ) -> Self {
        let url = request.url();
        let method = request.method();

        let url_template = gax::options::internal::get_path_template(options);
        let otel_name = url_template.map_or_else(
            || method.to_string(),
            |template| format!("{} {}", method, template),
        );

        let http_request_resend_count = if prior_attempt_count > 0 {
            Some(prior_attempt_count as i64)
        } else {
            None
        };

        let (gcp_client_service, gcp_client_version, gcp_client_artifact, url_domain) =
            instrumentation.map_or((None, None, None, None), |info| {
                (
                    Some(info.service_name.to_string()),
                    Some(info.client_version.to_string()),
                    Some(info.client_artifact.to_string()),
                    Some(info.default_host.to_string()),
                )
            });

        Self {
            rpc_system: "http".to_string(),
            otel_kind: "Client".to_string(),
            otel_name,
            otel_status: "Unset".to_string(),
            http_request_method: method.to_string(),
            server_address: url.host_str().map(String::from),
            server_port: url.port_or_known_default().map(|p| p as i64),
            url_full: url.to_string(),
            url_scheme: Some(url.scheme().to_string()),
            url_template: url_template.map(String::from),
            url_domain,
            http_response_status_code: None,
            error_type: None,
            http_request_resend_count,
            gcp_client_service,
            gcp_client_version,
            gcp_client_repo: "googleapis/google-cloud-rust".to_string(),
            gcp_client_artifact,
        }
    }

    /// Updates the span info based on the outcome of the HTTP request.
    /// This method should be called after the request has completed.
    // TODO(#3239): Remove once used in http.rs
    #[allow(dead_code)]
    pub(crate) fn update_from_response(
        &mut self,
        result: &Result<reqwest::Response, reqwest::Error>,
    ) {
        match result {
            Ok(response) => {
                self.http_response_status_code = Some(response.status().as_u16() as i64);
                if response.status().is_success() {
                    self.otel_status = "Ok".to_string();
                } else {
                    self.otel_status = "Error".to_string();
                    self.error_type = Some(response.status().to_string());
                }
            }
            Err(err) => {
                self.otel_status = "Error".to_string();
                if err.is_timeout() {
                    self.error_type = Some("TIMEOUT".to_string());
                } else if err.is_connect() {
                    self.error_type = Some("CONNECTION_ERROR".to_string());
                } else if err.is_request() {
                    self.error_type = Some("REQUEST_ERROR".to_string());
                } else {
                    self.error_type = Some("UNKNOWN".to_string());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::options::InstrumentationClientInfo;
    use gax::options::RequestOptions;
    use http::Method;
    use reqwest;

    #[tokio::test]
    async fn test_http_span_info_from_request_basic() {
        let request =
            reqwest::Request::new(Method::GET, "https://example.com/test".parse().unwrap());
        let options = RequestOptions::default();

        let span_info = HttpSpanInfo::from_request(&request, &options, None, 0);

        assert_eq!(span_info.rpc_system, "http");
        assert_eq!(span_info.otel_kind, "Client");
        assert_eq!(span_info.otel_name, "GET");
        assert_eq!(span_info.otel_status, "Unset");
        assert_eq!(span_info.http_request_method, "GET");
        assert_eq!(span_info.server_address, Some("example.com".to_string()));
        assert_eq!(span_info.server_port, Some(443));
        assert_eq!(span_info.url_full, "https://example.com/test");
        assert_eq!(span_info.url_scheme, Some("https".to_string()));
        assert_eq!(span_info.url_template, None);
        assert_eq!(span_info.url_domain, None);
        assert_eq!(span_info.http_response_status_code, None);
        assert_eq!(span_info.error_type, None);
        assert_eq!(span_info.http_request_resend_count, None);
        assert_eq!(span_info.gcp_client_service, None);
        assert_eq!(span_info.gcp_client_version, None);
        assert_eq!(span_info.gcp_client_repo, "googleapis/google-cloud-rust");
        assert_eq!(span_info.gcp_client_artifact, None);
    }

    #[tokio::test]
    async fn test_http_span_info_from_request_with_instrumentation() {
        let request = reqwest::Request::new(
            Method::POST,
            "https://test.service.dev:443/v1/items".parse().unwrap(),
        );
        let options = RequestOptions::default();
        const INFO: InstrumentationClientInfo = InstrumentationClientInfo {
            service_name: "test.service",
            client_version: "1.2.3",
            client_artifact: "google-cloud-test",
            default_host: "test.service.dev",
        };

        let span_info = HttpSpanInfo::from_request(&request, &options, Some(&INFO), 0);

        assert_eq!(
            span_info.gcp_client_service,
            Some("test.service".to_string())
        );
        assert_eq!(span_info.gcp_client_version, Some("1.2.3".to_string()));
        assert_eq!(
            span_info.gcp_client_artifact,
            Some("google-cloud-test".to_string())
        );
        assert_eq!(span_info.url_domain, Some("test.service.dev".to_string()));
        assert_eq!(
            span_info.server_address,
            Some("test.service.dev".to_string())
        );
        assert_eq!(span_info.server_port, Some(443));
    }

    #[tokio::test]
    async fn test_http_span_info_from_request_with_path_template() {
        let request = reqwest::Request::new(
            Method::GET,
            "https://example.com/items/123".parse().unwrap(),
        );
        let options = gax::options::internal::set_path_template(
            RequestOptions::default(),
            Some("/items/{item_id}".to_string()),
        );

        let span_info = HttpSpanInfo::from_request(&request, &options, None, 0);

        assert_eq!(span_info.url_template, Some("/items/{item_id}".to_string()));
        assert_eq!(span_info.otel_name, "GET /items/{item_id}");
    }

    #[tokio::test]
    async fn test_http_span_info_from_request_with_prior_attempt_count() {
        let request =
            reqwest::Request::new(Method::GET, "https://example.com/test".parse().unwrap());
        let options = RequestOptions::default();

        // prior_attempt_count is 0 for the first try
        let span_info = HttpSpanInfo::from_request(&request, &options, None, 0);
        assert_eq!(span_info.http_request_resend_count, None);

        // prior_attempt_count is 1 for the second try (first retry)
        let span_info = HttpSpanInfo::from_request(&request, &options, None, 1);
        assert_eq!(span_info.http_request_resend_count, Some(1));

        let span_info = HttpSpanInfo::from_request(&request, &options, None, 5);
        assert_eq!(span_info.http_request_resend_count, Some(5));
    }

    #[tokio::test]
    async fn test_update_from_response_success() {
        let request =
            reqwest::Request::new(Method::GET, "https://example.com/test".parse().unwrap());
        let mut span_info =
            HttpSpanInfo::from_request(&request, &RequestOptions::default(), None, 0);

        let response =
            reqwest::Response::from(http::Response::builder().status(200).body("").unwrap());
        span_info.update_from_response(&Ok(response));

        assert_eq!(span_info.otel_status, "Ok");
        assert_eq!(span_info.http_response_status_code, Some(200));
        assert_eq!(span_info.error_type, None);
    }

    #[tokio::test]
    async fn test_update_from_response_http_error() {
        let request =
            reqwest::Request::new(Method::GET, "https://example.com/test".parse().unwrap());
        let mut span_info =
            HttpSpanInfo::from_request(&request, &RequestOptions::default(), None, 0);

        let response =
            reqwest::Response::from(http::Response::builder().status(404).body("").unwrap());
        span_info.update_from_response(&Ok(response));

        assert_eq!(span_info.otel_status, "Error");
        assert_eq!(span_info.http_response_status_code, Some(404));
        assert_eq!(span_info.error_type, Some("404 Not Found".to_string()));
    }
}
