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

use crate::observability::attributes::keys::*;
use crate::observability::attributes::*;
use crate::observability::errors::ErrorType;
use google_cloud_gax::error::Error;
use opentelemetry_semantic_conventions::trace as otel_trace;
use tracing::Span;

/// This trait simplifies the implementation of tracing.
///
/// # Example
/// ```
/// use google_cloud_gax::error::Error;
/// async fn some_method_with_tracing() -> Result<String, Error> {
///     use tracing::Instrument;
///     use google_cloud_gax_internal::observability::ResultExt;
///     let span = tracing::info_span!("my span");
///     some_method()
///         .instrument(span.clone())
///         .await
///         .record_in_span(&span)
/// }
/// async fn some_method() -> Result<String, Error> {
/// # panic!()
/// }
/// ```
pub trait ResultExt: sealed::ResultExt {
    fn record_in_span(self, span: &Span) -> Self;
}

/// Seals the [ResultExt] trait so only this crate can implement it.
mod sealed {
    pub trait ResultExt {}
}

impl<T> sealed::ResultExt for std::result::Result<T, Error> {}
impl<T> ResultExt for std::result::Result<T, Error> {
    fn record_in_span(self, span: &Span) -> Self {
        match &self {
            Ok(_) => {}
            Err(e) => {
                span.record(OTEL_STATUS_CODE, otel_status_codes::ERROR);
                let error_type = ErrorType::from_gax_error(e);
                span.record(otel_trace::ERROR_TYPE, error_type.as_str());
                span.record(OTEL_STATUS_DESCRIPTION, e.to_string());
            }
        };
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::options::InstrumentationClientInfo;
    use google_cloud_test_utils::test_layer::{AttributeValue, TestLayer};
    use std::collections::BTreeMap;
    use tracing::field;

    const INFO: InstrumentationClientInfo = InstrumentationClientInfo {
        service_name: "test.service",
        client_version: "1.2.3",
        client_artifact: "google-cloud-test",
        default_host: "example.com",
        disable_actionable_error_logging: false,
    };

    #[tokio::test]
    async fn client_request_span() {
        let guard = TestLayer::initialize();
        let _span = crate::client_request_span!("TestClient", "test_method", &INFO);
        let captured = TestLayer::capture(&guard);
        let got = match &captured[..] {
            [span] => BTreeMap::from_iter(span.attributes.clone()),
            _ => panic!("expected a single span to be captured: {captured:?}"),
        };
        let want: BTreeMap<String, AttributeValue> = [
            (
                OTEL_NAME,
                "google_cloud_gax_internal::TestClient::test_method".into(),
            ),
            (OTEL_KIND, "Internal".into()),
            (otel_trace::RPC_SYSTEM, "http".into()),
            (otel_trace::RPC_SERVICE, "test.service".into()),
            (otel_trace::RPC_METHOD, "test_method".into()),
            (GCP_CLIENT_SERVICE, "test.service".into()),
            (GCP_CLIENT_VERSION, "1.2.3".into()),
            (GCP_CLIENT_REPO, "googleapis/google-cloud-rust".into()),
            (GCP_CLIENT_ARTIFACT, "google-cloud-test".into()),
            (GCP_CLIENT_LANGUAGE, "rust".into()),
            (OTEL_STATUS_CODE, "UNSET".into()),
            ("gax.client.span", true.into()),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();

        assert_eq!(got, want);
    }

    #[test]
    fn result_ext_ok() {
        use ResultExt;
        let guard = TestLayer::initialize();
        let result = {
            let span = tracing::info_span!(
                "test-only",
                { OTEL_STATUS_CODE } = otel_status_codes::UNSET,
                { otel_trace::ERROR_TYPE } = field::Empty,
                { OTEL_STATUS_DESCRIPTION } = field::Empty,
            );
            let _enter = span.enter();
            Ok("unused").record_in_span(&span)
        };
        assert!(matches!(result, Ok("unused")), "{result:?}");
        let captured = TestLayer::capture(&guard);
        let attributes = match &captured[..] {
            [span] => &span.attributes,
            _ => panic!("expected exactly one span in capture: {captured:?}"),
        };
        let want = BTreeMap::from_iter(
            [
                (OTEL_STATUS_CODE, Some("UNSET")),
                (otel_trace::ERROR_TYPE, None),
                (OTEL_STATUS_DESCRIPTION, None),
            ]
            .map(|(k, v)| (k, v.map(str::to_string))),
        );
        let got = want
            .keys()
            .map(|k| (*k, attributes.get(*k).and_then(|v| v.as_string())))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(want, got, "attributes = {attributes:?}");
    }

    #[test]
    fn result_ext_error() {
        use ResultExt;
        let guard = TestLayer::initialize();
        let result = {
            let span = tracing::info_span!(
                "test-only",
                { OTEL_STATUS_CODE } = otel_status_codes::UNSET,
                { otel_trace::ERROR_TYPE } = field::Empty,
                { OTEL_STATUS_DESCRIPTION } = field::Empty,
            );
            let _enter = span.enter();
            Err::<&str, Error>(Error::timeout("test timeout")).record_in_span(&span)
        };
        assert!(matches!(result, Err(ref e) if e.is_timeout()), "{result:?}");
        let captured = TestLayer::capture(&guard);
        let attributes = match &captured[..] {
            [span] => &span.attributes,
            _ => panic!("expected exactly one span in capture: {captured:?}"),
        };
        let want = BTreeMap::from_iter(
            [
                (OTEL_STATUS_CODE, Some("ERROR")),
                (otel_trace::ERROR_TYPE, Some("CLIENT_TIMEOUT")),
                (
                    OTEL_STATUS_DESCRIPTION,
                    Some("the request exceeded the request deadline test timeout"),
                ),
            ]
            .map(|(k, v)| (k, v.map(str::to_string))),
        );
        let got = want
            .keys()
            .map(|k| (*k, attributes.get(*k).and_then(|v| v.as_string())))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(want, got, "attributes = {attributes:?}");
    }
}
