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

//! An attempt interceptor that populates the `google-cloud-resource-prefix` header.
//!
//! Spanner frontends use the `google-cloud-resource-prefix` header to route requests to the
//! regional endpoint that serves the target database. Requests without this header fall back to
//! the global routing target. The other Spanner client libraries (Java, Go, C++, Python, ...) send
//! `projects/{project}/instances/{instance}/databases/{database}` on every data plane RPC.

use gaxi::attempt_interceptor::AttemptInterceptor;
use http::HeaderMap;
use http::header::{HeaderName, HeaderValue};

pub(crate) static RESOURCE_PREFIX_HEADER: HeaderName =
    HeaderName::from_static("google-cloud-resource-prefix");

static REQUEST_PARAMS_HEADER: HeaderName = HeaderName::from_static("x-goog-request-params");

/// The routing parameters that carry a database (or a child of a database) resource name on
/// Spanner data plane RPCs.
const RESOURCE_PARAM_KEYS: [&str; 2] = ["session", "database"];

/// The number of `/`-separated segments in
/// `projects/{project}/instances/{instance}/databases/{database}`.
const DATABASE_NAME_SEGMENTS: usize = 6;

/// Intercepts outgoing Spanner RPC attempts and sets the `google-cloud-resource-prefix` header to
/// the database name derived from the `x-goog-request-params` routing header.
///
/// Every Spanner data plane RPC carries either a `session=` or a `database=` routing parameter,
/// both of which start with the full database name. The interceptor leaves the headers unchanged
/// if the resource prefix header is already set or no database name can be derived.
#[derive(Debug, Clone, Default)]
pub(crate) struct SpannerResourcePrefixInterceptor;

impl AttemptInterceptor for SpannerResourcePrefixInterceptor {
    fn intercept(&self, headers: &mut HeaderMap, _attempt: u32) {
        if headers.contains_key(&RESOURCE_PREFIX_HEADER) {
            return;
        }
        let Some(database) = headers
            .get(&REQUEST_PARAMS_HEADER)
            .and_then(|value| value.to_str().ok())
            .and_then(database_from_request_params)
        else {
            return;
        };
        if let Ok(value) = HeaderValue::from_str(&database) {
            headers.insert(RESOURCE_PREFIX_HEADER.clone(), value);
        }
    }
}

/// Extracts `projects/{project}/instances/{instance}/databases/{database}` from the value of an
/// `x-goog-request-params` header.
///
/// The header contains `&`-separated `key=value` pairs. Values may be URL-encoded (AIP-4222), so
/// encoded `/` separators (`%2F`) are decoded before parsing.
fn database_from_request_params(params: &str) -> Option<String> {
    params
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .filter(|(key, _)| RESOURCE_PARAM_KEYS.contains(key))
        .find_map(|(_, value)| database_name(&value.replace("%2F", "/").replace("%2f", "/")))
}

/// Truncates a Spanner resource name to its database name, or returns `None` if the resource name
/// does not start with `projects/{project}/instances/{instance}/databases/{database}`.
fn database_name(resource: &str) -> Option<String> {
    let segments: Vec<&str> = resource
        .splitn(DATABASE_NAME_SEGMENTS + 1, '/')
        .take(DATABASE_NAME_SEGMENTS)
        .collect();
    match segments.as_slice() {
        [
            "projects",
            project,
            "instances",
            instance,
            "databases",
            database,
        ] if !project.is_empty() && !instance.is_empty() && !database.is_empty() => {
            Some(segments.join("/"))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_case::test_case;

    const DATABASE: &str = "projects/test-project/instances/test-instance/databases/test-db";

    fn intercept(request_params: Option<&'static str>) -> HeaderMap {
        let mut headers = HeaderMap::new();
        if let Some(params) = request_params {
            headers.insert(
                REQUEST_PARAMS_HEADER.clone(),
                HeaderValue::from_static(params),
            );
        }
        SpannerResourcePrefixInterceptor.intercept(&mut headers, 1);
        headers
    }

    #[test]
    fn traits() {
        static_assertions::assert_impl_all!(
            SpannerResourcePrefixInterceptor: AttemptInterceptor,
            Send,
            Sync,
            std::fmt::Debug,
            Clone,
            Default
        );
    }

    #[test_case("session=projects/test-project/instances/test-instance/databases/test-db/sessions/s1"; "session")]
    #[test_case("database=projects/test-project/instances/test-instance/databases/test-db"; "database")]
    #[test_case("&database=projects/test-project/instances/test-instance/databases/test-db"; "leading separator")]
    #[test_case("session=projects%2Ftest-project%2Finstances%2Ftest-instance%2Fdatabases%2Ftest-db%2Fsessions%2Fs1"; "url encoded")]
    #[test_case("other=value&session=projects/test-project/instances/test-instance/databases/test-db/sessions/s1"; "multiple params")]
    fn sets_resource_prefix(request_params: &'static str) {
        let headers = intercept(Some(request_params));
        assert_eq!(
            headers
                .get(&RESOURCE_PREFIX_HEADER)
                .map(HeaderValue::as_bytes),
            Some(DATABASE.as_bytes()),
            "{headers:?}"
        );
    }

    #[test_case(None; "no request params")]
    #[test_case(Some("session="); "empty value")]
    #[test_case(Some("name=projects/test-project/instances/test-instance/databases/test-db"); "unknown key")]
    #[test_case(Some("database=projects/test-project/instances/test-instance"); "instance only")]
    #[test_case(Some("database=projects/test-project/instances//databases/test-db"); "empty instance")]
    #[test_case(Some("database=projects/p/locations/l/databases/test-db"); "unexpected collection")]
    fn skips_resource_prefix(request_params: Option<&'static str>) {
        let headers = intercept(request_params);
        assert!(
            !headers.contains_key(&RESOURCE_PREFIX_HEADER),
            "{headers:?}"
        );
    }

    #[test]
    fn preserves_existing_resource_prefix() {
        let mut headers = HeaderMap::new();
        headers.insert(
            REQUEST_PARAMS_HEADER.clone(),
            HeaderValue::from_static(
                "session=projects/test-project/instances/test-instance/databases/test-db/sessions/s1",
            ),
        );
        headers.insert(
            RESOURCE_PREFIX_HEADER.clone(),
            HeaderValue::from_static("projects/p/instances/i/databases/custom"),
        );

        SpannerResourcePrefixInterceptor.intercept(&mut headers, 2);

        let values: Vec<_> = headers.get_all(&RESOURCE_PREFIX_HEADER).iter().collect();
        assert_eq!(values, ["projects/p/instances/i/databases/custom"]);
    }

    #[test]
    fn idempotent_across_attempts() {
        let mut headers = HeaderMap::new();
        headers.insert(
            REQUEST_PARAMS_HEADER.clone(),
            HeaderValue::from_static(
                "session=projects/test-project/instances/test-instance/databases/test-db/sessions/s1",
            ),
        );

        SpannerResourcePrefixInterceptor.intercept(&mut headers, 1);
        SpannerResourcePrefixInterceptor.intercept(&mut headers, 2);

        let values: Vec<_> = headers.get_all(&RESOURCE_PREFIX_HEADER).iter().collect();
        assert_eq!(values, [DATABASE]);
    }
}
