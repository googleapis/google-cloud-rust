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

use http::header::{HeaderMap, HeaderName};

pub(crate) const X_GOOG_USER_PROJECT: HeaderName = HeaderName::from_static("x-goog-user-project");
pub(crate) const X_GOOG_API_KEY: HeaderName = HeaderName::from_static("x-goog-api-key");
pub(crate) const X_GOOG_REQUEST_PARAMS: HeaderName =
    HeaderName::from_static("x-goog-request-params");
pub(crate) const X_GOOG_API_CLIENT: HeaderName = HeaderName::from_static("x-goog-api-client");

/// Strips all reserved system and telemetry keys from the provided custom headers map.
///
/// This is a shared utility across both HTTP and gRPC transport layers, with end-to-end
/// sanitization behavior covered by transport test suites on both sides.
pub(crate) fn sanitize_custom_headers(headers: &mut HeaderMap) {
    for key in [
        http::header::USER_AGENT,
        http::header::AUTHORIZATION,
        X_GOOG_API_KEY,
        X_GOOG_USER_PROJECT,
        X_GOOG_REQUEST_PARAMS,
        X_GOOG_API_CLIENT,
    ] {
        headers.remove(key);
    }
}
