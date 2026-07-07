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

use google_cloud_gax::Result;
use google_cloud_gax::error::Error;
use google_cloud_gax::options::RequestOptions;
use google_cloud_gax::options::internal::RequestOptionsExt as _;
use http::{HeaderMap, header::HeaderName};

const X_GOOG_API_CLIENT: HeaderName = HeaderName::from_static("x-goog-api-client");
const X_GOOG_REQUEST_PARAMS: HeaderName = HeaderName::from_static("x-goog-request-params");
const X_GOOG_USER_PROJECT: HeaderName = HeaderName::from_static("x-goog-user-project");

/// Constructs the headers required for Google Cloud API requests.
/// Custom headers can be provided through `RequestOptions`.
/// Returns an error if any of the header values fail to parse.
pub(crate) fn make_headers(
    api_client_header: &'static str,
    request_params: &str,
    options: &RequestOptions,
) -> Result<HeaderMap> {
    let mut headers = HeaderMap::new();

    if let Some(user_agent) = options.user_agent() {
        headers.insert(
            http::header::USER_AGENT,
            http::header::HeaderValue::from_str(user_agent).map_err(Error::ser)?,
        );
    }

    if let Some(quota_project) = options.quota_project() {
        headers.insert(
            X_GOOG_USER_PROJECT,
            http::header::HeaderValue::from_str(quota_project).map_err(Error::ser)?,
        );
    }

    headers.append(
        X_GOOG_API_CLIENT,
        http::header::HeaderValue::from_static(api_client_header),
    );

    if !request_params.is_empty() {
        // When using routing info to populate the request parameters it is
        // possible that none of the path template matches. AIP-4222 says:
        //
        //     If none of the routing parameters matched their respective
        //     fields, the routing header **must not** be sent.
        //
        headers.append(
            X_GOOG_REQUEST_PARAMS,
            http::header::HeaderValue::from_str(request_params).map_err(Error::ser)?,
        );
    }

    if let Some(custom_headers) = options.get_extension::<HeaderMap>() {
        headers.extend(custom_headers.clone());
    }

    Ok(headers)
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::header::HeaderValue;
    use pretty_assertions::assert_eq;

    type TestResult = anyhow::Result<()>;

    const API_CLIENT_HEADER: &str = "test-client/1.0";

    #[test]
    fn make_headers_with_standard_headers() -> TestResult {
        // Arrange
        const USER_AGENT: &str = "custom-user-agent/v1.2.3";
        const QUOTA_PROJECT: &str = "user-quota-project";
        const REQUEST_PARAMS: &str = "resource=projects%2Ftest";

        let mut options = RequestOptions::default();
        options.set_user_agent(USER_AGENT);
        options.set_quota_project(QUOTA_PROJECT);

        // Act
        let headers = make_headers(API_CLIENT_HEADER, REQUEST_PARAMS, &options)?;

        // Assert
        assert_eq!(headers.get(X_GOOG_API_CLIENT).unwrap(), API_CLIENT_HEADER);
        assert_eq!(headers.get(X_GOOG_REQUEST_PARAMS).unwrap(), REQUEST_PARAMS);
        assert_eq!(headers.get(X_GOOG_USER_PROJECT).unwrap(), QUOTA_PROJECT);
        assert_eq!(headers.get(http::header::USER_AGENT).unwrap(), USER_AGENT);
        Ok(())
    }

    #[test]
    fn make_headers_omits_unset_params() -> TestResult {
        // Act
        let headers = make_headers(API_CLIENT_HEADER, "", &RequestOptions::default())?;

        // Assert
        assert_eq!(headers.get(X_GOOG_API_CLIENT).unwrap(), API_CLIENT_HEADER);
        assert!(headers.get(X_GOOG_REQUEST_PARAMS).is_none(), "{headers:?}");
        assert!(headers.get(X_GOOG_USER_PROJECT).is_none(), "{headers:?}");
        assert!(
            headers.get(http::header::USER_AGENT).is_none(),
            "{headers:?}"
        );
        Ok(())
    }

    #[test]
    fn make_headers_with_custom_headers() -> TestResult {
        // Arrange
        const CUSTOM_HEADER_NAME: &str = "x-custom-header";
        const CUSTOM_HEADER: &str = "custom-value";
        const CUSTOM_REQUEST_PARAMS: &str = "param=1";

        let mut custom_headers = HeaderMap::new();
        custom_headers.insert(CUSTOM_HEADER_NAME, HeaderValue::from_static(CUSTOM_HEADER));

        let options = RequestOptions::default().insert_extension(custom_headers);

        // Act
        let headers = make_headers(API_CLIENT_HEADER, CUSTOM_REQUEST_PARAMS, &options)?;

        // Assert
        assert_eq!(headers.get(X_GOOG_API_CLIENT).unwrap(), API_CLIENT_HEADER);
        assert_eq!(
            headers.get(X_GOOG_REQUEST_PARAMS).unwrap(),
            CUSTOM_REQUEST_PARAMS
        );
        assert_eq!(headers.get(CUSTOM_HEADER_NAME).unwrap(), CUSTOM_HEADER);
        Ok(())
    }

    #[test]
    fn make_headers_with_invalid_header_values() {
        // Invalid user agent
        let mut options = RequestOptions::default();
        options.set_user_agent("invalid\nagent");
        let res = make_headers(API_CLIENT_HEADER, "param=1", &options);
        assert!(res.is_err(), "{res:?}");

        // Invalid quota project
        let mut options = RequestOptions::default();
        options.set_quota_project("invalid\nproject");
        let res = make_headers(API_CLIENT_HEADER, "param=1", &options);
        assert!(res.is_err(), "{res:?}");

        // Invalid request params
        let options = RequestOptions::default();
        let res = make_headers(API_CLIENT_HEADER, "invalid\nparams", &options);
        assert!(res.is_err(), "{res:?}");
    }
}
