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

use crate::options::ClientConfig;
use google_cloud_auth::credentials::{
    Builder as CredentialsBuilder, CacheableResource, Credentials,
};
use google_cloud_gax::Result;
use google_cloud_gax::client_builder::{Error as BuilderError, Result as ClientBuilderResult};
use google_cloud_gax::error::Error;
use google_cloud_gax::options::RequestOptions;
use google_cloud_gax::options::internal::RequestOptionsExt as _;
use http::{HeaderMap, header::HeaderName};

const X_GOOG_API_CLIENT: HeaderName = HeaderName::from_static("x-goog-api-client");
const X_GOOG_REQUEST_PARAMS: HeaderName = HeaderName::from_static("x-goog-request-params");
const X_GOOG_USER_PROJECT: HeaderName = HeaderName::from_static("x-goog-user-project");

/// Extends the supplied `headers` map with authentication headers from a
/// `Credentials` object. For entries with the same header name, the one in
/// `headers` takes precedence.
pub(crate) async fn add_auth_headers(
    headers: HeaderMap,
    credentials: &Credentials,
) -> Result<HeaderMap> {
    let h = credentials
        .headers(http::Extensions::new())
        .await
        .map_err(Error::authentication)?;

    let CacheableResource::New { mut data, .. } = h else {
        unreachable!("headers are not cached");
    };

    // Note that client headers override credential headers (e.g. for `x-goog-user-project`).
    data.extend(headers);
    Ok(data)
}

/// Returns a clone of `Credentials` if already present in `config`;
/// otherwise, returns a new default `Credentials` object.
pub(crate) fn make_credentials(config: &ClientConfig) -> ClientBuilderResult<Credentials> {
    if let Some(c) = config.cred.clone() {
        return Ok(c);
    }

    CredentialsBuilder::default()
        .build()
        .map_err(BuilderError::cred)
}

/// Constructs the headers required for Google Cloud API requests.
/// Custom headers can be provided through `RequestOptions`.
/// Returns an error if any of the header values fail to parse.
pub(crate) fn make_headers(
    api_client_header: &'static str,
    request_params: &str,
    options: &RequestOptions,
) -> Result<HeaderMap> {
    let mut headers = HeaderMap::new();

    if let Some(custom_headers) = options.get_extension::<HeaderMap>() {
        headers.extend(custom_headers.clone());
    }

    // Sanitize user custom headers by stripping away any keys conflicting with system headers.
    for key in [
        http::header::USER_AGENT,
        X_GOOG_USER_PROJECT,
        X_GOOG_REQUEST_PARAMS,
    ] {
        headers.remove(key);
    }

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

    headers.insert(
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
        // It also specifies that multiple parameters must be sent as URL-encoded
        // key=value pairs separated by an ampersand, which means there should
        // only ever be a single X-Goog-Request-Params header.
        headers.insert(
            X_GOOG_REQUEST_PARAMS,
            http::header::HeaderValue::from_str(request_params).map_err(Error::ser)?,
        );
    }

    Ok(headers)
}

#[cfg(test)]
mod tests {
    use super::*;
    use google_cloud_auth::credentials::{CacheableResource, CredentialsProvider, EntityTag};
    use google_cloud_auth::errors::CredentialsError;
    use http::{Extensions, header::HeaderName, header::HeaderValue};
    use pretty_assertions::assert_eq;

    type AuthResult<T> = std::result::Result<T, CredentialsError>;
    type TestResult = anyhow::Result<()>;

    mockall::mock! {
        #[derive(Debug)]
        Credentials {}

        impl CredentialsProvider for Credentials {
            async fn headers(&self, extensions: Extensions) -> AuthResult<CacheableResource<HeaderMap>>;
            async fn universe_domain(&self) -> Option<String>;
        }
    }

    const API_CLIENT_HEADER: &str = "test-client/1.0";

    #[tokio::test]
    async fn add_auth_headers_merges_auth_and_client_headers() -> TestResult {
        // Arrange
        let credential_auth = "authorization";
        let credential_token = "bearer test-token";
        let credential_project = "credential-quota-project";

        let request_project = "request-quota-project";
        let request_header = "x-request-header";
        let request_value = "request-value";

        let auth_headers = HeaderMap::from_iter([
            (
                HeaderName::from_static(credential_auth),
                HeaderValue::from_static(credential_token),
            ),
            (
                X_GOOG_USER_PROJECT,
                HeaderValue::from_static(credential_project),
            ),
        ]);

        let mut provider = MockCredentials::new();
        provider.expect_headers().return_once(|_extensions| {
            Ok(CacheableResource::New {
                entity_tag: EntityTag::default(),
                data: auth_headers,
            })
        });
        let credentials = Credentials::from(provider);

        let mut request_headers = HeaderMap::new();
        request_headers.insert(
            // This one should take precedence.
            X_GOOG_USER_PROJECT,
            HeaderValue::from_static(request_project),
        );
        request_headers.insert(
            HeaderName::from_static(request_header),
            HeaderValue::from_static(request_value),
        );

        // Act
        let headers = add_auth_headers(request_headers, &credentials).await?;

        // Assert
        assert_eq!(
            headers.get(credential_auth).expect("auth header"),
            credential_token
        );
        assert_eq!(
            headers
                .get(&X_GOOG_USER_PROJECT)
                .expect("user project header"),
            request_project
        );
        assert_eq!(
            headers.get(request_header).expect("request header"),
            request_value
        );
        Ok(())
    }

    #[tokio::test]
    async fn make_credentials_uses_config_credentials() -> TestResult {
        // No good way to directly check which credentials are used, so
        // we check it indirectly.
        // Arrange
        let expected_domain = "domain";

        let mut provider = MockCredentials::new();
        provider
            .expect_universe_domain()
            .times(1)
            .return_once(|| Some(expected_domain.to_string()));
        let credentials = Credentials::from(provider);

        let mut config = ClientConfig::default();
        config.cred = Some(credentials);

        // Act
        let result = make_credentials(&config)?;

        // Assert
        assert_eq!(
            result.universe_domain().await.as_deref(),
            Some(expected_domain)
        );
        Ok(())
    }

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

    fn test_custom_headers() -> HeaderMap {
        let mut custom_headers = HeaderMap::new();
        // Try to override system headers with conflicting custom values
        custom_headers.insert(
            http::header::USER_AGENT,
            HeaderValue::from_static("custom-agent"),
        );
        custom_headers.insert(
            X_GOOG_USER_PROJECT,
            HeaderValue::from_static("custom-project"),
        );
        custom_headers.insert(X_GOOG_API_CLIENT, HeaderValue::from_static("custom-client"));
        custom_headers.insert(
            X_GOOG_REQUEST_PARAMS,
            HeaderValue::from_static("custom-params"),
        );
        // A legitimate custom header
        custom_headers.insert(
            "x-legitimate-header",
            HeaderValue::from_static("legitimate-value"),
        );
        custom_headers
    }

    #[test]
    fn make_headers_enforces_system_precedence_with_values() -> TestResult {
        // Arrange
        const TEST_USER_AGENT: &str = "system-user-agent/v1.2.3";
        const TEST_QUOTA_PROJECT: &str = "system-quota-project";
        const TEST_REQUEST_PARAMS: &str = "resource=projects%2Ftest";

        let mut options = RequestOptions::default();
        options.set_user_agent(TEST_USER_AGENT);
        options.set_quota_project(TEST_QUOTA_PROJECT);
        let options = options.insert_extension(test_custom_headers());

        // Act
        let headers = make_headers(API_CLIENT_HEADER, TEST_REQUEST_PARAMS, &options)?;

        // Assert
        let expected = HeaderMap::from_iter([
            (
                http::header::USER_AGENT,
                HeaderValue::from_static(TEST_USER_AGENT),
            ),
            (
                X_GOOG_USER_PROJECT,
                HeaderValue::from_static(TEST_QUOTA_PROJECT),
            ),
            (
                X_GOOG_REQUEST_PARAMS,
                HeaderValue::from_static(TEST_REQUEST_PARAMS),
            ),
            (
                X_GOOG_API_CLIENT,
                HeaderValue::from_static(API_CLIENT_HEADER),
            ),
            (
                HeaderName::from_static("x-legitimate-header"),
                HeaderValue::from_static("legitimate-value"),
            ),
        ]);
        assert_eq!(headers, expected);

        Ok(())
    }

    #[test]
    fn make_headers_enforces_system_precedence_without_values() -> TestResult {
        // Arrange
        let options = RequestOptions::default().insert_extension(test_custom_headers());

        // Act (pass empty request params, empty user agent, empty quota project)
        let headers = make_headers(API_CLIENT_HEADER, "", &options)?;

        // Assert
        let expected = HeaderMap::from_iter([
            (
                X_GOOG_API_CLIENT,
                HeaderValue::from_static(API_CLIENT_HEADER),
            ),
            (
                HeaderName::from_static("x-legitimate-header"),
                HeaderValue::from_static("legitimate-value"),
            ),
        ]);
        assert_eq!(headers, expected);

        Ok(())
    }
}
