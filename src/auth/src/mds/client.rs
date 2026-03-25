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

use crate::errors::CredentialsError;
use crate::io::{HttpRequest, HttpResponse, SharedEnvProvider, SharedHttpClientProvider};
use crate::token::Token;
use std::time::Duration;
use tokio::time::Instant;

/// A client for GCP Compute Engine Metadata Service (MDS).
#[derive(Clone, Debug)]
pub(crate) struct Client {
    endpoint: String,
    /// True if the endpoint was NOT overridden by env var or constructor arg.
    pub(crate) is_default_endpoint: bool,
    http: SharedHttpClientProvider,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub(crate) struct MDSTokenResponse {
    pub(crate) access_token: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) expires_in: Option<u64>,
    pub(crate) token_type: String,
}

impl Client {
    /// Creates a new client for the Metadata Service.
    pub(crate) fn new(
        endpoint_override: Option<String>,
        env: SharedEnvProvider,
        http: SharedHttpClientProvider,
    ) -> Self {
        let (endpoint, is_default_endpoint) = Self::resolve_endpoint(endpoint_override, &env);
        let endpoint = endpoint.trim_end_matches('/').to_string();

        Self {
            endpoint,
            is_default_endpoint,
            http,
        }
    }

    /// Determine the endpoint and whether it was overridden
    fn resolve_endpoint(
        endpoint_override: Option<String>,
        env: &SharedEnvProvider,
    ) -> (String, bool) {
        if let Some(host) = env.var(super::GCE_METADATA_HOST_ENV_VAR) {
            // Check GCE_METADATA_HOST environment variable first
            (format!("http://{host}"), false)
        } else if let Some(e) = endpoint_override {
            // Else, check if an endpoint was provided to the mds::Builder
            (e, false)
        } else {
            // Else, use the default metadata root
            (super::METADATA_ROOT.to_string(), true)
        }
    }

    /// Creates a GET request to the MDS service with the correct headers.
    fn get(&self, path: &str) -> HttpRequest {
        let url = format!("{}{}", self.endpoint, path);
        HttpRequest::get(url).header(super::METADATA_FLAVOR, super::METADATA_FLAVOR_VALUE)
    }

    /// Executes an HTTP request via the provider and maps transport errors.
    async fn execute(
        &self,
        request: HttpRequest,
        error_message: &str,
    ) -> crate::Result<HttpResponse> {
        let response = self
            .http
            .execute(request)
            .await
            .map_err(|e| crate::errors::from_http_error(e, error_message))?;

        Self::check_response_status(response, error_message)
    }

    /// Fetches an access token for the default service account.
    pub(crate) async fn access_token(&self, scopes: Option<Vec<String>>) -> crate::Result<Token> {
        let path = format!("{}/token", super::MDS_DEFAULT_URI);
        let request = self.get(&path);

        // Use the `scopes` option if set, otherwise let the MDS use the default
        // scopes.
        let scopes = scopes.as_ref().map(|v| v.join(","));
        let request = scopes
            .into_iter()
            .fold(request, |r, s| r.query("scopes", s));

        let error_message = "failed to fetch access token";

        // If the connection to MDS was not successful, it is useful to retry when really
        // running on MDS environments and not useful if there is no MDS. We will mark the error
        // as retryable and let the retry policy determine whether to retry or not. Whenever we
        // define a default retry policy, we can skip retrying this case.
        let response = self.execute(request, error_message).await?;

        let mds_response: MDSTokenResponse = response.json().map_err(|e| {
            // Decoding errors are not transient. Typically they indicate a badly
            // configured MDS endpoint, or DNS redirecting the request to a random
            // server, e.g., ISPs that redirect unknown services to HTTP.
            CredentialsError::from_source(e.is_io(), e)
        })?;

        Ok(Token {
            token: mds_response.access_token,
            token_type: mds_response.token_type,
            expires_at: mds_response
                .expires_in
                .map(|d| Instant::now() + Duration::from_secs(d)),
            metadata: None,
        })
    }

    /// Fetches an ID token for the default service account.
    /// Used by idtoken feature.
    #[cfg(feature = "idtoken")]
    pub(crate) async fn id_token(
        &self,
        target_audience: &str,
        format: Option<String>,
        licenses: Option<String>,
    ) -> crate::Result<String> {
        let path = format!("{}/identity", super::MDS_DEFAULT_URI);
        let request = self.get(&path).query("audience", target_audience);
        let request = format.iter().fold(request, |r, f| r.query("format", f));
        let request = licenses.iter().fold(request, |r, l| r.query("licenses", l));

        let error_message = "failed to fetch id token";

        let response = self.execute(request, error_message).await?;

        let token = response
            .text()
            .map_err(|e| CredentialsError::from_source(false, e))?;

        Ok(token)
    }

    /// Fetches the email address of the service account from the Metadata Service.
    pub(crate) async fn email(&self) -> crate::Result<String> {
        let path = format!("{}/email", super::MDS_DEFAULT_URI);
        let request = self.get(&path);
        let error_message = "failed to fetch email";

        let response = self.execute(request, error_message).await?;

        let email = response
            .text()
            .map_err(|e| CredentialsError::from_source(false, e))?;

        Ok(email)
    }

    fn check_response_status(
        response: HttpResponse,
        error_message: &str,
    ) -> crate::Result<HttpResponse> {
        if !response.is_success() {
            Err(crate::errors::from_http_response(&response, error_message))
        } else {
            Ok(response)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::{SharedEnvProvider, SharedHttpClientProvider};
    use crate::mds::MDS_DEFAULT_URI;
    use httptest::{Expectation, Server, matchers::*, responders::*};
    use scoped_env::ScopedEnv;
    use serial_test::{parallel, serial};

    fn new_test_client(endpoint: Option<String>) -> Client {
        Client::new(
            endpoint,
            SharedEnvProvider::default(),
            SharedHttpClientProvider::default(),
        )
    }

    #[tokio::test]
    #[parallel]
    async fn test_access_token_success() {
        let server = Server::run();
        let client = new_test_client(Some(format!("http://{}", server.addr())));
        let response = MDSTokenResponse {
            access_token: "test-token".to_string(),
            expires_in: Some(3600),
            token_type: "Bearer".to_string(),
        };

        server.expect(
            Expectation::matching(all_of![
                request::method("GET"),
                request::path(format!("{}/token", MDS_DEFAULT_URI)),
                request::query(url_decoded(contains((
                    "scopes",
                    "scope1,scope2".to_string()
                )))),
            ])
            .respond_with(
                status_code(200)
                    .insert_header("Content-Type", "application/json")
                    .body(serde_json::to_string(&response).unwrap()),
            ),
        );

        let token = client
            .access_token(Some(vec!["scope1".to_string(), "scope2".to_string()]))
            .await
            .unwrap();
        assert_eq!(token.token, "test-token");
        assert_eq!(token.token_type, "Bearer");
    }

    #[tokio::test]
    #[parallel]
    async fn test_access_token_failure() {
        let server = Server::run();
        let client = new_test_client(Some(format!("http://{}", server.addr())));

        server.expect(
            Expectation::matching(all_of![
                request::method("GET"),
                request::path(format!("{}/token", MDS_DEFAULT_URI)),
            ])
            .respond_with(status_code(404).body("Not Found")),
        );

        let err = client.access_token(None).await.unwrap_err();
        assert!(err.to_string().contains("failed to fetch access token"));
    }

    #[tokio::test]
    #[parallel]
    #[cfg(feature = "idtoken")]
    async fn test_id_token_success() {
        let server = Server::run();
        let client = new_test_client(Some(format!("http://{}", server.addr())));

        server.expect(
            Expectation::matching(all_of![
                request::method("GET"),
                request::path(format!("{}/identity", MDS_DEFAULT_URI)),
                request::query(url_decoded(contains(("audience", "test-aud".to_string())))),
                request::query(url_decoded(contains(("format", "full".to_string())))),
                request::query(url_decoded(contains(("licenses", "TRUE".to_string())))),
            ])
            .respond_with(status_code(200).body("test-id-token")),
        );

        let token = client
            .id_token(
                "test-aud",
                Some("full".to_string()),
                Some("TRUE".to_string()),
            )
            .await
            .unwrap();
        assert_eq!(token, "test-id-token");
    }

    #[tokio::test]
    #[parallel]
    #[cfg(feature = "idtoken")]
    async fn test_id_token_failure() {
        let server = Server::run();
        let client = new_test_client(Some(format!("http://{}", server.addr())));

        server.expect(
            Expectation::matching(all_of![
                request::method("GET"),
                request::path(format!("{}/identity", MDS_DEFAULT_URI)),
            ])
            .respond_with(status_code(404).body("Not Found")),
        );

        let err = client.id_token("test-aud", None, None).await.unwrap_err();
        assert!(err.to_string().contains("failed to fetch id token"));
    }

    #[tokio::test]
    #[parallel]
    async fn test_email_success() {
        let server = Server::run();
        let client = new_test_client(Some(format!("http://{}", server.addr())));

        server.expect(
            Expectation::matching(all_of![
                request::method("GET"),
                request::path(format!("{}/email", MDS_DEFAULT_URI)),
            ])
            .respond_with(status_code(200).body("test@example.com")),
        );

        let email = client.email().await.unwrap();
        assert_eq!(email, "test@example.com");
    }

    #[tokio::test]
    #[parallel]
    async fn test_email_failure() {
        let server = Server::run();
        let client = new_test_client(Some(format!("http://{}", server.addr())));

        server.expect(
            Expectation::matching(all_of![
                request::method("GET"),
                request::path(format!("{}/email", MDS_DEFAULT_URI)),
            ])
            .respond_with(status_code(404).body("Not Found")),
        );

        let err = client.email().await.unwrap_err();
        assert!(err.to_string().contains("failed to fetch email"));
    }

    #[test]
    #[parallel]
    fn test_resolve_endpoint_default() {
        let client = new_test_client(None);
        assert_eq!(client.endpoint, "http://metadata.google.internal");
    }

    #[test]
    #[parallel]
    fn test_resolve_endpoint_override() {
        let client = new_test_client(Some("http://custom.endpoint".to_string()));
        assert_eq!(client.endpoint, "http://custom.endpoint");
    }

    #[test]
    #[serial]
    fn test_resolve_endpoint_env_var() {
        let _s = ScopedEnv::set(super::super::GCE_METADATA_HOST_ENV_VAR, "env.var.host");
        let client = new_test_client(None);
        assert_eq!(client.endpoint, "http://env.var.host");
    }

    #[test]
    #[serial]
    fn test_resolve_endpoint_priority() {
        let _s = ScopedEnv::set(super::super::GCE_METADATA_HOST_ENV_VAR, "env.priority.host");
        // Env var should take precedence over constructor argument
        let client = new_test_client(Some("http://custom.endpoint".to_string()));
        assert_eq!(client.endpoint, "http://env.priority.host");
    }
}
