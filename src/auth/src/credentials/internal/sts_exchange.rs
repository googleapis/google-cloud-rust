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

use crate::credentials::errors::CredentialsError;
use base64::Engine;
use serde::Deserialize;
use std::collections::HashMap;

type Result<T> = std::result::Result<T, CredentialsError>;

/// Token Exchange grant type for a sts exchange.
pub(crate) const TOKEN_EXCHANGE_GRANT_TYPE: &str =
    "urn:ietf:params:oauth:grant-type:token-exchange";
/// Refresh Token Exchange grant type for a sts exchange.
pub(crate) const REFRESH_TOKEN_GRANT_TYPE: &str = "refresh_token";
/// TokenType for a sts exchange.
pub(crate) const ACCESS_TOKEN_TYPE: &str = "urn:ietf:params:oauth:token-type:access_token";
/// JWT TokenType for a sts exchange.
pub(crate) const JWT_TOKEN_TYPE: &str = "urn:ietf:params:oauth:token-type:jwt";

/// Handles OAuth2 Secure Token Service (STS) exchange.
/// Reference: https://datatracker.ietf.org/doc/html/rfc8693
struct STSHandler {
    client: reqwest::Client,
}

impl STSHandler {
    pub fn new() -> Self {
        let client = reqwest::Client::new();
        Self { client }
    }

    /// performs the token exchange using a refresh token flow with
    /// the provided [RefreshAccessTokenRequest] information.
    async fn refresh_access_token(&self, req: RefreshAccessTokenRequest) -> Result<TokenResponse> {
        let mut params: HashMap<&str, String> = HashMap::new();
        params.insert("grant_type", REFRESH_TOKEN_GRANT_TYPE.to_string());
        params.insert("refresh_token", req.refresh_token);

        self.execute(req.url, req.authentication, req.headers, params)
            .await
    }

    /// performs an oauth2 token exchange with the provided [ExchangeTokenRequest] information.
    async fn exchange_token(&self, req: ExchangeTokenRequest) -> Result<TokenResponse> {
        let mut params: HashMap<&str, String> = HashMap::new();

        params.insert("grant_type", TOKEN_EXCHANGE_GRANT_TYPE.to_string());
        params.insert("requested_token_type", ACCESS_TOKEN_TYPE.to_string());

        params.insert("subject_token", req.subject_token);
        params.insert("subject_token_type", req.subject_token_type);

        if !req.scope.is_empty() {
            params.insert("scope", req.scope.join(" "));
        }

        if let Some(audience) = req.audience {
            params.insert("audience", audience);
        }
        if let Some(resource) = req.resource {
            params.insert("resource", resource);
        }
        if let Some(actor_token) = req.actor_token {
            params.insert("actor_token", actor_token);
        }
        if let Some(actor_token_type) = req.actor_token_type {
            params.insert("actor_token_type", actor_token_type);
        }

        if let Some(options) = req.extra_options {
            if let Ok(value) = serde_json::to_value(options) {
                params.insert("options", value.to_string());
            }
        }

        self.execute(req.url, req.authentication, req.headers, params)
            .await
    }

    /// execute http request and token exchange
    pub(crate) async fn execute(
        &self,
        url: String,
        client_auth: ClientAuthentication,
        headers: http::HeaderMap,
        params: HashMap<&str, String>,
    ) -> Result<TokenResponse> {
        let mut headers = headers.clone();
        let mut params = params.clone();
        println!("[execute] url: {}", url);
        client_auth.inject_auth(&mut headers, &mut params);
        println!("[execute] headers: {:?}", headers);

        let res = self
            .client
            .post(url)
            .form(&params)
            .headers(headers)
            .send()
            .await
            .map_err(|err| {
                CredentialsError::from_str(false, format!("failed to request token: {}", err))
            })?;

        let status = res.status();
        println!("[execute] status: {:?}", status);
        if !status.is_success() {
            return Err(CredentialsError::from_str(
                false,
                format!("error requesting token, failed with status {status}"),
            ));
        }
        println!("[execute] response: {:?}", res);
        let token_res = res
            .json::<TokenResponse>()
            .await
            .map_err(|err| CredentialsError::new(false, err))?;
        Ok(token_res)
    }
}

/// TokenResponse is used to decode the remote server response during
/// an oauth2 token exchange.
#[derive(Deserialize, Default, PartialEq, Debug)]
pub(crate) struct TokenResponse {
    access_token: String,
    issued_token_type: String,
    token_type: String,
    expires_in: u64,
    scope: String,
    refresh_token: Option<String>,
}

/// Authentication style via headers or form params.
/// See https://datatracker.ietf.org/doc/html/rfc6749#section-2.3.1.
#[derive(Debug, Clone)]
enum ClientAuthStyle {
    InParams,
    InHeader,
}

/// ClientAuthentication represents an OAuth client ID and secret and the
/// mechanism for passing these credentials as stated
/// in https://datatracker.ietf.org/doc/html/rfc6749#section-2.3.1.
#[derive(Clone, Debug)]
pub(crate) struct ClientAuthentication {
    auth_style: ClientAuthStyle,
    client_id: Option<String>,
    client_secret: Option<String>,
}

impl Default for ClientAuthentication {
    fn default() -> Self {
        Self {
            auth_style: ClientAuthStyle::InParams,
            client_id: None,
            client_secret: None,
        }
    }
}

impl ClientAuthentication {
    // Add authentication to a Secure Token Service exchange request.
    // Modifies either the passed headers or form parameters
    // depending on the desired authentication format.
    pub(crate) fn inject_auth(
        &self,
        headers: &mut http::HeaderMap,
        params: &mut HashMap<&str, String>,
    ) {
        if let (Some(client_id), Some(client_secret)) =
            (self.client_id.clone(), self.client_secret.clone())
        {
            match self.auth_style {
                ClientAuthStyle::InHeader => {
                    let plain_header = format!("{}:{}", client_id, client_secret);
                    let encoded =
                        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(plain_header);
                    let header = http::HeaderValue::from_str(format!("Basic {encoded}").as_str());
                    if let Ok(value) = header {
                        headers.insert("Authorization", value);
                    }
                }
                _ => {
                    params.insert("client_id", client_id);
                    params.insert("client_secret", client_secret);
                }
            }
        }
    }
}

/// Information required to perform an oauth2 token exchange with the provided endpoint.
#[derive(Default)]
pub struct ExchangeTokenRequest {
    url: String,
    authentication: ClientAuthentication,
    headers: http::HeaderMap,
    resource: Option<String>,
    subject_token: String,
    subject_token_type: String,
    audience: Option<String>,
    scope: Vec<String>,
    actor_token: Option<String>,
    actor_token_type: Option<String>,
    extra_options: Option<HashMap<String, String>>,
}

/// Information required to perform the token exchange using a refresh token flow.
#[derive(Default)]
pub struct RefreshAccessTokenRequest {
    url: String,
    authentication: ClientAuthentication,
    headers: http::HeaderMap,
    refresh_token: String,
}

#[cfg(test)]
mod test {
    use super::*;
    use httptest::{Expectation, Server, matchers::*, responders::*};
    use tokio_test::assert_err;
    type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

    #[tokio::test]
    async fn exchange_token() -> TestResult {
        let client_auth = ClientAuthentication {
            auth_style: ClientAuthStyle::InHeader,
            client_id: Some("client_id".to_string()),
            client_secret: Some("supersecret".to_string()),
        };
        let response_body = r#"{"access_token":"an_example_token","issued_token_type":"urn:ietf:params:oauth:token-type:access_token","token_type":"Bearer","expires_in":3600,"scope":"https://www.googleapis.com/auth/cloud-platform"}"#;

        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::method_path("POST", "/sts"),
                request::body(url_decoded(contains((
                    "grant_type",
                    TOKEN_EXCHANGE_GRANT_TYPE
                )))),
                request::body(url_decoded(contains(("subject_token", "an_example_token")))),
                request::body(url_decoded(contains((
                    "requested_token_type",
                    ACCESS_TOKEN_TYPE
                )))),
                request::body(url_decoded(contains((
                    "subject_token_type",
                    JWT_TOKEN_TYPE
                )))),
                request::body(url_decoded(contains((
                    "audience",
                    "32555940559.apps.googleusercontent.com"
                )))),
                request::body(url_decoded(contains((
                    "scope",
                    "https://www.googleapis.com/auth/cloud-platform"
                )))),
                request::headers(contains((
                    "authorization",
                    "Basic Y2xpZW50X2lkOnN1cGVyc2VjcmV0"
                ))),
                request::headers(contains((
                    "content-type",
                    "application/x-www-form-urlencoded"
                ))),
            ])
            .respond_with(status_code(200).body(response_body)),
        );

        let url = server.url("/sts");
        let mut headers = http::HeaderMap::new();
        headers.insert(
            http::header::CONTENT_TYPE,
            http::HeaderValue::from_static("application/x-www-form-urlencoded"),
        );
        let token_req = ExchangeTokenRequest {
            url: url.to_string(),
            headers: headers,
            authentication: client_auth,
            audience: Some("32555940559.apps.googleusercontent.com".to_string()),
            scope: ["https://www.googleapis.com/auth/cloud-platform".to_string()].to_vec(),
            subject_token: "an_example_token".to_string(),
            subject_token_type: JWT_TOKEN_TYPE.to_string(),
            ..ExchangeTokenRequest::default()
        };
        let handler = STSHandler::new();
        let resp = handler.exchange_token(token_req).await?;

        assert_eq!(
            resp,
            TokenResponse {
                access_token: "an_example_token".to_string(),
                refresh_token: None,
                issued_token_type: ACCESS_TOKEN_TYPE.to_string(),
                token_type: "Bearer".to_string(),
                expires_in: 3600,
                scope: "https://www.googleapis.com/auth/cloud-platform".to_string(),
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn exchange_token_err() -> TestResult {
        let client_auth = ClientAuthentication {
            auth_style: ClientAuthStyle::InHeader,
            client_id: None,
            client_secret: None,
        };
        let response_body = r#"{"error":"bad request"}"#;

        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::method_path("POST", "/fail"),
                request::body(url_decoded(contains((
                    "grant_type",
                    TOKEN_EXCHANGE_GRANT_TYPE
                )))),
                request::body(url_decoded(contains(("subject_token", "an_example_token")))),
                request::body(url_decoded(contains((
                    "requested_token_type",
                    ACCESS_TOKEN_TYPE
                )))),
                request::body(url_decoded(contains((
                    "subject_token_type",
                    JWT_TOKEN_TYPE
                )))),
                request::headers(contains((
                    "content-type",
                    "application/x-www-form-urlencoded"
                ))),
            ])
            .respond_with(status_code(400).body(response_body)),
        );

        let url = server.url("/fail");
        let headers = http::HeaderMap::new();
        let token_req = ExchangeTokenRequest {
            url: url.to_string(),
            headers: headers,
            authentication: client_auth,
            subject_token: "an_example_token".to_string(),
            subject_token_type: JWT_TOKEN_TYPE.to_string(),
            ..ExchangeTokenRequest::default()
        };
        let handler = STSHandler::new();
        let err = assert_err!(handler.exchange_token(token_req).await);

        let expected_err = crate::errors::CredentialsError::from_str(
            false,
            "error requesting token, failed with status 400 Bad Request",
        );
        assert_eq!(err.to_string(), expected_err.to_string());

        Ok(())
    }

    #[tokio::test]
    async fn refresh_access_token() -> TestResult {
        let client_auth = ClientAuthentication {
            auth_style: ClientAuthStyle::InParams,
            client_id: Some("client_id".to_string()),
            client_secret: Some("supersecret".to_string()),
        };

        let response_body = r#"{"access_token":"an_example_token","issued_token_type":"urn:ietf:params:oauth:token-type:access_token","token_type":"Bearer","expires_in":3600,"scope":"https://www.googleapis.com/auth/cloud-platform"}"#;

        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::method_path("POST", "/refresh_token"),
                request::body(url_decoded(contains((
                    "grant_type",
                    REFRESH_TOKEN_GRANT_TYPE
                )))),
                request::body(url_decoded(contains((
                    "refresh_token",
                    "an_example_refresh_token"
                )))),
                request::body(url_decoded(contains(("client_id", "client_id")))),
                request::body(url_decoded(contains(("client_secret", "supersecret")))),
                request::headers(contains((
                    "content-type",
                    "application/x-www-form-urlencoded"
                ))),
            ])
            .respond_with(status_code(200).body(response_body)),
        );

        let url = server.url("/refresh_token");
        let mut headers = http::HeaderMap::new();
        headers.insert(
            http::header::CONTENT_TYPE,
            http::HeaderValue::from_static("application/x-www-form-urlencoded"),
        );
        let token_req = RefreshAccessTokenRequest {
            url: url.to_string(),
            authentication: client_auth,
            headers: headers,
            refresh_token: "an_example_refresh_token".to_string(),
        };
        let handler = STSHandler::new();
        let resp = handler.refresh_access_token(token_req).await?;

        assert_eq!(
            resp,
            TokenResponse {
                access_token: "an_example_token".to_string(),
                refresh_token: None,
                issued_token_type: ACCESS_TOKEN_TYPE.to_string(),
                token_type: "Bearer".to_string(),
                expires_in: 3600,
                scope: "https://www.googleapis.com/auth/cloud-platform".to_string(),
            }
        );

        Ok(())
    }
}
