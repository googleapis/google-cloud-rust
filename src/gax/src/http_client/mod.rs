// Copyright 2024 Google LLC
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

use retry::backoff_policy::BackoffPolicy;
use retry::error::Error;
use retry::error::HttpError;
use retry::error::ServiceError;
use retry::exponential_backoff::ExponentialBackoff;
use retry::loop_state::LoopState;
use retry::options;
use retry::polling_backoff_policy::PollingBackoffPolicy;
use retry::polling_policy::Aip194Strict;
use retry::polling_policy::PollingPolicy;
use retry::retry_client;
use retry::retry_policy::RetryPolicy;
use retry::retry_throttler::RetryThrottlerWrapped;
use retry::Result;
use auth::credentials::{create_access_token_credential, Credential};
use std::sync::Arc;
use std::future::Future;
use std::pin::Pin;
use retry::retry_client::GetAuthHeaders;
use http::header::{HeaderName, HeaderValue};

#[derive(Clone, Debug)]
pub struct ReqwestClient {
    inner: reqwest::Client,
    cred: Credential,
    endpoint: String,
    retry_client: retry::retry_client::RetryClient,
}

impl ReqwestClient {
    pub async fn new(config: ClientConfig, default_endpoint: &str) -> Result<Self> {
        let inner = reqwest::Client::new();
        let endpoint = config
            .endpoint
            .unwrap_or_else(|| default_endpoint.to_string());
        let retry_client = retry::retry_client::RetryClient {
            retry_policy: config.retry_policy,
            backoff_policy: config.backoff_policy,
            retry_throttler: config.retry_throttler,
            polling_policy: config.polling_policy,
            polling_backoff_policy: config.polling_backoff_policy,
        };
        let cred = if let Some(c) = config.cred {
            c
        } else {
            create_access_token_credential(retry_client.clone())
                .await
                .map_err(Error::authentication)?
        };
        Ok(Self {
            inner,
            cred,
            endpoint,
            retry_client
        })
    }

    pub fn builder(&self, method: reqwest::Method, path: String) -> reqwest::RequestBuilder {
        self.inner
            .request(method, format!("{}{path}", &self.endpoint))
    }

    pub async fn execute<I: serde::ser::Serialize, O: serde::de::DeserializeOwned>(
        &self,
        mut builder: reqwest::RequestBuilder,
        body: Option<I>,
        options: retry::options::RequestOptions,
    ) -> Result<O> {
        if let Some(user_agent) = options.user_agent() {
            builder = builder.header(
                reqwest::header::USER_AGENT,
                reqwest::header::HeaderValue::from_str(user_agent).map_err(Error::other)?,
            );
        }
        if let Some(body) = body {
            builder = builder.json(&body);
        }

        let auth_headers = self
            .cred
            .get_headers()
            .await
            .map_err(Error::authentication)?;
        for header in auth_headers.into_iter() {
            builder = builder.header(header.0, header.1);
        }
        
        self.retry_client.request(builder, &options).await
    }   
}

#[derive(serde::Serialize)]
pub struct NoBody {}


pub type ClientConfig = crate::client_config::ClientConfig;

#[cfg(test)]
mod test {
    use super::*;
    use std::collections::HashMap;
    type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

    #[test]
    fn headers_empty() -> TestResult {
        let http_resp = http::Response::builder()
            .status(reqwest::StatusCode::OK)
            .body("")?;
        let response: reqwest::Response = http_resp.into();
        let got = retry::retry_client::convert_headers(response.headers());
        assert!(got.is_empty(), "{got:?}");
        Ok(())
    }

    #[test]
    fn headers_basic() -> TestResult {
        let http_resp = http::Response::builder()
            .header("content-type", "application/json")
            .header("x-test-k1", "v1")
            .status(reqwest::StatusCode::OK)
            .body("")?;
        let response: reqwest::Response = http_resp.into();
        let got = retry::retry_client::convert_headers(response.headers());
        let want = HashMap::from(
            [("content-type", "application/json"), ("x-test-k1", "v1")]
                .map(|(k, v)| (k.to_string(), v.to_string())),
        );
        assert_eq!(got, want);
        Ok(())
    }

    #[test]
    fn headers_sensitive() -> TestResult {
        let sensitive = {
            let mut h = reqwest::header::HeaderValue::from_static("abc123");
            h.set_sensitive(true);
            h
        };
        let http_resp = http::Response::builder()
            .header("content-type", "application/json")
            .header("x-test-k1", "v1")
            .header("x-sensitive", sensitive)
            .status(reqwest::StatusCode::OK)
            .body("")?;
        let response: reqwest::Response = http_resp.into();
        let got = retry::retry_client::convert_headers(response.headers());
        let want = HashMap::from(
            [
                ("content-type", "application/json"),
                ("x-test-k1", "v1"),
                ("x-sensitive", retry::retry_client::SENSITIVE_HEADER),
            ]
            .map(|(k, v)| (k.to_string(), v.to_string())),
        );
        assert_eq!(got, want);
        Ok(())
    }

    // #[tokio::test]
    // async fn client_http_error_bytes() -> TestResult {
    //     let http_resp = http::Response::builder()
    //         .header("Content-Type", "application/json")
    //         .status(400)
    //         .body(r#"{"error": "bad request"}"#)?;
    //     let response: reqwest::Response = http_resp.into();
    //     assert!(response.status().is_client_error());
    //     let response = retry::retry_client::to_http_error::<()>(response).await;
    //     assert!(response.is_err(), "{response:?}");
    //     let err = response.err().unwrap();
    //     let err = err.as_inner::<HttpError>().unwrap();
    //     assert_eq!(err.status_code(), 400);
    //     let want = HashMap::from(
    //         [("content-type", "application/json")].map(|(k, v)| (k.to_string(), v.to_string())),
    //     );
    //     assert_eq!(err.headers(), &want);
    //     assert_eq!(
    //         err.payload(),
    //         Some(bytes::Bytes::from(r#"{"error": "bad request"}"#)).as_ref()
    //     );
    //     Ok(())
    // }

    // #[tokio::test]
    // async fn client_error_with_status() -> TestResult {
    //     use retry::error::rpc::*;
    //     use retry::error::ServiceError;
    //     let status = Status {
    //         code: 404,
    //         message: "The thing is not there, oh noes!".to_string(),
    //         status: Some("NOT_FOUND".to_string()),
    //         details: vec![StatusDetails::LocalizedMessage(
    //             rpc::model::LocalizedMessage::default()
    //                 .set_locale("en-US")
    //                 .set_message("we searched everywhere, honest"),
    //         )],
    //     };
    //     let body = serde_json::json!({"error": serde_json::to_value(&status)?});
    //     let http_resp = http::Response::builder()
    //         .header("Content-Type", "application/json")
    //         .status(404)
    //         .body(body.to_string())?;
    //     let response: reqwest::Response = http_resp.into();
    //     assert!(response.status().is_client_error());
    //     let response = ReqwestClient::to_http_error::<()>(response).await;
    //     assert!(response.is_err(), "{response:?}");
    //     let err = response.err().unwrap();
    //     let err = err.as_inner::<ServiceError>().unwrap();
    //     assert_eq!(err.status(), &status);
    //     assert_eq!(err.http_status_code(), &Some(404 as u16));
    //     let want = HashMap::from(
    //         [("content-type", "application/json")].map(|(k, v)| (k.to_string(), v.to_string())),
    //     );
    //     assert_eq!(err.headers(), &Some(want));
    //     Ok(())
    // }
}
