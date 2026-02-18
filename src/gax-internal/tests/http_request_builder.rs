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

//! Integration tests for `google-cloud-gax-internal::http::http_request_builder`.

mod mock_credentials;

#[cfg(all(test, feature = "_internal-http-client"))]
mod tests {
    use super::mock_credentials::mock_credentials;
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
    use google_cloud_gax::options::RequestOptions;
    use google_cloud_gax_internal::http::ReqwestClient;
    use google_cloud_gax_internal::http::reqwest::{HeaderValue, Method};
    use google_cloud_gax_internal::options::ClientConfig;
    use http::StatusCode;
    use serde_json::Value;
    use std::collections::BTreeMap;
    use test_case::test_case;

    #[tokio::test]
    async fn errors() -> anyhow::Result<()> {
        let server = httptest::Server::run();
        server.expect(
            httptest::Expectation::matching(httptest::matchers::request::method_path(
                "PUT", "/upload",
            ))
            .respond_with(
                httptest::responders::status_code(308).append_header("Location", "new-location"),
            ),
        );
        server.expect(
            httptest::Expectation::matching(httptest::matchers::request::method_path(
                "GET",
                "/storage/v1/b/my-bucket/o/my-object",
            ))
            .respond_with(httptest::responders::status_code(404).body("NOT FOUND")),
        );
        let mut config = ClientConfig::default();
        config.disable_follow_redirects = true;
        config.cred = Some(Anonymous::new().build());
        let client = ReqwestClient::new(config, &server.url_str("")).await?;

        let builder = client.http_builder_with_url(
            Method::PUT,
            &server.url_str("/upload"),
            "https://test.googleapis.com",
        )?;
        let response = builder.send(RequestOptions::default(), None, 0).await?;
        assert_eq!(response.status(), StatusCode::PERMANENT_REDIRECT);
        assert_eq!(
            response.headers().get("location"),
            Some(&HeaderValue::from_static("new-location"))
        );

        let builder = client.http_builder(Method::GET, "storage/v1/b/my-bucket/o/my-object");
        let response = builder.send(RequestOptions::default(), None, 0).await?;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        Ok(())
    }

    #[test_case(RequestOptions::default(), vec![])]
    #[test_case(user_agent_options(), vec![("user-agent".to_string(), "test-agent/123")])]
    #[tokio::test]
    async fn expected_headers_with_endpoint(
        options: RequestOptions,
        extra_headers: Vec<(String, &str)>,
    ) -> anyhow::Result<()> {
        let (endpoint, _server) = echo_server::start().await?;
        let client = echo_server::builder(endpoint.clone())
            .with_credentials(mock_credentials())
            .build()
            .await?;
        let builder = client.http_builder(Method::GET, "/echo");
        let response = builder
            .send(options, None, 0)
            .await?
            .json::<Value>()
            .await?;
        check_headers(&endpoint, response, extra_headers)?;
        Ok(())
    }

    #[test_case(RequestOptions::default(), vec![])]
    #[test_case(user_agent_options(), vec![("user-agent".to_string(), "test-agent/123")])]
    #[tokio::test]
    async fn expected_headers_with_url(
        options: RequestOptions,
        extra_headers: Vec<(String, &str)>,
    ) -> anyhow::Result<()> {
        let (endpoint, _server) = echo_server::start().await?;
        let client = echo_server::builder(endpoint.clone())
            .with_credentials(mock_credentials())
            .build()
            .await?;
        let builder =
            client.http_builder_with_url(Method::GET, &format!("{endpoint}/echo"), &endpoint)?;
        let response = builder
            .send(options, None, 0)
            .await?
            .json::<Value>()
            .await?;
        check_headers(&endpoint, response, extra_headers)?;
        Ok(())
    }

    fn user_agent_options() -> RequestOptions {
        let mut options = RequestOptions::default();
        options.set_user_agent("test-agent/123");
        options
    }

    #[track_caller]
    fn check_headers(
        endpoint: &str,
        response: Value,
        extra_headers: Vec<(String, &str)>,
    ) -> anyhow::Result<()> {
        let headers = response
            .as_object()
            .and_then(|o| o.get("headers"))
            .and_then(|o| o.as_object())
            .unwrap_or_else(|| panic!("missing `headers` object in response: {response:?}"));
        let got = BTreeMap::from_iter(
            headers
                .iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s))),
        );

        let url = ::reqwest::Url::parse(endpoint)?;
        let want = BTreeMap::from_iter(
            [
                // Magic strings from the `mock_credentials` function.
                ("auth-key-1", "auth-value-1"),
                ("auth-key-2", "auth-value-2"),
                ("host", url.host_str().expect("endpoint has host")),
            ]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .chain(extra_headers),
        );

        let mismatch = want
            .iter()
            .filter(|(k, v)| got.get(*k).is_none_or(|g| g != *v))
            .collect::<Vec<_>>();
        assert!(
            mismatch.is_empty(),
            "mismatch = {mismatch:?}\ngot  = {got:?}\nwant = {want:?}"
        );

        Ok(())
    }
}
