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

mod mock_credentials;

#[cfg(all(test, feature = "_internal-http-client"))]
mod tests {
    use super::mock_credentials::{MockCredentials, mock_credentials};
    use google_cloud_auth::credentials::{CacheableResource, Credentials, EntityTag};
    use google_cloud_gax::options::RequestOptions;
    use google_cloud_gax::options::internal::RequestOptionsExt;
    use http::{HeaderMap, HeaderValue, header::HeaderName};
    use serde_json::json;

    const X_GOOG_USER_PROJECT: &str = "x-goog-user-project";

    fn with_custom_header(
        options: RequestOptions,
        name: &'static str,
        value: &'static str,
    ) -> RequestOptions {
        let mut headers = options
            .get_extension::<HeaderMap>()
            .cloned()
            .unwrap_or_default();
        headers.insert(
            HeaderName::from_static(name),
            HeaderValue::from_static(value),
        );
        options.insert_extension(headers)
    }

    #[tokio::test]
    async fn custom_headers_emit_on_wire() -> anyhow::Result<()> {
        let (endpoint, _server) = echo_server::start().await?;
        let client = echo_server::builder(endpoint)
            .with_credentials(Credentials::from(mock_credentials()))
            .build()
            .await?;

        let builder = client.builder(reqwest::Method::GET, "/echo".into());
        let mut options = RequestOptions::default();
        options = with_custom_header(options, "x-client-tracking-id", "req-12345");
        options = with_custom_header(options, "x-foo", "bar");

        let response: serde_json::Value = client
            .execute(builder, Some(json!({})), options)
            .await?
            .into_body();

        assert_eq!(
            get_header_value(&response, "x-client-tracking-id").as_deref(),
            Some("req-12345"),
            "{response:?}"
        );
        assert_eq!(
            get_header_value(&response, "x-foo").as_deref(),
            Some("bar"),
            "{response:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn system_headers_overwrite_custom_headers() -> anyhow::Result<()> {
        let (endpoint, _server) = echo_server::start().await?;

        let mut mock = MockCredentials::new();
        mock.expect_headers().returning(|_exts| {
            let mut map = HeaderMap::new();
            map.insert(
                http::header::AUTHORIZATION,
                HeaderValue::from_static("Bearer valid-token"),
            );
            Ok(CacheableResource::New {
                data: map,
                entity_tag: EntityTag::default(),
            })
        });
        mock.expect_universe_domain().returning(|| None);

        let client = echo_server::builder(endpoint)
            .with_credentials(Credentials::from(mock))
            .build()
            .await?;

        let builder = client.builder(reqwest::Method::GET, "/echo".into());
        let mut options = RequestOptions::default();
        options = with_custom_header(options, "authorization", "Bearer fake-token");
        options = with_custom_header(options, "user-agent", "fake-agent");
        options = with_custom_header(options, X_GOOG_USER_PROJECT, "fake-project");

        options.set_user_agent("real-sdk-agent");
        options.set_quota_project("real-project");

        let response: serde_json::Value = client
            .execute(builder, Some(json!({})), options)
            .await?
            .into_body();

        assert_eq!(
            get_header_value(&response, "authorization").as_deref(),
            Some("Bearer valid-token"),
            "{response:?}"
        );
        assert_eq!(
            get_header_value(&response, "user-agent").as_deref(),
            Some("real-sdk-agent"),
            "{response:?}"
        );
        assert_eq!(
            get_header_value(&response, X_GOOG_USER_PROJECT).as_deref(),
            Some("real-project"),
            "{response:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn system_headers_unset_strip_custom_headers() -> anyhow::Result<()> {
        let (endpoint, _server) = echo_server::start().await?;
        let client = echo_server::builder(endpoint)
            .with_credentials(Credentials::from(mock_credentials()))
            .build()
            .await?;

        let builder = client.builder(reqwest::Method::GET, "/echo".into());
        let mut options = RequestOptions::default();
        options = with_custom_header(options, "user-agent", "fake-agent");
        options = with_custom_header(options, X_GOOG_USER_PROJECT, "fake-project");

        let response: serde_json::Value = client
            .execute(builder, Some(json!({})), options)
            .await?
            .into_body();

        assert!(
            get_header_value(&response, "user-agent").is_none(),
            "user-agent should be stripped when unset on options: {response:?}"
        );
        assert!(
            get_header_value(&response, X_GOOG_USER_PROJECT).is_none(),
            "x-goog-user-project should be stripped when unset on options: {response:?}"
        );
        Ok(())
    }

    fn get_header_value(response: &serde_json::Value, name: &str) -> Option<String> {
        response
            .as_object()
            .and_then(|o| o.get("headers"))
            .and_then(|h| h.get(name))
            .and_then(|v| v.as_str())
            .map(str::to_string)
    }
}
