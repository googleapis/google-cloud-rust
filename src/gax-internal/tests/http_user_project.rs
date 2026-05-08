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
    use http::{HeaderMap, HeaderValue};
    use serde_json::json;

    const X_GOOG_USER_PROJECT: &str = "x-goog-user-project";
    const CRED_QUOTA_PROJECT: &str = "cred_quota_project";
    const USER_QUOTA_PROJECT: &str = "project_lazy_dog";

    #[tokio::test]
    async fn user_project_emits_header() -> anyhow::Result<()> {
        let (endpoint, _server) = echo_server::start().await?;
        let client = echo_server::builder(endpoint)
            .with_credentials(Credentials::from(mock_credentials()))
            .build()
            .await?;

        let builder = client.builder(reqwest::Method::GET, "/echo".into());
        let mut options = RequestOptions::default();
        options.set_quota_project(USER_QUOTA_PROJECT);
        let response: serde_json::Value = client
            .execute(builder, Some(json!({})), options)
            .await?
            .into_body();
        assert_eq!(
            get_header_value(&response, X_GOOG_USER_PROJECT).as_deref(),
            Some(USER_QUOTA_PROJECT),
            "{response:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn no_user_project_no_header() -> anyhow::Result<()> {
        let (endpoint, _server) = echo_server::start().await?;
        let client = echo_server::builder(endpoint)
            .with_credentials(Credentials::from(mock_credentials()))
            .build()
            .await?;

        let builder = client.builder(reqwest::Method::GET, "/echo".into());
        let response: serde_json::Value = client
            .execute(builder, Some(json!({})), RequestOptions::default())
            .await?
            .into_body();
        assert!(
            get_header_value(&response, X_GOOG_USER_PROJECT).is_none(),
            "{response:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn user_project_strips_credential_quota_project() -> anyhow::Result<()> {
        let (endpoint, _server) = echo_server::start().await?;

        let mut mock = MockCredentials::new();
        mock.expect_headers().returning(|_exts| {
            let mut map = HeaderMap::new();
            map.insert(
                http::header::AUTHORIZATION,
                HeaderValue::from_static("Bearer test-token"),
            );
            map.insert(
                X_GOOG_USER_PROJECT,
                HeaderValue::from_static(CRED_QUOTA_PROJECT),
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
        options.set_quota_project(USER_QUOTA_PROJECT);
        let response: serde_json::Value = client
            .execute(builder, Some(json!({})), options)
            .await?
            .into_body();

        assert_eq!(
            get_header_value(&response, X_GOOG_USER_PROJECT).as_deref(),
            Some(USER_QUOTA_PROJECT),
            "{response:?}"
        );
        let headers = response.get("headers").and_then(|h| h.as_object());
        let leaked = headers
            .map(|h| h.values().any(|v| v.as_str() == Some(CRED_QUOTA_PROJECT)))
            .unwrap_or(false);
        assert!(
            !leaked,
            "credential's quota_project value leaked onto the wire: {response:?}"
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
