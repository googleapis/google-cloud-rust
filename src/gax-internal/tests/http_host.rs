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

#[cfg(all(test, feature = "_internal-http-client"))]
mod tests {
    use google_cloud_auth::credentials::{Credentials, anonymous::Builder as Anonymous};
    use google_cloud_gax::options::*;
    use google_cloud_gax_internal::http::ReqwestClient;
    use serde_json::json;
    use std::str::FromStr;

    fn test_credentials() -> Credentials {
        Anonymous::new().build()
    }

    #[tokio::test]
    async fn host_is_present() -> anyhow::Result<()> {
        let (endpoint, _server) = echo_server::start().await?;
        let client = echo_server::builder(endpoint.clone())
            .with_credentials(test_credentials())
            .build()
            .await?;

        builder_simple(&client, &endpoint).await?;
        // This does not have a retry loop, calling it send deflakes connection
        // problems to the server.
        builder_http(&client, &endpoint).await?;
        Ok(())
    }

    async fn builder_simple(client: &ReqwestClient, endpoint: &str) -> anyhow::Result<()> {
        let builder = client.builder(reqwest::Method::GET, "/echo".into());
        let body = json!({});
        let options = RequestOptions::default();
        let response: serde_json::Value = client
            .execute(builder, Some(body), options)
            .await?
            .into_body();
        let got = get_header_value(&response, "host");
        let uri = http::Uri::from_str(endpoint)?;
        assert_eq!(
            got.as_deref(),
            uri.authority().map(|a| a.host()),
            "{response:?}"
        );
        Ok(())
    }

    async fn builder_http(client: &ReqwestClient, endpoint: &str) -> anyhow::Result<()> {
        let builder = client.http_builder_with_url(
            reqwest::Method::GET,
            &format!("{endpoint}/echo"),
            "https://test.googleapis.com",
        )?;
        let options = RequestOptions::default();
        let response = builder.body("{}").send(options, None, 0).await?;
        let response = response.json::<serde_json::Value>().await?;
        let got = get_header_value(&response, "host");
        assert_eq!(got.as_deref(), Some("test.googleapis.com"), "{response:?}");
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
