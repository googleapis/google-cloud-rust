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

//! Verify generated clients prevent traversal attacks.

#[cfg(test)]
mod tests {
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
    use google_cloud_workflows_v1::client::Workflows;
    use httptest::{Expectation, Server, matchers::*, responders::*};
    use serde_json::json;

    const INPUT_PATH: &str = "projects/p/locations/l/operations/o?$httpMethod=DELETE";
    // Note the percent-encoding for the ? character. The `reqwest::Url` class only encodes the path
    // characters that require encoding, in this case `?`. That is enough to disable any
    // path-traversal shenanigans.
    const WANT_PATH: &str = "/v1/projects/p/locations/l/operations/o%3F$httpMethod=DELETE";

    #[tokio::test(flavor = "multi_thread")]
    async fn traversal_is_prevented() -> anyhow::Result<()> {
        let server = start();
        let endpoint = server.url_str("/");
        let endpoint = endpoint.strip_suffix("/").expect("ends in /");

        let client = Workflows::builder()
            .with_endpoint(endpoint)
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        let _ = client.get_operation().set_name(INPUT_PATH).send().await?;
        Ok(())
    }

    fn start() -> Server {
        let server = Server::run();

        server.expect(
            Expectation::matching(all_of![
                request::method("GET"),
                request::path(WANT_PATH),
                request::query(url_decoded(contains(("$alt", "json;enum-encoding=int"))))
            ])
            .respond_with(json_encoded(json!({}))),
        );

        server
    }
}
