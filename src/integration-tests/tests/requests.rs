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

//! Verify generated clients correctly send POST requests with empty bodies.

#[cfg(test)]
mod requests {
    use httptest::{Expectation, Server, matchers::*, responders::*};
    use serde_json::json;

    #[tokio::test(flavor = "multi_thread")]
    async fn post_with_empty_body() -> anyhow::Result<()> {
        let server = start();
        let endpoint = server.url_str("/ui");

        let client = aiplatform::client::PredictionService::builder()
            .with_endpoint(&endpoint)
            .with_credentials(auth::credentials::testing::test_credentials())
            .build()
            .await?;

        client
            .cancel_operation()
            .set_name("projects/test-project/locations/test-locations/operations/test-001")
            .send()
            .await?;
        Ok(())
    }

    fn start() -> Server {
        let server = Server::run();

        server.expect(
            Expectation::matching(all_of![
                request::method("POST"),
                request::path(matches("^/ui/")),
            ])
            .times(0) // should not be called
            .respond_with(json_encoded(json! {"missing content-length"})),
        );

        server.expect(
            Expectation::matching(all_of![
                request::method("POST"),
                request::path(matches("^/ui/")),
                request::headers(contains(key("content-length"))),
            ])
            .respond_with(json_encoded(json!({}))),
        );

        server
    }
}
