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
    use google_cloud_aiplatform_v1::client::PredictionService;
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
    use httptest::{Expectation, Server, matchers::*, responders::*};
    use serde_json::json;

    #[tokio::test(flavor = "multi_thread")]
    async fn post_with_empty_body() -> anyhow::Result<()> {
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
        let endpoint = server.url_str("/ui");

        let client = PredictionService::builder()
            .with_endpoint(&endpoint)
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        client
            .cancel_operation()
            .set_name("projects/test-project/locations/test-locations/operations/test-001")
            .send()
            .await?;
        Ok(())
    }

    // This is a regression test for [#6515]
    //
    // The EmbedContent RPC is defined as follows:
    //
    // ```
    // rpc EmbedContent(EmbedContentRequest) returns (EmbedContentResponse) {
    //   option (google.api.http) = {
    //     post: "/v1/{model=projects/*/locations/*/publishers/*/models/*}:embedContent"
    //     body: "*"
    //   };
    //   option (google.api.method_signature) = "model,content";
    // }
    // ```
    //
    // We want to ensure that the `model` field in the path is not also
    // serialized in the request body.
    //
    // [#6515]: https://github.com/googleapis/google-cloud-rust/issues/6515
    #[tokio::test(flavor = "multi_thread")]
    async fn path_variables_not_in_full_body() -> anyhow::Result<()> {
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::method("POST"),
                request::path(matches("^/ui/")),
                request::body(json_decoded(eq(json!({})))),
            ])
            .respond_with(json_encoded(json!({}))),
        );
        let endpoint = server.url_str("/ui");

        let client = PredictionService::builder()
            .with_endpoint(&endpoint)
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        client
            .embed_content()
            .set_model("projects/test-project/locations/global/publishers/google/models/gemini-embedding-2")
            .send()
            .await?;
        Ok(())
    }
}
