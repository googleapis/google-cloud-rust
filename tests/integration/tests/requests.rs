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

    #[tokio::test(flavor = "multi_thread")]
    async fn http_body_request_receive_trigger_webhook() -> anyhow::Result<()> {
        use google_cloud_api::model::HttpBody;
        use google_cloud_build_v1::client::CloudBuild;

        let raw_payload = b"raw-webhook-payload";
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::method_path(
                    "POST",
                    "/v1/projects/test-project/triggers/test-trigger:webhook",
                ),
                request::headers(contains(("content-type", "application/x-custom-webhook"))),
                request::body(raw_payload.as_slice()),
            ])
            .respond_with(
                status_code(200)
                    .insert_header("content-type", "application/json")
                    .body("{}"),
            ),
        );

        let endpoint = server.url_str("");
        let client = CloudBuild::builder()
            .with_endpoint(endpoint.trim_end_matches('/'))
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        client
            .receive_trigger_webhook()
            .set_project_id("test-project")
            .set_trigger("test-trigger")
            .set_body(
                HttpBody::new()
                    .set_content_type("application/x-custom-webhook")
                    .set_data(bytes::Bytes::from_static(raw_payload)),
            )
            .send()
            .await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn http_body_response_binary_payload() -> anyhow::Result<()> {
        let binary_response = b"\x89PNG\r\n\x1a\nraw-binary-bytes";
        let server = Server::run();
        server.expect(
            Expectation::matching(request::method_path(
                "POST",
                "/v1/projects/test-project/locations/us-central1/endpoints/test-endpoint:rawPredict",
            ))
            .respond_with(
                status_code(200)
                    .insert_header("content-type", "image/png")
                    .body(binary_response.as_slice()),
            ),
        );

        let endpoint = server.url_str("");
        let client = PredictionService::builder()
            .with_endpoint(endpoint.trim_end_matches('/'))
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        let response = client
            .raw_predict()
            .set_endpoint("projects/test-project/locations/us-central1/endpoints/test-endpoint")
            .send()
            .await?;

        assert_eq!(response.content_type, "image/png");
        assert_eq!(response.data.as_ref(), binary_response);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn http_body_response_json_payload() -> anyhow::Result<()> {
        let json_response = br#"{"predictions": [1, 2, 3]}"#;
        let server = Server::run();
        server.expect(
            Expectation::matching(request::method_path(
                "POST",
                "/v1/projects/test-project/locations/us-central1/endpoints/test-endpoint:rawPredict",
            ))
            .respond_with(
                status_code(200)
                    .insert_header("content-type", "application/json")
                    .body(json_response.as_slice()),
            ),
        );

        let endpoint = server.url_str("");
        let client = PredictionService::builder()
            .with_endpoint(endpoint.trim_end_matches('/'))
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        let response = client
            .raw_predict()
            .set_endpoint("projects/test-project/locations/us-central1/endpoints/test-endpoint")
            .send()
            .await?;

        assert_eq!(response.content_type, "application/json");
        assert_eq!(response.data.as_ref(), json_response);
        Ok(())
    }
}
