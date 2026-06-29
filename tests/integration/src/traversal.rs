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
    use test_case::test_case;

    // A `?` character should be percent-encoded to avoid treating it like a query parameter.
    #[test_case(
        "projects/p/locations/l/operations/o?$httpMethod=DELETE",
        "/v1/projects/p/locations/l/operations/o%3F$httpMethod=DELETE"
    )]
    // A `%` character is passed through. The service is supposed to treat that as part of
    // the path.
    #[test_case(
        "projects/p/locations/l/operations/o%3F$httpMethod=DELETE",
        "/v1/projects/p/locations/l/operations/o%3F$httpMethod=DELETE"
    )]
    #[tokio::test(flavor = "multi_thread")]
    async fn path_is_escaped(name: &str, want_path: &'static str) -> anyhow::Result<()> {
        let server = start(want_path);
        let endpoint = server.url_str("/");
        let endpoint = endpoint.strip_suffix("/").expect("ends in /");

        let client = Workflows::builder()
            .with_endpoint(endpoint)
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        let _ = client.get_operation().set_name(name).send().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn additional_components_are_rejected() -> anyhow::Result<()> {
        let client = Workflows::builder()
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        // The generated code rejects any name with more components than expected.
        let result = client
            .list_operations()
            .set_name("projects/p/locations/l/operations/too-much")
            .send()
            .await;
        assert!(matches!(result, Err(ref e) if e.is_binding()), "{result:?}");
        Ok(())
    }

    fn start(path: &'static str) -> Server {
        let server = Server::run();

        server.expect(
            Expectation::matching(all_of![
                request::method("GET"),
                request::path(path),
                request::query(url_decoded(contains(("$alt", "json;enum-encoding=int"))))
            ])
            .respond_with(json_encoded(json!({}))),
        );

        server
    }
}
