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

#[cfg(all(test, feature = "_internal-grpc-client"))]
mod tests {
    use super::mock_credentials::{MockCredentials, mock_credentials};
    use google_cloud_auth::credentials::{CacheableResource, Credentials, EntityTag};
    use google_cloud_gax::Result;
    use google_cloud_gax::options::RequestOptions;
    use google_cloud_gax_internal::grpc;
    use grpc_server::{builder, google, start_echo_server};
    use http::{HeaderMap, HeaderValue};

    const X_GOOG_USER_PROJECT: &str = "x-goog-user-project";
    const CRED_QUOTA_PROJECT: &str = "cred_quota_project";
    const USER_QUOTA_PROJECT: &str = "project_lazy_dog";

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn user_project_emits_header() -> anyhow::Result<()> {
        let (endpoint, _server) = start_echo_server().await?;
        let client = builder(endpoint)
            .with_credentials(mock_credentials())
            .build()
            .await?;

        let mut options = RequestOptions::default();
        options.set_quota_project(USER_QUOTA_PROJECT);
        let response = send_request(client, options).await?;
        assert_eq!(
            response
                .metadata
                .get(X_GOOG_USER_PROJECT)
                .map(String::as_str),
            Some(USER_QUOTA_PROJECT)
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn no_user_project_no_header() -> anyhow::Result<()> {
        let (endpoint, _server) = start_echo_server().await?;
        let client = builder(endpoint)
            .with_credentials(mock_credentials())
            .build()
            .await?;

        let response = send_request(client, RequestOptions::default()).await?;
        assert!(
            !response.metadata.contains_key(X_GOOG_USER_PROJECT),
            "{:?}",
            response.metadata
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn user_project_strips_credential_quota_project() -> anyhow::Result<()> {
        let (endpoint, _server) = start_echo_server().await?;

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

        let client = builder(endpoint)
            .with_credentials(Credentials::from(mock))
            .build()
            .await?;

        let mut options = RequestOptions::default();
        options.set_quota_project(USER_QUOTA_PROJECT);
        let response = send_request(client, options).await?;

        assert_eq!(
            response
                .metadata
                .get(X_GOOG_USER_PROJECT)
                .map(String::as_str),
            Some(USER_QUOTA_PROJECT)
        );
        assert!(
            !response.metadata.values().any(|v| v == CRED_QUOTA_PROJECT),
            "credential's quota_project value leaked onto the wire: {:?}",
            response.metadata
        );
        Ok(())
    }

    async fn send_request(
        client: grpc::Client,
        options: RequestOptions,
    ) -> Result<google::test::v1::EchoResponse> {
        let extensions = {
            let mut e = tonic::Extensions::new();
            e.insert(tonic::GrpcMethod::new(
                "google.test.v1.EchoServices",
                "Echo",
            ));
            e
        };
        let request = google::test::v1::EchoRequest {
            message: "message".into(),
            ..google::test::v1::EchoRequest::default()
        };
        client
            .execute(
                extensions,
                http::uri::PathAndQuery::from_static("/google.test.v1.EchoService/Echo"),
                request,
                options,
                "test-only-api-client/1.0",
                "",
            )
            .await
            .map(tonic::Response::into_inner)
    }
}
