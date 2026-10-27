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
    use grpc_server::transport::{EchoTransport, transport_builder};
    use grpc_server::{both_transports, google, start_echo_server};
    use http::{HeaderMap, HeaderValue};
    use pretty_assertions::assert_eq;

    const X_GOOG_USER_PROJECT: &str = "x-goog-user-project";
    const CRED_QUOTA_PROJECT: &str = "cred_quota_project";
    const USER_QUOTA_PROJECT: &str = "project_lazy_dog";

    both_transports!(
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        user_project_emits_header
    );
    async fn user_project_emits_header<C: EchoTransport>() -> anyhow::Result<()> {
        // Arrange
        let (endpoint, _server) = start_echo_server().await?;
        let client = transport_builder::<C>(endpoint)
            .with_credentials(mock_credentials())
            .build()
            .await?;
        let mut options = RequestOptions::default();
        options.set_quota_project(USER_QUOTA_PROJECT);

        // Act
        let response = send_request(client, options).await?;

        // Assert
        assert_eq!(
            response
                .metadata
                .get(X_GOOG_USER_PROJECT)
                .map(String::as_str),
            Some(USER_QUOTA_PROJECT)
        );
        Ok(())
    }

    both_transports!(
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        no_user_project_no_header
    );
    async fn no_user_project_no_header<C: EchoTransport>() -> anyhow::Result<()> {
        // Arrange
        let (endpoint, _server) = start_echo_server().await?;
        let client = transport_builder::<C>(endpoint)
            .with_credentials(mock_credentials())
            .build()
            .await?;

        // Act
        let response = send_request(client, RequestOptions::default()).await?;

        // Assert
        assert!(
            !response.metadata.contains_key(X_GOOG_USER_PROJECT),
            "{:?}",
            response.metadata
        );
        Ok(())
    }

    both_transports!(
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        user_project_strips_credential_quota_project
    );
    async fn user_project_strips_credential_quota_project<C: EchoTransport>() -> anyhow::Result<()>
    {
        // Arrange
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

        let client = transport_builder::<C>(endpoint)
            .with_credentials(Credentials::from(mock))
            .build()
            .await?;

        let mut options = RequestOptions::default();
        options.set_quota_project(USER_QUOTA_PROJECT);

        // Act
        let response = send_request(client, options).await?;

        // Assert
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

    async fn send_request<C: EchoTransport>(
        client: C,
        options: RequestOptions,
    ) -> Result<google::test::v1::EchoResponse> {
        let request = google::test::v1::EchoRequest {
            message: "message".into(),
            ..google::test::v1::EchoRequest::default()
        };
        client
            .echo(request, options, "")
            .await
            .map(tonic::Response::into_inner)
    }
}
