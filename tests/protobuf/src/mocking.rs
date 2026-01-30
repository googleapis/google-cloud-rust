// Copyright 2024 Google LLC
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

//! Verify it is possible to mock most API calls.

#[cfg(test)]
mod mocking {
    use google_cloud_gax::Result as GaxResult;
    use google_cloud_gax::error::Error;
    use google_cloud_gax::options::RequestOptions;
    use google_cloud_gax::response::Response as GaxResponse;
    use google_cloud_secretmanager_v1::client::SecretManagerService;
    use google_cloud_secretmanager_v1::model::{CreateSecretRequest, Secret};
    use google_cloud_secretmanager_v1::stub::SecretManagerService as Stub;

    mockall::mock! {
        #[derive(Debug)]
        SecretManagerService {}
        impl Stub for SecretManagerService {
            async fn create_secret(&self, req: CreateSecretRequest, _options: RequestOptions) -> GaxResult<GaxResponse<Secret>>;
        }
    }

    /// The function under test.
    async fn helper(
        client: &SecretManagerService,
        project: &str,
        region: &str,
        id: &str,
    ) -> GaxResult<Secret> {
        client
            .create_secret()
            .set_parent(format!("projects/{project}/locations/{region}"))
            .set_secret_id(id)
            .send()
            .await
    }

    #[tokio::test]
    async fn test_helper() -> anyhow::Result<()> {
        let mut mock = MockSecretManagerService::new();
        mock.expect_create_secret()
            .withf(|r, _| {
                r.parent == "projects/my-project/locations/us-central1"
                    && r.secret_id == "my-secret-id"
                    && r.secret.is_none()
            })
            .return_once(|_, _| Err(unavailable()));

        let client = SecretManagerService::from_stub(mock);

        let response = helper(&client, "my-project", "us-central1", "my-secret-id").await;
        assert!(response.is_err());

        Ok(())
    }

    fn unavailable() -> Error {
        use google_cloud_gax::error::rpc::{Code, Status};
        Error::service(
            Status::default()
                .set_code(Code::Unavailable)
                .set_message("try-again"),
        )
    }
}
