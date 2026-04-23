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

#[cfg(test)]
mod tests {
    use gaxi::grpc::tonic::Response;
    use gcs::builder_ext::UserProjectExt;
    use gcs::client::StorageControl;
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
    use google_cloud_storage as gcs;
    use storage_grpc_mock::{MockStorage, google, start};

    const PROJECT_NAME: &str = "project_lazy_dog";

    #[tokio::test]
    async fn get_bucket_with_user_project() -> anyhow::Result<()> {
        let mut mock = MockStorage::new();
        mock.expect_get_bucket()
            .withf(|req| {
                req.metadata()
                    .get("x-goog-user-project")
                    .and_then(|v| v.to_str().ok())
                    == Some(PROJECT_NAME)
            })
            .times(1)
            .returning(|_| Ok(Response::new(google::storage::v2::Bucket::default())));

        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;

        let client = StorageControl::builder()
            .with_endpoint(endpoint)
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        let _ = client
            .get_bucket()
            .set_name("projects/_/buckets/my-bucket")
            .with_user_project(PROJECT_NAME)
            .send()
            .await?;

        Ok(())
    }
}
