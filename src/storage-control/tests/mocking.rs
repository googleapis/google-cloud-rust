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

#[cfg(test)]
mod mocking {
    type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
    use google_cloud_storage_control as gcs;

    mockall::mock! {
        #[derive(Debug)]
        StorageControl {}
        impl gcs::stub::StorageControl for StorageControl {
            async fn get_bucket(&self, req: gcs::model::GetBucketRequest, _options: gax::options::RequestOptions) -> gax::Result<gax::response::Response<gcs::model::Bucket>>;
            async fn get_folder(&self, req: gcs::model::GetFolderRequest, _options: gax::options::RequestOptions) -> gax::Result<gax::response::Response<gcs::model::Folder>>;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_mocks() -> Result<()> {
        let mut mock = MockStorageControl::new();
        mock.expect_get_bucket()
            .return_once(|_, _| Err(gax::error::Error::other("simulated failure")));
        mock.expect_get_folder()
            .return_once(|_, _| Err(gax::error::Error::other("simulated failure")));

        let client = gcs::client::StorageControl::from_stub(mock);

        let response = client.get_bucket().send().await;
        assert!(response.is_err());
        let response = client.get_folder().send().await;
        assert!(response.is_err());

        Ok(())
    }
}
