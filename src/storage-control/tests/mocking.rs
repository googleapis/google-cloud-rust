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
mod test {
    use google_cloud_storage_control as gcs;
    use paste::paste;

    #[derive(Debug)]
    struct DefaultStorageControl;
    impl gcs::stub::StorageControl for DefaultStorageControl {}

    mockall::mock! {
        #[derive(Debug)]
        StorageControl {}
        impl gcs::stub::StorageControl for StorageControl {
            async fn delete_bucket( &self, _req: gcs::model::DeleteBucketRequest, _options: gax::options::RequestOptions) -> gax::Result<gax::response::Response<()>>;
            async fn get_bucket( &self, _req: gcs::model::GetBucketRequest, _options: gax::options::RequestOptions) -> gax::Result<gax::response::Response<gcs::model::Bucket>>;
            async fn create_bucket( &self, _req: gcs::model::CreateBucketRequest, _options: gax::options::RequestOptions) -> gax::Result<gax::response::Response<gcs::model::Bucket>>;
            async fn list_buckets( &self, _req: gcs::model::ListBucketsRequest, _options: gax::options::RequestOptions) -> gax::Result<gax::response::Response<gcs::model::ListBucketsResponse>>;
            async fn lock_bucket_retention_policy( &self, _req: gcs::model::LockBucketRetentionPolicyRequest, _options: gax::options::RequestOptions) -> gax::Result<gax::response::Response<gcs::model::Bucket>>;
            async fn get_iam_policy( &self, _req: iam_v1::model::GetIamPolicyRequest, _options: gax::options::RequestOptions) -> gax::Result<gax::response::Response<iam_v1::model::Policy>>;
            async fn set_iam_policy( &self, _req: iam_v1::model::SetIamPolicyRequest, _options: gax::options::RequestOptions) -> gax::Result<gax::response::Response<iam_v1::model::Policy>>;
            async fn test_iam_permissions( &self, _req: iam_v1::model::TestIamPermissionsRequest, _options: gax::options::RequestOptions) -> gax::Result<gax::response::Response<iam_v1::model::TestIamPermissionsResponse>>;
            async fn update_bucket( &self, _req: gcs::model::UpdateBucketRequest, _options: gax::options::RequestOptions) -> gax::Result<gax::response::Response<gcs::model::Bucket>>;
            async fn compose_object( &self, _req: gcs::model::ComposeObjectRequest, _options: gax::options::RequestOptions) -> gax::Result<gax::response::Response<gcs::model::Object>>;
            async fn delete_object( &self, _req: gcs::model::DeleteObjectRequest, _options: gax::options::RequestOptions) -> gax::Result<gax::response::Response<()>>;
            async fn restore_object( &self, _req: gcs::model::RestoreObjectRequest, _options: gax::options::RequestOptions) -> gax::Result<gax::response::Response<gcs::model::Object>>;
            async fn get_object( &self, _req: gcs::model::GetObjectRequest, _options: gax::options::RequestOptions) -> gax::Result<gax::response::Response<gcs::model::Object>>;
            async fn update_object( &self, _req: gcs::model::UpdateObjectRequest, _options: gax::options::RequestOptions) -> gax::Result<gax::response::Response<gcs::model::Object>>;
            async fn list_objects( &self, _req: gcs::model::ListObjectsRequest, _options: gax::options::RequestOptions) -> gax::Result<gax::response::Response<gcs::model::ListObjectsResponse>>;
            async fn rewrite_object( &self, _req: gcs::model::RewriteObjectRequest, _options: gax::options::RequestOptions) -> gax::Result<gax::response::Response<gcs::model::RewriteResponse>>;
            async fn move_object( &self, _req: gcs::model::MoveObjectRequest, _options: gax::options::RequestOptions) -> gax::Result<gax::response::Response<gcs::model::Object>>;

            async fn create_folder( &self, _req: gcs::model::CreateFolderRequest, _options: gax::options::RequestOptions) -> gax::Result<gax::response::Response<gcs::model::Folder>>;
            async fn delete_folder( &self, _req: gcs::model::DeleteFolderRequest, _options: gax::options::RequestOptions) -> gax::Result<gax::response::Response<()>>;
            async fn get_folder( &self, _req: gcs::model::GetFolderRequest, _options: gax::options::RequestOptions) -> gax::Result<gax::response::Response<gcs::model::Folder>>;
            async fn list_folders( &self, _req: gcs::model::ListFoldersRequest, _options: gax::options::RequestOptions) -> gax::Result<gax::response::Response<gcs::model::ListFoldersResponse>>;
            async fn rename_folder( &self, _req: gcs::model::RenameFolderRequest, _options: gax::options::RequestOptions) -> gax::Result<gax::response::Response<longrunning::model::Operation>>;
            async fn get_storage_layout( &self, _req: gcs::model::GetStorageLayoutRequest, _options: gax::options::RequestOptions) -> gax::Result<gax::response::Response<gcs::model::StorageLayout>>;
            async fn create_managed_folder( &self, _req: gcs::model::CreateManagedFolderRequest, _options: gax::options::RequestOptions) -> gax::Result<gax::response::Response<gcs::model::ManagedFolder>>;
            async fn delete_managed_folder( &self, _req: gcs::model::DeleteManagedFolderRequest, _options: gax::options::RequestOptions) -> gax::Result<gax::response::Response<()>>;
            async fn get_managed_folder( &self, _req: gcs::model::GetManagedFolderRequest, _options: gax::options::RequestOptions) -> gax::Result<gax::response::Response<gcs::model::ManagedFolder>>;
            async fn list_managed_folders( &self, _req: gcs::model::ListManagedFoldersRequest, _options: gax::options::RequestOptions) -> gax::Result<gax::response::Response<gcs::model::ListManagedFoldersResponse>>;
            async fn create_anywhere_cache( &self, _req: gcs::model::CreateAnywhereCacheRequest, _options: gax::options::RequestOptions) -> gax::Result<gax::response::Response<longrunning::model::Operation>>;
            async fn update_anywhere_cache( &self, _req: gcs::model::UpdateAnywhereCacheRequest, _options: gax::options::RequestOptions) -> gax::Result<gax::response::Response<longrunning::model::Operation>>;
            async fn disable_anywhere_cache( &self, _req: gcs::model::DisableAnywhereCacheRequest, _options: gax::options::RequestOptions) -> gax::Result<gax::response::Response<gcs::model::AnywhereCache>>;
            async fn pause_anywhere_cache( &self, _req: gcs::model::PauseAnywhereCacheRequest, _options: gax::options::RequestOptions) -> gax::Result<gax::response::Response<gcs::model::AnywhereCache>>;
            async fn resume_anywhere_cache( &self, _req: gcs::model::ResumeAnywhereCacheRequest, _options: gax::options::RequestOptions) -> gax::Result<gax::response::Response<gcs::model::AnywhereCache>>;
            async fn get_anywhere_cache( &self, _req: gcs::model::GetAnywhereCacheRequest, _options: gax::options::RequestOptions) -> gax::Result<gax::response::Response<gcs::model::AnywhereCache>>;
            async fn list_anywhere_caches( &self, _req: gcs::model::ListAnywhereCachesRequest, _options: gax::options::RequestOptions) -> gax::Result<gax::response::Response<gcs::model::ListAnywhereCachesResponse>>;
            async fn get_folder_intelligence_config( &self, _req: gcs::model::GetFolderIntelligenceConfigRequest, _options: gax::options::RequestOptions) -> gax::Result<gax::response::Response<gcs::model::IntelligenceConfig>>;
            async fn update_folder_intelligence_config( &self, _req: gcs::model::UpdateFolderIntelligenceConfigRequest, _options: gax::options::RequestOptions) -> gax::Result<gax::response::Response<gcs::model::IntelligenceConfig>>;
            async fn get_project_intelligence_config( &self, _req: gcs::model::GetProjectIntelligenceConfigRequest, _options: gax::options::RequestOptions) -> gax::Result<gax::response::Response<gcs::model::IntelligenceConfig>>;
            async fn update_project_intelligence_config( &self, _req: gcs::model::UpdateProjectIntelligenceConfigRequest, _options: gax::options::RequestOptions) -> gax::Result<gax::response::Response<gcs::model::IntelligenceConfig>>;
            async fn get_organization_intelligence_config( &self, _req: gcs::model::GetOrganizationIntelligenceConfigRequest, _options: gax::options::RequestOptions) -> gax::Result<gax::response::Response<gcs::model::IntelligenceConfig>>;
            async fn update_organization_intelligence_config( &self, _req: gcs::model::UpdateOrganizationIntelligenceConfigRequest, _options: gax::options::RequestOptions) -> gax::Result<gax::response::Response<gcs::model::IntelligenceConfig>>;
            async fn get_operation( &self, _req: longrunning::model::GetOperationRequest, _options: gax::options::RequestOptions) -> gax::Result<gax::response::Response<longrunning::model::Operation>>;
        }
    }

    macro_rules! stub_tests {
        ($($method:ident),*) => {
            $( paste! {
                #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
                async fn [<mock_stub_$method>]() {
                    let mut mock = MockStorageControl::new();
                    mock.[<expect_$method>]()
                        .times(1)
                        .returning(|_, _| Err(gax::error::Error::other("simulated failure")));
                    let client = gcs::client::StorageControl::from_stub(mock);
                    let _ = client.$method().send().await.unwrap_err();
                }

                #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
                #[should_panic]
                async fn [<default_stub_$method>]() {
                    let client = gcs::client::StorageControl::from_stub(DefaultStorageControl);
                    let _ = client.$method().send().await;
                }
            })*
        };
    }

    stub_tests!(
        delete_bucket,
        get_bucket,
        create_bucket,
        list_buckets,
        lock_bucket_retention_policy,
        get_iam_policy,
        set_iam_policy,
        test_iam_permissions,
        update_bucket,
        compose_object,
        delete_object,
        restore_object,
        get_object,
        update_object,
        list_objects,
        rewrite_object,
        move_object,
        create_folder,
        delete_folder,
        get_folder,
        list_folders,
        rename_folder,
        get_storage_layout,
        create_managed_folder,
        delete_managed_folder,
        get_managed_folder,
        list_managed_folders,
        create_anywhere_cache,
        update_anywhere_cache,
        disable_anywhere_cache,
        pause_anywhere_cache,
        resume_anywhere_cache,
        get_anywhere_cache,
        list_anywhere_caches,
        get_folder_intelligence_config,
        update_folder_intelligence_config,
        get_project_intelligence_config,
        update_project_intelligence_config,
        get_organization_intelligence_config,
        update_organization_intelligence_config,
        get_operation
    );
}
