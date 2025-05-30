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
            async fn get_operation( &self, _req: longrunning::model::GetOperationRequest, _options: gax::options::RequestOptions) -> gax::Result<gax::response::Response<longrunning::model::Operation>>;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn mocking() {
        let mut seq = mockall::Sequence::new();
        let mut mock = MockStorageControl::new();
        mock.expect_delete_bucket()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_, _| Err(gax::error::Error::other("simulated failure")));
        mock.expect_get_bucket()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_, _| Err(gax::error::Error::other("simulated failure")));
        mock.expect_create_bucket()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_, _| Err(gax::error::Error::other("simulated failure")));
        mock.expect_list_buckets()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_, _| Err(gax::error::Error::other("simulated failure")));
        mock.expect_lock_bucket_retention_policy()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_, _| Err(gax::error::Error::other("simulated failure")));
        mock.expect_get_iam_policy()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_, _| Err(gax::error::Error::other("simulated failure")));
        mock.expect_set_iam_policy()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_, _| Err(gax::error::Error::other("simulated failure")));
        mock.expect_test_iam_permissions()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_, _| Err(gax::error::Error::other("simulated failure")));
        mock.expect_update_bucket()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_, _| Err(gax::error::Error::other("simulated failure")));
        mock.expect_compose_object()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_, _| Err(gax::error::Error::other("simulated failure")));
        mock.expect_delete_object()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_, _| Err(gax::error::Error::other("simulated failure")));
        mock.expect_restore_object()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_, _| Err(gax::error::Error::other("simulated failure")));
        mock.expect_get_object()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_, _| Err(gax::error::Error::other("simulated failure")));
        mock.expect_update_object()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_, _| Err(gax::error::Error::other("simulated failure")));
        mock.expect_list_objects()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_, _| Err(gax::error::Error::other("simulated failure")));
        mock.expect_rewrite_object()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_, _| Err(gax::error::Error::other("simulated failure")));
        mock.expect_move_object()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_, _| Err(gax::error::Error::other("simulated failure")));

        mock.expect_create_folder()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_, _| Err(gax::error::Error::other("simulated failure")));
        mock.expect_delete_folder()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_, _| Err(gax::error::Error::other("simulated failure")));
        mock.expect_get_folder()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_, _| Err(gax::error::Error::other("simulated failure")));
        mock.expect_list_folders()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_, _| Err(gax::error::Error::other("simulated failure")));
        mock.expect_rename_folder()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_, _| Err(gax::error::Error::other("simulated failure")));
        mock.expect_get_storage_layout()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_, _| Err(gax::error::Error::other("simulated failure")));
        mock.expect_create_managed_folder()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_, _| Err(gax::error::Error::other("simulated failure")));
        mock.expect_delete_managed_folder()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_, _| Err(gax::error::Error::other("simulated failure")));
        mock.expect_get_managed_folder()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_, _| Err(gax::error::Error::other("simulated failure")));
        mock.expect_list_managed_folders()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_, _| Err(gax::error::Error::other("simulated failure")));
        mock.expect_create_anywhere_cache()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_, _| Err(gax::error::Error::other("simulated failure")));
        mock.expect_update_anywhere_cache()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_, _| Err(gax::error::Error::other("simulated failure")));
        mock.expect_disable_anywhere_cache()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_, _| Err(gax::error::Error::other("simulated failure")));
        mock.expect_pause_anywhere_cache()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_, _| Err(gax::error::Error::other("simulated failure")));
        mock.expect_resume_anywhere_cache()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_, _| Err(gax::error::Error::other("simulated failure")));
        mock.expect_get_anywhere_cache()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_, _| Err(gax::error::Error::other("simulated failure")));
        mock.expect_list_anywhere_caches()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_, _| Err(gax::error::Error::other("simulated failure")));
        mock.expect_get_operation()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_, _| Err(gax::error::Error::other("simulated failure")));

        let client = gcs::client::StorageControl::from_stub(mock);

        let _ = client.delete_bucket().send().await.unwrap_err();
        let _ = client.get_bucket().send().await.unwrap_err();
        let _ = client.create_bucket().send().await.unwrap_err();
        let _ = client.list_buckets().send().await.unwrap_err();
        let _ = client
            .lock_bucket_retention_policy()
            .send()
            .await
            .unwrap_err();
        let _ = client.get_iam_policy().send().await.unwrap_err();
        let _ = client.set_iam_policy().send().await.unwrap_err();
        let _ = client.test_iam_permissions().send().await.unwrap_err();
        let _ = client.update_bucket().send().await.unwrap_err();
        let _ = client.compose_object().send().await.unwrap_err();
        let _ = client.delete_object().send().await.unwrap_err();
        let _ = client.restore_object().send().await.unwrap_err();
        let _ = client.get_object().send().await.unwrap_err();
        let _ = client.update_object().send().await.unwrap_err();
        let _ = client.list_objects().send().await.unwrap_err();
        let _ = client.rewrite_object().send().await.unwrap_err();
        let _ = client.move_object().send().await.unwrap_err();

        let _ = client.create_folder().send().await.unwrap_err();
        let _ = client.delete_folder().send().await.unwrap_err();
        let _ = client.get_folder().send().await.unwrap_err();
        let _ = client.list_folders().send().await.unwrap_err();
        let _ = client.rename_folder().send().await.unwrap_err();
        let _ = client.get_storage_layout().send().await.unwrap_err();
        let _ = client.create_managed_folder().send().await.unwrap_err();
        let _ = client.delete_managed_folder().send().await.unwrap_err();
        let _ = client.get_managed_folder().send().await.unwrap_err();
        let _ = client.list_managed_folders().send().await.unwrap_err();
        let _ = client.create_anywhere_cache().send().await.unwrap_err();
        let _ = client.update_anywhere_cache().send().await.unwrap_err();
        let _ = client.disable_anywhere_cache().send().await.unwrap_err();
        let _ = client.pause_anywhere_cache().send().await.unwrap_err();
        let _ = client.resume_anywhere_cache().send().await.unwrap_err();
        let _ = client.get_anywhere_cache().send().await.unwrap_err();
        let _ = client.list_anywhere_caches().send().await.unwrap_err();
        let _ = client.get_operation().send().await.unwrap_err();
    }

    mod default_stub {
        use super::gcs;

        #[derive(Debug)]
        struct DefaultStorageControl;
        impl gcs::stub::StorageControl for DefaultStorageControl {}

        macro_rules! default_stub_method {
            ($($method:ident),*) => {
                $(
                    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
                    #[should_panic]
                    async fn $method() {
                        let client = gcs::client::StorageControl::from_stub(DefaultStorageControl);
                        let _ = client.$method().send().await;
                    }
                )*
            };
        }

        default_stub_method!(
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
            get_operation
        );
    }
}
