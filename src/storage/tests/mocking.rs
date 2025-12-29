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
mod tests {
    use gax::error::{
        Error,
        rpc::{Code, Status},
    };
    use gcs::Result;
    use gcs::model::{Object, ReadObjectRequest};
    use gcs::model_ext::{ObjectHighlights, WriteObjectRequest};
    use gcs::read_object::ReadObjectResponse;
    use gcs::request_options::RequestOptions;
    use gcs::streaming_source::{BytesSource, Payload, Seek, StreamingSource};
    use gcs::{
        model_ext::{OpenObjectRequest, ReadRange},
        object_descriptor::{HeaderMap, ObjectDescriptor},
    };
    use google_cloud_storage as gcs;
    use pastey::paste;

    mockall::mock! {
        #[derive(Debug)]
        Storage {}
        impl gcs::stub::Storage for Storage {
            async fn read_object(&self, _req: ReadObjectRequest, _options: RequestOptions) -> Result<ReadObjectResponse>;
            async fn write_object_buffered<P: StreamingSource + Send + Sync + 'static>(
                &self,
                _payload: P,
                _req: WriteObjectRequest,
                _options: RequestOptions,
            ) -> Result<Object>;
            async fn write_object_unbuffered<P: StreamingSource + Seek + Send + Sync + 'static>(
                &self,
                _payload: P,
                _req: WriteObjectRequest,
                _options: RequestOptions,
            ) -> Result<Object>;
            async fn open_object(
                &self,
                _request: OpenObjectRequest,
                _options: RequestOptions,
            ) -> Result<(ObjectDescriptor, Vec<ReadObjectResponse>)>;
        }
    }

    mockall::mock! {
        #[derive(Debug)]
        Descriptor {}
        impl gcs::stub::ObjectDescriptor for Descriptor {
            fn object(&self) -> Object;
            async fn read_range(&self, range: ReadRange) -> ReadObjectResponse;
            fn headers(&self) -> HeaderMap;
        }
    }

    #[tokio::test]
    async fn mock_read_object_fail() {
        let mut mock = MockStorage::new();
        mock.expect_read_object()
            .return_once(|_, _| Err(Error::service(Status::default().set_code(Code::Aborted))));
        let client = gcs::client::Storage::from_stub(mock);
        let _ = client
            .read_object("projects/_/buckets/my-bucket", "my-object")
            .send()
            .await
            .unwrap_err();
    }

    #[tokio::test]
    async fn mock_read_object_success() -> anyhow::Result<()> {
        const LAZY: &str = "the quick brown fox jumps over the lazy dog";
        let object = {
            let mut o = ObjectHighlights::default();
            o.etag = "custom-etag".to_string();
            o
        };

        let mut mock = MockStorage::new();
        mock.expect_read_object().return_once({
            let o = object.clone();
            move |_, _| Ok(ReadObjectResponse::from_source(o, LAZY))
        });

        let client = gcs::client::Storage::from_stub(mock);
        let mut reader = client
            .read_object("projects/_/buckets/my-bucket", "my-object")
            .send()
            .await?;
        assert_eq!(&object, &reader.object());

        let mut contents = Vec::new();
        while let Some(chunk) = reader.next().await.transpose()? {
            contents.extend_from_slice(&chunk);
        }
        let contents = bytes::Bytes::from_owner(contents);
        assert_eq!(contents, LAZY);
        Ok(())
    }

    #[tokio::test]
    async fn mock_write_object_buffered() {
        let mut mock = MockStorage::new();
        mock.expect_write_object_buffered()
            .return_once(|_payload: Payload<BytesSource>, _, _| {
                Err(Error::service(Status::default().set_code(Code::Aborted)))
            });
        let client = gcs::client::Storage::from_stub(mock);
        let _ = client
            .write_object("projects/_/buckets/my-bucket", "my-object", "hello")
            .send_buffered()
            .await
            .unwrap_err();
    }

    #[tokio::test]
    async fn mock_write_object_unbuffered() {
        let mut mock = MockStorage::new();
        mock.expect_write_object_unbuffered().return_once(
            |_payload: Payload<BytesSource>, _, _| {
                Err(Error::service(Status::default().set_code(Code::Aborted)))
            },
        );
        let client = gcs::client::Storage::from_stub(mock);
        let _ = client
            .write_object("projects/_/buckets/my-bucket", "my-object", "hello")
            .send_unbuffered()
            .await
            .unwrap_err();
    }

    #[tokio::test]
    async fn mock_open_object_fail() {
        let status = Status::default().set_code(Code::Aborted);
        let want = status.clone();
        let mut mock = MockStorage::new();
        mock.expect_open_object()
            .return_once(move |_, _| Err(Error::service(status)));
        let client = gcs::client::Storage::from_stub(mock);
        let err = client
            .open_object("projects/_/buckets/my-bucket", "my-object")
            .send()
            .await
            .unwrap_err();
        assert_eq!(err.status(), Some(&want), "{err:?}");
    }

    #[tokio::test]
    async fn mock_open_object_success() -> anyhow::Result<()> {
        const LAZY: &str = "the quick brown fox jumps over the lazy dog";
        let object = Object::new().set_etag("custom-etag");
        let highlights = {
            let mut h = ObjectHighlights::default();
            h.etag = object.etag.clone();
            h
        };

        let mut mock_descriptor = MockDescriptor::new();
        mock_descriptor
            .expect_object()
            .times(1)
            .return_const(object.clone());
        mock_descriptor.expect_read_range().times(1).return_once({
            let h = highlights.clone();
            move |_| ReadObjectResponse::from_source(h, LAZY)
        });

        let mut mock = MockStorage::new();
        mock.expect_open_object()
            .return_once(move |_, _| Ok((ObjectDescriptor::new(mock_descriptor), Vec::new())));

        let client = gcs::client::Storage::from_stub(mock);
        let descriptor = client
            .open_object("projects/_/buckets/my-bucket", "my-object")
            .send()
            .await?;
        assert_eq!(object, descriptor.object());

        let mut reader = descriptor.read_range(ReadRange::offset(123)).await;
        assert_eq!(&highlights, &reader.object());

        let mut contents = Vec::new();
        while let Some(chunk) = reader.next().await.transpose()? {
            contents.extend_from_slice(&chunk);
        }
        let contents = bytes::Bytes::from_owner(contents);
        assert_eq!(contents, LAZY);
        Ok(())
    }

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
                        .returning(|_, _| Err(Error::service(Status::default().set_code(Code::Aborted))));
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
