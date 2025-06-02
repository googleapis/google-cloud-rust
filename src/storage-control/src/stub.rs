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

//! Traits to mock the clients in this library.
//!
//! Application developers may need to mock the clients in this library to test
//! how their application works with different (and sometimes hard to trigger)
//! client and service behavior. Such test can define mocks implementing the
//! trait(s) defined in this module, initialize the client with an instance of
//! this mock in their tests, and verify their application responds as expected.

#![allow(rustdoc::broken_intra_doc_links)]

/// Defines the trait used to implement [super::client::StorageControl].
///
/// Application developers may need to implement this trait to mock
/// `client::StorageControl`. In other use-cases, application developers only
/// use `client::StorageControl` and need not be concerned with this trait or
/// its implementations.
///
/// Services gain new RPCs routinely. Consequently, this trait gains new methods
/// too. To avoid breaking applications the trait provides a default
/// implementation of each method. Most of these implementations just return an
/// error.
pub trait StorageControl: std::fmt::Debug + Send + Sync {
    /// Implements [super::client::StorageControl::delete_bucket].
    fn delete_bucket(
        &self,
        _req: crate::model::DeleteBucketRequest,
        _options: gax::options::RequestOptions,
    ) -> impl std::future::Future<Output = crate::Result<gax::response::Response<()>>> + Send {
        gaxi::unimplemented::unimplemented_stub()
    }

    /// Implements [super::client::StorageControl::get_bucket].
    fn get_bucket(
        &self,
        _req: crate::model::GetBucketRequest,
        _options: gax::options::RequestOptions,
    ) -> impl std::future::Future<
        Output = crate::Result<gax::response::Response<crate::model::Bucket>>,
    > + Send {
        gaxi::unimplemented::unimplemented_stub()
    }

    /// Implements [super::client::StorageControl::create_bucket].
    fn create_bucket(
        &self,
        _req: crate::model::CreateBucketRequest,
        _options: gax::options::RequestOptions,
    ) -> impl std::future::Future<
        Output = crate::Result<gax::response::Response<crate::model::Bucket>>,
    > + Send {
        gaxi::unimplemented::unimplemented_stub()
    }

    /// Implements [super::client::StorageControl::list_buckets].
    fn list_buckets(
        &self,
        _req: crate::model::ListBucketsRequest,
        _options: gax::options::RequestOptions,
    ) -> impl std::future::Future<
        Output = crate::Result<gax::response::Response<crate::model::ListBucketsResponse>>,
    > + Send {
        gaxi::unimplemented::unimplemented_stub()
    }

    /// Implements [super::client::StorageControl::lock_bucket_retention_policy].
    fn lock_bucket_retention_policy(
        &self,
        _req: crate::model::LockBucketRetentionPolicyRequest,
        _options: gax::options::RequestOptions,
    ) -> impl std::future::Future<
        Output = crate::Result<gax::response::Response<crate::model::Bucket>>,
    > + Send {
        gaxi::unimplemented::unimplemented_stub()
    }

    /// Implements [super::client::StorageControl::get_iam_policy].
    fn get_iam_policy(
        &self,
        _req: iam_v1::model::GetIamPolicyRequest,
        _options: gax::options::RequestOptions,
    ) -> impl std::future::Future<
        Output = crate::Result<gax::response::Response<iam_v1::model::Policy>>,
    > + Send {
        gaxi::unimplemented::unimplemented_stub()
    }

    /// Implements [super::client::StorageControl::set_iam_policy].
    fn set_iam_policy(
        &self,
        _req: iam_v1::model::SetIamPolicyRequest,
        _options: gax::options::RequestOptions,
    ) -> impl std::future::Future<
        Output = crate::Result<gax::response::Response<iam_v1::model::Policy>>,
    > + Send {
        gaxi::unimplemented::unimplemented_stub()
    }

    /// Implements [super::client::StorageControl::test_iam_permissions].
    fn test_iam_permissions(
        &self,
        _req: iam_v1::model::TestIamPermissionsRequest,
        _options: gax::options::RequestOptions,
    ) -> impl std::future::Future<
        Output = crate::Result<gax::response::Response<iam_v1::model::TestIamPermissionsResponse>>,
    > + Send {
        gaxi::unimplemented::unimplemented_stub()
    }

    /// Implements [super::client::StorageControl::update_bucket].
    fn update_bucket(
        &self,
        _req: crate::model::UpdateBucketRequest,
        _options: gax::options::RequestOptions,
    ) -> impl std::future::Future<
        Output = crate::Result<gax::response::Response<crate::model::Bucket>>,
    > + Send {
        gaxi::unimplemented::unimplemented_stub()
    }

    /// Implements [super::client::StorageControl::compose_object].
    fn compose_object(
        &self,
        _req: crate::model::ComposeObjectRequest,
        _options: gax::options::RequestOptions,
    ) -> impl std::future::Future<
        Output = crate::Result<gax::response::Response<crate::model::Object>>,
    > + Send {
        gaxi::unimplemented::unimplemented_stub()
    }

    /// Implements [super::client::StorageControl::delete_object].
    fn delete_object(
        &self,
        _req: crate::model::DeleteObjectRequest,
        _options: gax::options::RequestOptions,
    ) -> impl std::future::Future<Output = crate::Result<gax::response::Response<()>>> + Send {
        gaxi::unimplemented::unimplemented_stub()
    }

    /// Implements [super::client::StorageControl::restore_object].
    fn restore_object(
        &self,
        _req: crate::model::RestoreObjectRequest,
        _options: gax::options::RequestOptions,
    ) -> impl std::future::Future<
        Output = crate::Result<gax::response::Response<crate::model::Object>>,
    > + Send {
        gaxi::unimplemented::unimplemented_stub()
    }

    /// Implements [super::client::StorageControl::get_object].
    fn get_object(
        &self,
        _req: crate::model::GetObjectRequest,
        _options: gax::options::RequestOptions,
    ) -> impl std::future::Future<
        Output = crate::Result<gax::response::Response<crate::model::Object>>,
    > + Send {
        gaxi::unimplemented::unimplemented_stub()
    }

    /// Implements [super::client::StorageControl::update_object].
    fn update_object(
        &self,
        _req: crate::model::UpdateObjectRequest,
        _options: gax::options::RequestOptions,
    ) -> impl std::future::Future<
        Output = crate::Result<gax::response::Response<crate::model::Object>>,
    > + Send {
        gaxi::unimplemented::unimplemented_stub()
    }

    /// Implements [super::client::StorageControl::list_objects].
    fn list_objects(
        &self,
        _req: crate::model::ListObjectsRequest,
        _options: gax::options::RequestOptions,
    ) -> impl std::future::Future<
        Output = crate::Result<gax::response::Response<crate::model::ListObjectsResponse>>,
    > + Send {
        gaxi::unimplemented::unimplemented_stub()
    }

    /// Implements [super::client::StorageControl::rewrite_object].
    fn rewrite_object(
        &self,
        _req: crate::model::RewriteObjectRequest,
        _options: gax::options::RequestOptions,
    ) -> impl std::future::Future<
        Output = crate::Result<gax::response::Response<crate::model::RewriteResponse>>,
    > + Send {
        gaxi::unimplemented::unimplemented_stub()
    }

    /// Implements [super::client::StorageControl::move_object].
    fn move_object(
        &self,
        _req: crate::model::MoveObjectRequest,
        _options: gax::options::RequestOptions,
    ) -> impl std::future::Future<
        Output = crate::Result<gax::response::Response<crate::model::Object>>,
    > + Send {
        gaxi::unimplemented::unimplemented_stub()
    }

    /// Implements [super::client::StorageControl::create_folder].
    fn create_folder(
        &self,
        _req: crate::model::CreateFolderRequest,
        _options: gax::options::RequestOptions,
    ) -> impl std::future::Future<
        Output = crate::Result<gax::response::Response<crate::model::Folder>>,
    > + Send {
        gaxi::unimplemented::unimplemented_stub()
    }

    /// Implements [super::client::StorageControl::delete_folder].
    fn delete_folder(
        &self,
        _req: crate::model::DeleteFolderRequest,
        _options: gax::options::RequestOptions,
    ) -> impl std::future::Future<Output = crate::Result<gax::response::Response<()>>> + Send {
        gaxi::unimplemented::unimplemented_stub()
    }

    /// Implements [super::client::StorageControl::get_folder].
    fn get_folder(
        &self,
        _req: crate::model::GetFolderRequest,
        _options: gax::options::RequestOptions,
    ) -> impl std::future::Future<
        Output = crate::Result<gax::response::Response<crate::model::Folder>>,
    > + Send {
        gaxi::unimplemented::unimplemented_stub()
    }

    /// Implements [super::client::StorageControl::list_folders].
    fn list_folders(
        &self,
        _req: crate::model::ListFoldersRequest,
        _options: gax::options::RequestOptions,
    ) -> impl std::future::Future<
        Output = crate::Result<gax::response::Response<crate::model::ListFoldersResponse>>,
    > + Send {
        gaxi::unimplemented::unimplemented_stub()
    }

    /// Implements [super::client::StorageControl::rename_folder].
    fn rename_folder(
        &self,
        _req: crate::model::RenameFolderRequest,
        _options: gax::options::RequestOptions,
    ) -> impl std::future::Future<
        Output = crate::Result<gax::response::Response<longrunning::model::Operation>>,
    > + Send {
        gaxi::unimplemented::unimplemented_stub()
    }

    /// Implements [super::client::StorageControl::get_storage_layout].
    fn get_storage_layout(
        &self,
        _req: crate::model::GetStorageLayoutRequest,
        _options: gax::options::RequestOptions,
    ) -> impl std::future::Future<
        Output = crate::Result<gax::response::Response<crate::model::StorageLayout>>,
    > + Send {
        gaxi::unimplemented::unimplemented_stub()
    }

    /// Implements [super::client::StorageControl::create_managed_folder].
    fn create_managed_folder(
        &self,
        _req: crate::model::CreateManagedFolderRequest,
        _options: gax::options::RequestOptions,
    ) -> impl std::future::Future<
        Output = crate::Result<gax::response::Response<crate::model::ManagedFolder>>,
    > + Send {
        gaxi::unimplemented::unimplemented_stub()
    }

    /// Implements [super::client::StorageControl::delete_managed_folder].
    fn delete_managed_folder(
        &self,
        _req: crate::model::DeleteManagedFolderRequest,
        _options: gax::options::RequestOptions,
    ) -> impl std::future::Future<Output = crate::Result<gax::response::Response<()>>> + Send {
        gaxi::unimplemented::unimplemented_stub()
    }

    /// Implements [super::client::StorageControl::get_managed_folder].
    fn get_managed_folder(
        &self,
        _req: crate::model::GetManagedFolderRequest,
        _options: gax::options::RequestOptions,
    ) -> impl std::future::Future<
        Output = crate::Result<gax::response::Response<crate::model::ManagedFolder>>,
    > + Send {
        gaxi::unimplemented::unimplemented_stub()
    }

    /// Implements [super::client::StorageControl::list_managed_folders].
    fn list_managed_folders(
        &self,
        _req: crate::model::ListManagedFoldersRequest,
        _options: gax::options::RequestOptions,
    ) -> impl std::future::Future<
        Output = crate::Result<gax::response::Response<crate::model::ListManagedFoldersResponse>>,
    > + Send {
        gaxi::unimplemented::unimplemented_stub()
    }

    /// Implements [super::client::StorageControl::create_anywhere_cache].
    fn create_anywhere_cache(
        &self,
        _req: crate::model::CreateAnywhereCacheRequest,
        _options: gax::options::RequestOptions,
    ) -> impl std::future::Future<
        Output = crate::Result<gax::response::Response<longrunning::model::Operation>>,
    > + Send {
        gaxi::unimplemented::unimplemented_stub()
    }

    /// Implements [super::client::StorageControl::update_anywhere_cache].
    fn update_anywhere_cache(
        &self,
        _req: crate::model::UpdateAnywhereCacheRequest,
        _options: gax::options::RequestOptions,
    ) -> impl std::future::Future<
        Output = crate::Result<gax::response::Response<longrunning::model::Operation>>,
    > + Send {
        gaxi::unimplemented::unimplemented_stub()
    }

    /// Implements [super::client::StorageControl::disable_anywhere_cache].
    fn disable_anywhere_cache(
        &self,
        _req: crate::model::DisableAnywhereCacheRequest,
        _options: gax::options::RequestOptions,
    ) -> impl std::future::Future<
        Output = crate::Result<gax::response::Response<crate::model::AnywhereCache>>,
    > + Send {
        gaxi::unimplemented::unimplemented_stub()
    }

    /// Implements [super::client::StorageControl::pause_anywhere_cache].
    fn pause_anywhere_cache(
        &self,
        _req: crate::model::PauseAnywhereCacheRequest,
        _options: gax::options::RequestOptions,
    ) -> impl std::future::Future<
        Output = crate::Result<gax::response::Response<crate::model::AnywhereCache>>,
    > + Send {
        gaxi::unimplemented::unimplemented_stub()
    }

    /// Implements [super::client::StorageControl::resume_anywhere_cache].
    fn resume_anywhere_cache(
        &self,
        _req: crate::model::ResumeAnywhereCacheRequest,
        _options: gax::options::RequestOptions,
    ) -> impl std::future::Future<
        Output = crate::Result<gax::response::Response<crate::model::AnywhereCache>>,
    > + Send {
        gaxi::unimplemented::unimplemented_stub()
    }

    /// Implements [super::client::StorageControl::get_anywhere_cache].
    fn get_anywhere_cache(
        &self,
        _req: crate::model::GetAnywhereCacheRequest,
        _options: gax::options::RequestOptions,
    ) -> impl std::future::Future<
        Output = crate::Result<gax::response::Response<crate::model::AnywhereCache>>,
    > + Send {
        gaxi::unimplemented::unimplemented_stub()
    }

    /// Implements [super::client::StorageControl::list_anywhere_caches].
    fn list_anywhere_caches(
        &self,
        _req: crate::model::ListAnywhereCachesRequest,
        _options: gax::options::RequestOptions,
    ) -> impl std::future::Future<
        Output = crate::Result<gax::response::Response<crate::model::ListAnywhereCachesResponse>>,
    > + Send {
        gaxi::unimplemented::unimplemented_stub()
    }

    /// Implements [super::client::StorageControl::get_operation].
    fn get_operation(
        &self,
        _req: longrunning::model::GetOperationRequest,
        _options: gax::options::RequestOptions,
    ) -> impl std::future::Future<
        Output = crate::Result<gax::response::Response<longrunning::model::Operation>>,
    > + Send {
        gaxi::unimplemented::unimplemented_stub()
    }
}

impl<T> crate::generated::gapic::stub::StorageControl for std::sync::Arc<T>
where
    T: StorageControl,
{
    fn delete_bucket(
        &self,
        req: crate::model::DeleteBucketRequest,
        options: gax::options::RequestOptions,
    ) -> impl std::future::Future<Output = crate::Result<gax::response::Response<()>>> {
        T::delete_bucket(self, req, options)
    }

    fn get_bucket(
        &self,
        req: crate::model::GetBucketRequest,
        options: gax::options::RequestOptions,
    ) -> impl std::future::Future<Output = crate::Result<gax::response::Response<crate::model::Bucket>>>
    {
        T::get_bucket(self, req, options)
    }

    fn create_bucket(
        &self,
        req: crate::model::CreateBucketRequest,
        options: gax::options::RequestOptions,
    ) -> impl std::future::Future<Output = crate::Result<gax::response::Response<crate::model::Bucket>>>
    {
        T::create_bucket(self, req, options)
    }

    fn list_buckets(
        &self,
        req: crate::model::ListBucketsRequest,
        options: gax::options::RequestOptions,
    ) -> impl std::future::Future<
        Output = crate::Result<gax::response::Response<crate::model::ListBucketsResponse>>,
    > {
        T::list_buckets(self, req, options)
    }

    fn lock_bucket_retention_policy(
        &self,
        req: crate::model::LockBucketRetentionPolicyRequest,
        options: gax::options::RequestOptions,
    ) -> impl std::future::Future<Output = crate::Result<gax::response::Response<crate::model::Bucket>>>
    {
        T::lock_bucket_retention_policy(self, req, options)
    }

    fn get_iam_policy(
        &self,
        req: iam_v1::model::GetIamPolicyRequest,
        options: gax::options::RequestOptions,
    ) -> impl std::future::Future<Output = crate::Result<gax::response::Response<iam_v1::model::Policy>>>
    {
        T::get_iam_policy(self, req, options)
    }

    fn set_iam_policy(
        &self,
        req: iam_v1::model::SetIamPolicyRequest,
        options: gax::options::RequestOptions,
    ) -> impl std::future::Future<Output = crate::Result<gax::response::Response<iam_v1::model::Policy>>>
    {
        T::set_iam_policy(self, req, options)
    }

    fn test_iam_permissions(
        &self,
        req: iam_v1::model::TestIamPermissionsRequest,
        options: gax::options::RequestOptions,
    ) -> impl std::future::Future<
        Output = crate::Result<gax::response::Response<iam_v1::model::TestIamPermissionsResponse>>,
    > {
        T::test_iam_permissions(self, req, options)
    }

    fn update_bucket(
        &self,
        req: crate::model::UpdateBucketRequest,
        options: gax::options::RequestOptions,
    ) -> impl std::future::Future<Output = crate::Result<gax::response::Response<crate::model::Bucket>>>
    {
        T::update_bucket(self, req, options)
    }

    fn compose_object(
        &self,
        req: crate::model::ComposeObjectRequest,
        options: gax::options::RequestOptions,
    ) -> impl std::future::Future<Output = crate::Result<gax::response::Response<crate::model::Object>>>
    {
        T::compose_object(self, req, options)
    }

    fn delete_object(
        &self,
        req: crate::model::DeleteObjectRequest,
        options: gax::options::RequestOptions,
    ) -> impl std::future::Future<Output = crate::Result<gax::response::Response<()>>> {
        T::delete_object(self, req, options)
    }

    fn restore_object(
        &self,
        req: crate::model::RestoreObjectRequest,
        options: gax::options::RequestOptions,
    ) -> impl std::future::Future<Output = crate::Result<gax::response::Response<crate::model::Object>>>
    {
        T::restore_object(self, req, options)
    }

    fn get_object(
        &self,
        req: crate::model::GetObjectRequest,
        options: gax::options::RequestOptions,
    ) -> impl std::future::Future<Output = crate::Result<gax::response::Response<crate::model::Object>>>
    {
        T::get_object(self, req, options)
    }

    fn update_object(
        &self,
        req: crate::model::UpdateObjectRequest,
        options: gax::options::RequestOptions,
    ) -> impl std::future::Future<Output = crate::Result<gax::response::Response<crate::model::Object>>>
    {
        T::update_object(self, req, options)
    }

    fn list_objects(
        &self,
        req: crate::model::ListObjectsRequest,
        options: gax::options::RequestOptions,
    ) -> impl std::future::Future<
        Output = crate::Result<gax::response::Response<crate::model::ListObjectsResponse>>,
    > {
        T::list_objects(self, req, options)
    }

    fn rewrite_object(
        &self,
        req: crate::model::RewriteObjectRequest,
        options: gax::options::RequestOptions,
    ) -> impl std::future::Future<
        Output = crate::Result<gax::response::Response<crate::model::RewriteResponse>>,
    > {
        T::rewrite_object(self, req, options)
    }

    fn move_object(
        &self,
        req: crate::model::MoveObjectRequest,
        options: gax::options::RequestOptions,
    ) -> impl std::future::Future<Output = crate::Result<gax::response::Response<crate::model::Object>>>
    {
        T::move_object(self, req, options)
    }
}

impl<T> crate::generated::gapic_control::stub::StorageControl for std::sync::Arc<T>
where
    T: StorageControl,
{
    fn create_folder(
        &self,
        req: crate::model::CreateFolderRequest,
        options: gax::options::RequestOptions,
    ) -> impl std::future::Future<Output = crate::Result<gax::response::Response<crate::model::Folder>>>
    {
        T::create_folder(self, req, options)
    }

    fn delete_folder(
        &self,
        req: crate::model::DeleteFolderRequest,
        options: gax::options::RequestOptions,
    ) -> impl std::future::Future<Output = crate::Result<gax::response::Response<()>>> {
        T::delete_folder(self, req, options)
    }

    fn get_folder(
        &self,
        req: crate::model::GetFolderRequest,
        options: gax::options::RequestOptions,
    ) -> impl std::future::Future<Output = crate::Result<gax::response::Response<crate::model::Folder>>>
    {
        T::get_folder(self, req, options)
    }

    fn list_folders(
        &self,
        req: crate::model::ListFoldersRequest,
        options: gax::options::RequestOptions,
    ) -> impl std::future::Future<
        Output = crate::Result<gax::response::Response<crate::model::ListFoldersResponse>>,
    > {
        T::list_folders(self, req, options)
    }

    fn rename_folder(
        &self,
        req: crate::model::RenameFolderRequest,
        options: gax::options::RequestOptions,
    ) -> impl std::future::Future<
        Output = crate::Result<gax::response::Response<longrunning::model::Operation>>,
    > {
        T::rename_folder(self, req, options)
    }

    fn get_storage_layout(
        &self,
        req: crate::model::GetStorageLayoutRequest,
        options: gax::options::RequestOptions,
    ) -> impl std::future::Future<
        Output = crate::Result<gax::response::Response<crate::model::StorageLayout>>,
    > {
        T::get_storage_layout(self, req, options)
    }

    fn create_managed_folder(
        &self,
        req: crate::model::CreateManagedFolderRequest,
        options: gax::options::RequestOptions,
    ) -> impl std::future::Future<
        Output = crate::Result<gax::response::Response<crate::model::ManagedFolder>>,
    > {
        T::create_managed_folder(self, req, options)
    }

    fn delete_managed_folder(
        &self,
        req: crate::model::DeleteManagedFolderRequest,
        options: gax::options::RequestOptions,
    ) -> impl std::future::Future<Output = crate::Result<gax::response::Response<()>>> {
        T::delete_managed_folder(self, req, options)
    }

    fn get_managed_folder(
        &self,
        req: crate::model::GetManagedFolderRequest,
        options: gax::options::RequestOptions,
    ) -> impl std::future::Future<
        Output = crate::Result<gax::response::Response<crate::model::ManagedFolder>>,
    > {
        T::get_managed_folder(self, req, options)
    }

    fn list_managed_folders(
        &self,
        req: crate::model::ListManagedFoldersRequest,
        options: gax::options::RequestOptions,
    ) -> impl std::future::Future<
        Output = crate::Result<gax::response::Response<crate::model::ListManagedFoldersResponse>>,
    > {
        T::list_managed_folders(self, req, options)
    }

    fn create_anywhere_cache(
        &self,
        req: crate::model::CreateAnywhereCacheRequest,
        options: gax::options::RequestOptions,
    ) -> impl std::future::Future<
        Output = crate::Result<gax::response::Response<longrunning::model::Operation>>,
    > {
        T::create_anywhere_cache(self, req, options)
    }

    fn update_anywhere_cache(
        &self,
        req: crate::model::UpdateAnywhereCacheRequest,
        options: gax::options::RequestOptions,
    ) -> impl std::future::Future<
        Output = crate::Result<gax::response::Response<longrunning::model::Operation>>,
    > {
        T::update_anywhere_cache(self, req, options)
    }

    fn disable_anywhere_cache(
        &self,
        req: crate::model::DisableAnywhereCacheRequest,
        options: gax::options::RequestOptions,
    ) -> impl std::future::Future<
        Output = crate::Result<gax::response::Response<crate::model::AnywhereCache>>,
    > {
        T::disable_anywhere_cache(self, req, options)
    }

    fn pause_anywhere_cache(
        &self,
        req: crate::model::PauseAnywhereCacheRequest,
        options: gax::options::RequestOptions,
    ) -> impl std::future::Future<
        Output = crate::Result<gax::response::Response<crate::model::AnywhereCache>>,
    > {
        T::pause_anywhere_cache(self, req, options)
    }

    fn resume_anywhere_cache(
        &self,
        req: crate::model::ResumeAnywhereCacheRequest,
        options: gax::options::RequestOptions,
    ) -> impl std::future::Future<
        Output = crate::Result<gax::response::Response<crate::model::AnywhereCache>>,
    > {
        T::resume_anywhere_cache(self, req, options)
    }

    fn get_anywhere_cache(
        &self,
        req: crate::model::GetAnywhereCacheRequest,
        options: gax::options::RequestOptions,
    ) -> impl std::future::Future<
        Output = crate::Result<gax::response::Response<crate::model::AnywhereCache>>,
    > {
        T::get_anywhere_cache(self, req, options)
    }

    fn list_anywhere_caches(
        &self,
        req: crate::model::ListAnywhereCachesRequest,
        options: gax::options::RequestOptions,
    ) -> impl std::future::Future<
        Output = crate::Result<gax::response::Response<crate::model::ListAnywhereCachesResponse>>,
    > {
        T::list_anywhere_caches(self, req, options)
    }

    fn get_operation(
        &self,
        req: longrunning::model::GetOperationRequest,
        options: gax::options::RequestOptions,
    ) -> impl std::future::Future<
        Output = crate::Result<gax::response::Response<longrunning::model::Operation>>,
    > {
        T::get_operation(self, req, options)
    }
}
