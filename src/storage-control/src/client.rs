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

//! Contains the StorageControl client and related types.

/// Implements a client for the Cloud Storage API.
///
/// # Example
/// ```
/// # tokio_test::block_on(async {
/// # use google_cloud_storage_control::client::StorageControl;
/// let client = StorageControl::builder().build().await?;
/// // use `client` to make requests to Cloud Storage.
/// # gax::client_builder::Result::<()>::Ok(()) });
/// ```
///
/// # Configuration
///
/// To configure `StorageControl` use the `with_*` methods in the type returned
/// by [builder()][StorageControl::builder]. The default configuration should
/// work for most applications. Common configuration changes include
///
/// * [with_endpoint()]: by default this client uses the global default endpoint
///   (`https://storage.googleapis.com`). Applications using regional
///   endpoints or running in restricted networks (e.g. a network configured
//    with [Private Google Access with VPC Service Controls]) may want to
///   override this default.
/// * [with_credentials()]: by default this client uses
///   [Application Default Credentials]. Applications using custom
///   authentication may need to override this default.
///
/// # Pooling and Cloning
///
/// `StorageControl` holds a connection pool internally, it is advised to
/// create one and the reuse it.  You do not need to wrap `StorageControl` in
/// an [Rc](std::rc::Rc) or [Arc](std::sync::Arc) to reuse it, because it
/// already uses an `Arc` internally.
///
/// # Service Description
///
/// The Cloud Storage API allows applications to read and write data through
/// the abstractions of buckets and objects. For a description of these
/// abstractions please see <https://cloud.google.com/storage/docs>.
///
/// This client is used to perform metadata operations, such as creating
/// buckets, deleting objects, listing objects, etc. It does not expose any
/// functions to write or read data in objects.
///
/// Resources are named as follows:
///
/// - Projects are referred to as they are defined by the Resource Manager API,
///   using strings like `projects/123456` or `projects/my-string-id`.
///
/// - Buckets are named using string names of the form:
///   `projects/{project}/buckets/{bucket}`
///   For globally unique buckets, `_` may be substituted for the project.
///
/// - Objects are uniquely identified by their name along with the name of the
///   bucket they belong to, as separate strings in this API. For example:
///   ```no_rust
///   bucket = "projects/_/buckets/my-bucket"
///   object = "my-object/with/a/folder-like/name"
///   ```
///   Note that object names can contain `/` characters, which are treated as
///   any other character (no special directory semantics).
///
/// [with_endpoint()]: ClientBuilder::with_endpoint
/// [with_credentials()]: ClientBuilder::with_credentials
/// [Private Google Access with VPC Service Controls]: https://cloud.google.com/vpc-service-controls/docs/private-connectivity
/// [Application Default Credentials]: https://cloud.google.com/docs/authentication#adc
#[derive(Clone, Debug)]
pub struct StorageControl {
    storage: super::generated::gapic::client::StorageControl,
    control: super::generated::gapic_control::client::StorageControl,
}

impl StorageControl {
    /// Returns a builder for [StorageControl].
    ///
    /// ```no_run
    /// # tokio_test::block_on(async {
    /// # use google_cloud_storage_control::client::StorageControl;
    /// let client = StorageControl::builder().build().await?;
    /// # gax::client_builder::Result::<()>::Ok(()) });
    /// ```
    pub fn builder() -> ClientBuilder {
        gax::client_builder::internal::new_builder(client_builder::Factory)
    }

    /// Permanently deletes an empty bucket.
    ///
    /// This request will fail if the bucket is not empty. You must manually
    /// delete the objects in a bucket (including archived and soft deleted
    /// objects) before deleting the bucket.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage_control::client::StorageControl;
    /// async fn example(client: &StorageControl) -> gax::Result<()> {
    ///     client.delete_bucket()
    ///         .set_name("projects/_/buckets/my-bucket")
    ///         .send()
    ///         .await?;
    ///     Ok(())
    /// }
    /// ```
    pub fn delete_bucket(&self) -> super::builder::storage_control::DeleteBucket {
        self.storage.delete_bucket()
    }

    /// Returns metadata for the specified bucket.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage_control::client::StorageControl;
    /// async fn example(client: &StorageControl) -> gax::Result<()> {
    ///     let bucket = client.get_bucket()
    ///         .set_name("projects/_/buckets/my-bucket")
    ///         .send()
    ///         .await?;
    ///     assert_eq!(&bucket.name, "projects/_/buckets/my-bucket");
    ///     println!("bucket details={bucket:?}");
    ///     Ok(())
    /// }
    /// ```
    pub fn get_bucket(&self) -> super::builder::storage_control::GetBucket {
        self.storage.get_bucket()
    }

    /// Creates a new bucket.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage_control::client::StorageControl;
    /// async fn example(client: &StorageControl) -> gax::Result<()> {
    ///     let bucket = client.create_bucket()
    ///         .set_parent("projects/my-project")
    ///         .set_bucket_id("my-bucket")
    ///         .send()
    ///         .await?;
    ///     assert_eq!(&bucket.name, "projects/_/buckets/my-bucket");
    ///     println!("bucket details={bucket:?}");
    ///     Ok(())
    /// }
    /// ```
    pub fn create_bucket(&self) -> super::builder::storage_control::CreateBucket {
        self.storage.create_bucket()
    }

    /// Retrieves a list of buckets for a given project.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage_control::client::StorageControl;
    /// async fn example(client: &StorageControl) -> gax::Result<()> {
    ///     use gax::paginator::{ItemPaginator, Paginator};
    ///     let mut items = client.list_buckets()
    ///         .set_parent("projects/my-project")
    ///         .by_item();
    ///     while let Some(bucket) = items.next().await {
    ///         let bucket = bucket?;
    ///         println!("  {bucket:?}");
    ///     }
    ///     Ok(())
    /// }
    /// ```
    pub fn list_buckets(&self) -> super::builder::storage_control::ListBuckets {
        self.storage.list_buckets()
    }

    /// Locks retention policy on a bucket.
    pub fn lock_bucket_retention_policy(
        &self,
    ) -> super::builder::storage_control::LockBucketRetentionPolicy {
        self.storage.lock_bucket_retention_policy()
    }

    /// Gets the IAM policy for a specified bucket.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage_control::client::StorageControl;
    /// async fn example(client: &StorageControl) -> gax::Result<()> {
    ///     let policy = client.get_iam_policy()
    ///         .set_resource("projects/_/buckets/my-bucket")
    ///         .send()
    ///         .await?;
    ///     println!("policy details={policy:?}");
    ///     Ok(())
    /// }
    /// ```
    pub fn get_iam_policy(&self) -> super::builder::storage_control::GetIamPolicy {
        self.storage.get_iam_policy()
    }

    /// Updates the IAM policy for a specified bucket.
    ///
    /// This is not an update. The supplied policy will overwrite any existing
    /// IAM Policy. You should first get the current IAM policy with
    /// `get_iam_policy()` and then modify that policy before supplying it to
    /// `set_iam_policy()`.
    ///
    /// # Example
    ///
    /// ```
    /// # use google_cloud_storage_control::client::StorageControl;
    /// # use iam_v1::model::Policy;
    /// async fn example(client: &StorageControl, updated_policy: Policy) -> gax::Result<()> {
    ///     let policy = client.set_iam_policy()
    ///         .set_resource("projects/_/buckets/my-bucket")
    ///         .set_update_mask(wkt::FieldMask::default().set_paths(["bindings"]))
    ///         .set_policy(updated_policy)
    ///         .send()
    ///         .await?;
    ///     println!("policy details={policy:?}");
    ///     Ok(())
    /// }
    /// ```
    pub fn set_iam_policy(&self) -> super::builder::storage_control::SetIamPolicy {
        self.storage.set_iam_policy()
    }

    /// Tests a set of permissions on the given bucket, object, or managed folder
    /// to see which, if any, are held by the caller.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage_control::client::StorageControl;
    /// async fn example(client: &StorageControl) -> gax::Result<()> {
    ///     let response = client.test_iam_permissions()
    ///         .set_resource("projects/_/buckets/my-bucket")
    ///         .set_permissions(["storage.buckets.get"])
    ///         .send()
    ///         .await?;
    ///     println!("response details={response:?}");
    ///     Ok(())
    /// }
    /// ```
    pub fn test_iam_permissions(&self) -> super::builder::storage_control::TestIamPermissions {
        self.storage.test_iam_permissions()
    }

    /// Updates a bucket. Equivalent to JSON API's storage.buckets.patch method.
    pub fn update_bucket(&self) -> super::builder::storage_control::UpdateBucket {
        self.storage.update_bucket()
    }

    /// Concatenates a list of existing objects into a new object in the same
    /// bucket.
    pub fn compose_object(&self) -> super::builder::storage_control::ComposeObject {
        self.storage.compose_object()
    }

    /// Permanently deletes an object and its metadata.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage_control::client::StorageControl;
    /// async fn example(client: &StorageControl) -> gax::Result<()> {
    ///     client.delete_object()
    ///         .set_bucket("projects/_/buckets/my-bucket")
    ///         .set_object("my-object")
    ///         .send()
    ///         .await?;
    ///     Ok(())
    /// }
    /// ```
    ///
    /// Deletions are permanent if versioning is not enabled for the bucket, or
    /// if the generation parameter is used, or if [soft delete] is not
    /// enabled for the bucket.
    ///
    /// When this method is used to delete an object from a bucket that has soft
    /// delete policy enabled, the object becomes soft deleted, and the
    /// `soft_delete_time` and `hard_delete_time` properties are set on the
    /// object. This method cannot be used to permanently delete soft-deleted
    /// objects. Soft-deleted objects are permanently deleted according to their
    /// `hard_delete_time`.
    ///
    /// You can use the `restore_object` method to restore soft-deleted objects
    /// until the soft delete retention period has passed.
    ///
    /// [soft delete]: https://cloud.google.com/storage/docs/soft-delete
    pub fn delete_object(&self) -> super::builder::storage_control::DeleteObject {
        self.storage.delete_object()
    }

    /// Restores a soft-deleted object.
    pub fn restore_object(&self) -> super::builder::storage_control::RestoreObject {
        self.storage.restore_object()
    }

    /// Retrieves object metadata.
    ///
    /// **IAM Permissions**:
    ///
    /// Requires `storage.objects.get`
    /// [IAM permission](https://cloud.google.com/iam/docs/overview#permissions) on
    /// the bucket. To return object ACLs, the authenticated user must also have
    /// the `storage.objects.getIamPolicy` permission.
    pub fn get_object(&self) -> super::builder::storage_control::GetObject {
        self.storage.get_object()
    }

    /// Updates an object's metadata.
    /// Equivalent to JSON API's storage.objects.patch.
    pub fn update_object(&self) -> super::builder::storage_control::UpdateObject {
        self.storage.update_object()
    }

    /// Retrieves the list of objects for a given bucket.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage_control::client::StorageControl;
    /// async fn example(client: &StorageControl) -> gax::Result<()> {
    ///     use gax::paginator::{ItemPaginator, Paginator};
    ///     let mut items = client.list_objects()
    ///         .set_parent("projects/_/buckets/my-bucket")
    ///         .by_item();
    ///     while let Some(object) = items.next().await {
    ///         let object = object?;
    ///         println!("  {object:?}");
    ///     }
    ///     Ok(())
    /// }
    /// ```
    pub fn list_objects(&self) -> super::builder::storage_control::ListObjects {
        self.storage.list_objects()
    }

    /// Rewrites a source object to a destination object. Optionally overrides
    /// metadata.
    pub fn rewrite_object(&self) -> super::builder::storage_control::RewriteObject {
        self.storage.rewrite_object()
    }

    /// Moves the source object to the destination object in the same bucket.
    pub fn move_object(&self) -> super::builder::storage_control::MoveObject {
        self.storage.move_object()
    }

    /// Creates a new folder.
    ///
    /// This operation is only applicable to a hierarchical namespace enabled
    /// bucket.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage_control::client::StorageControl;
    /// async fn example(client: &StorageControl) -> gax::Result<()> {
    ///     let folder = client.create_folder()
    ///         .set_parent("projects/my-project/buckets/my-bucket")
    ///         .set_folder_id("my-folder/my-subfolder/")
    ///         .send()
    ///         .await?;
    ///     println!("folder details={folder:?}");
    ///     Ok(())
    /// }
    /// ```
    pub fn create_folder(&self) -> super::builder::storage_control::CreateFolder {
        self.control.create_folder()
    }

    /// Permanently deletes an empty folder.
    ///
    /// This operation is only applicable to a hierarchical namespace enabled
    /// bucket.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage_control::client::StorageControl;
    /// async fn example(client: &StorageControl) -> gax::Result<()> {
    ///     client.delete_folder()
    ///         .set_name("projects/_/buckets/my-bucket/folders/my-folder/my-subfolder/")
    ///         .send()
    ///         .await?;
    ///     Ok(())
    /// }
    /// ```
    pub fn delete_folder(&self) -> super::builder::storage_control::DeleteFolder {
        self.control.delete_folder()
    }

    /// Returns metadata for the specified folder.
    ///
    /// This operation is only applicable to a hierarchical namespace enabled
    /// bucket.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage_control::client::StorageControl;
    /// async fn example(client: &StorageControl) -> gax::Result<()> {
    ///     let folder = client.get_folder()
    ///         .set_name("projects/_/buckets/my-bucket/folders/my-folder/my-subfolder/")
    ///         .send()
    ///         .await?;
    ///     println!("folder details={folder:?}");
    ///     Ok(())
    /// }
    /// ```
    pub fn get_folder(&self) -> super::builder::storage_control::GetFolder {
        self.control.get_folder()
    }

    /// Retrieves a list of folders.
    ///
    /// This operation is only applicable to a hierarchical namespace enabled
    /// bucket.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage_control::client::StorageControl;
    /// async fn example(client: &StorageControl) -> gax::Result<()> {
    ///     use gax::paginator::ItemPaginator as _;
    ///     let mut folders = client.list_folders()
    ///         .set_parent("projects/_/buckets/my-bucket")
    ///         .by_item();
    ///     while let Some(folder) = folders.next().await {
    ///         let folder = folder?;
    ///         println!("  {folder:?}");
    ///     }
    ///     Ok(())
    /// }
    /// ```
    pub fn list_folders(&self) -> super::builder::storage_control::ListFolders {
        self.control.list_folders()
    }

    /// Renames a source folder to a destination folder.
    ///
    /// This operation is only applicable to a hierarchical namespace enabled
    /// bucket.
    ///
    /// During a rename, the source and destination folders are locked until the
    /// long running operation completes.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage_control::client::StorageControl;
    /// async fn example(client: &StorageControl) -> gax::Result<()> {
    ///     use lro::Poller as _;
    ///     let folder = client.rename_folder()
    ///         .set_name("projects/_/buckets/my-bucket/folders/my-folder/my-subfolder/")
    ///         .set_destination_folder_id("my-folder/my-renamed-subfolder/")
    ///         .poller()
    ///         .until_done()
    ///         .await?;
    ///     println!("folder details={folder:?}");
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Long running operations
    ///
    /// This method is used to start, and/or poll a [long-running Operation].
    /// The [Working with long-running operations] chapter in the [user guide]
    /// covers these operations in detail.
    ///
    /// [long-running operation]: https://google.aip.dev/151
    /// [user guide]: https://googleapis.github.io/google-cloud-rust/
    /// [working with long-running operations]: https://googleapis.github.io/google-cloud-rust/working_with_long_running_operations.html
    pub fn rename_folder(&self) -> super::builder::storage_control::RenameFolder {
        self.control.rename_folder()
    }

    /// Returns the storage layout configuration for a given bucket.
    pub fn get_storage_layout(&self) -> super::builder::storage_control::GetStorageLayout {
        self.control.get_storage_layout()
    }

    /// Creates a new managed folder.
    pub fn create_managed_folder(&self) -> super::builder::storage_control::CreateManagedFolder {
        self.control.create_managed_folder()
    }

    /// Permanently deletes an empty managed folder.
    pub fn delete_managed_folder(&self) -> super::builder::storage_control::DeleteManagedFolder {
        self.control.delete_managed_folder()
    }

    /// Returns metadata for the specified managed folder.
    pub fn get_managed_folder(&self) -> super::builder::storage_control::GetManagedFolder {
        self.control.get_managed_folder()
    }

    /// Retrieves a list of managed folders for a given bucket.
    pub fn list_managed_folders(&self) -> super::builder::storage_control::ListManagedFolders {
        self.control.list_managed_folders()
    }

    /// Creates an Anywhere Cache instance.
    ///
    /// # Long running operations
    ///
    /// This method is used to start, and/or poll a [long-running Operation].
    /// The [Working with long-running operations] chapter in the [user guide]
    /// covers these operations in detail.
    ///
    /// [long-running operation]: https://google.aip.dev/151
    /// [user guide]: https://googleapis.github.io/google-cloud-rust/
    /// [working with long-running operations]: https://googleapis.github.io/google-cloud-rust/working_with_long_running_operations.html
    pub fn create_anywhere_cache(&self) -> super::builder::storage_control::CreateAnywhereCache {
        self.control.create_anywhere_cache()
    }

    /// Updates an Anywhere Cache instance. Mutable fields include `ttl` and
    /// `admission_policy`.
    ///
    /// # Long running operations
    ///
    /// This method is used to start, and/or poll a [long-running Operation].
    /// The [Working with long-running operations] chapter in the [user guide]
    /// covers these operations in detail.
    ///
    /// [long-running operation]: https://google.aip.dev/151
    /// [user guide]: https://googleapis.github.io/google-cloud-rust/
    /// [working with long-running operations]: https://googleapis.github.io/google-cloud-rust/working_with_long_running_operations.html
    pub fn update_anywhere_cache(&self) -> super::builder::storage_control::UpdateAnywhereCache {
        self.control.update_anywhere_cache()
    }

    /// Disables an Anywhere Cache instance. A disabled instance is read-only. The
    /// disablement could be revoked by calling ResumeAnywhereCache. The cache
    /// instance will be deleted automatically if it remains in the disabled state
    /// for at least one hour.
    pub fn disable_anywhere_cache(&self) -> super::builder::storage_control::DisableAnywhereCache {
        self.control.disable_anywhere_cache()
    }

    /// Pauses an Anywhere Cache instance.
    pub fn pause_anywhere_cache(&self) -> super::builder::storage_control::PauseAnywhereCache {
        self.control.pause_anywhere_cache()
    }

    /// Resumes a disabled or paused Anywhere Cache instance.
    pub fn resume_anywhere_cache(&self) -> super::builder::storage_control::ResumeAnywhereCache {
        self.control.resume_anywhere_cache()
    }

    /// Gets an Anywhere Cache instance.
    pub fn get_anywhere_cache(&self) -> super::builder::storage_control::GetAnywhereCache {
        self.control.get_anywhere_cache()
    }

    /// Lists Anywhere Cache instances for a given bucket.
    pub fn list_anywhere_caches(&self) -> super::builder::storage_control::ListAnywhereCaches {
        self.control.list_anywhere_caches()
    }

    /// Returns the Project scoped singleton IntelligenceConfig resource.
    pub fn get_project_intelligence_config(
        &self,
    ) -> super::builder::storage_control::GetProjectIntelligenceConfig {
        self.control.get_project_intelligence_config()
    }

    /// Updates the Project scoped singleton IntelligenceConfig resource.
    pub fn update_project_intelligence_config(
        &self,
    ) -> super::builder::storage_control::UpdateProjectIntelligenceConfig {
        self.control.update_project_intelligence_config()
    }

    /// Returns the Folder scoped singleton IntelligenceConfig resource.
    pub fn get_folder_intelligence_config(
        &self,
    ) -> super::builder::storage_control::GetFolderIntelligenceConfig {
        self.control.get_folder_intelligence_config()
    }

    /// Updates the Folder scoped singleton IntelligenceConfig resource.
    pub fn update_folder_intelligence_config(
        &self,
    ) -> super::builder::storage_control::UpdateFolderIntelligenceConfig {
        self.control.update_folder_intelligence_config()
    }

    /// Returns the Organization scoped singleton IntelligenceConfig resource.
    pub fn get_organization_intelligence_config(
        &self,
    ) -> super::builder::storage_control::GetOrganizationIntelligenceConfig {
        self.control.get_organization_intelligence_config()
    }

    /// Updates the Organization scoped singleton IntelligenceConfig resource.
    pub fn update_organization_intelligence_config(
        &self,
    ) -> super::builder::storage_control::UpdateOrganizationIntelligenceConfig {
        self.control.update_organization_intelligence_config()
    }

    /// Provides the [Operations][google.longrunning.Operations] service functionality in this service.
    ///
    /// [google.longrunning.Operations]: longrunning::client::Operations
    pub fn get_operation(&self) -> super::builder::storage_control::GetOperation {
        self.control.get_operation()
    }

    /// Creates a new client from the provided stub.
    ///
    /// The most common case for calling this function is in tests mocking the
    /// client's behavior.
    pub fn from_stub<T>(stub: T) -> Self
    where
        T: super::stub::StorageControl + 'static,
    {
        let stub = std::sync::Arc::new(stub);
        Self {
            storage: super::generated::gapic::client::StorageControl::from_stub(stub.clone()),
            control: super::generated::gapic_control::client::StorageControl::from_stub(stub),
        }
    }

    pub(crate) async fn new(
        config: gaxi::options::ClientConfig,
    ) -> gax::client_builder::Result<Self> {
        let storage = super::generated::gapic::client::StorageControl::new(config.clone()).await?;
        let control = super::generated::gapic_control::client::StorageControl::new(config).await?;
        Ok(Self { storage, control })
    }
}

/// A builder for [StorageControl].
///
/// ```
/// # tokio_test::block_on(async {
/// # use google_cloud_storage_control::*;
/// # use client::ClientBuilder;
/// # use client::StorageControl;
/// let builder : ClientBuilder = StorageControl::builder();
/// let client = builder
///     .with_endpoint("https://storage.googleapis.com")
///     .build().await?;
/// # gax::client_builder::Result::<()>::Ok(()) });
/// ```
pub type ClientBuilder =
    gax::client_builder::ClientBuilder<client_builder::Factory, gaxi::options::Credentials>;

pub(crate) mod client_builder {
    use super::StorageControl;
    pub struct Factory;
    impl gax::client_builder::internal::ClientFactory for Factory {
        type Client = StorageControl;
        type Credentials = gaxi::options::Credentials;
        async fn build(
            self,
            config: gaxi::options::ClientConfig,
        ) -> gax::client_builder::Result<Self::Client> {
            Self::Client::new(config).await
        }
    }
}

#[cfg(test)]
mod test {
    use super::StorageControl;

    #[tokio::test]
    async fn builder() -> anyhow::Result<()> {
        let _ = StorageControl::builder()
            .with_credentials(auth::credentials::testing::test_credentials())
            .build()
            .await?;
        Ok(())
    }
}
