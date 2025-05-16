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

//! Contains the Storage client and related types.

/// Implements a client for the Cloud Storage API.
///
/// # Example
/// ```
/// # tokio_test::block_on(async {
/// # use google_cloud_storage_control::client::Storage;
/// let client = Storage::builder().build().await?;
/// // use `client` to make requests to Cloud Storage.
/// # gax::Result::<()>::Ok(()) });
/// ```
///
/// # Configuration
///
/// To configure `Storage` use the `with_*` methods in the type returned
/// by [builder()][Storage::builder]. The default configuration should
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
/// `Storage` holds a connection pool internally, it is advised to
/// create one and the reuse it.  You do not need to wrap `Storage` in
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
pub struct Storage {
    storage: super::generated::gapic::client::Storage,
    control: super::generated::gapic_control::client::StorageControl,
}

impl Storage {
    /// Returns a builder for [Storage].
    ///
    /// ```no_run
    /// # tokio_test::block_on(async {
    /// # use google_cloud_storage_control::client::Storage;
    /// let client = Storage::builder().build().await?;
    /// # gax::Result::<()>::Ok(()) });
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
    /// # Parameters
    /// * `name` - the bucket name. In `projects/_/buckets/{bucket_id}` format.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage_control::client::Storage;
    /// async fn example(client: &Storage) -> gax::Result<()> {
    ///     client.delete_bucket("projects/_/buckets/my-bucket").send().await?;
    ///     Ok(())
    /// }
    /// ```
    pub fn delete_bucket<T: Into<String>>(&self, name: T) -> super::builder::storage::DeleteBucket {
        self.storage.delete_bucket().set_name(name)
    }

    /// Returns metadata for the specified bucket.
    ///
    /// # Parameters
    /// * `name` - the bucket name. In `projects/_/buckets/{bucket_id}` format.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage_control::client::Storage;
    /// async fn example(client: &Storage) -> gax::Result<()> {
    ///     let bucket = client.get_bucket("projects/_/buckets/my-bucket").send().await?;
    ///     assert_eq!(&bucket.name, "projects/_/buckets/my-bucket");
    ///     println!("bucket details={bucket:?}");
    ///     Ok(())
    /// }
    /// ```
    pub fn get_bucket<T: Into<String>>(&self, name: T) -> super::builder::storage::GetBucket {
        self.storage.get_bucket().set_name(name)
    }

    /// Creates a new bucket.
    ///
    /// # Parameters
    /// * `name` - the bucket name. In `projects/_/buckets/{bucket_id}` format.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage_control::client::Storage;
    /// async fn example(client: &Storage) -> gax::Result<()> {
    ///     let bucket = client.create_bucket("projects/my-project", "my-bucket").send().await?;
    ///     assert_eq!(&bucket.name, "projects/_/buckets/my-bucket");
    ///     println!("bucket details={bucket:?}");
    ///     Ok(())
    /// }
    /// ```
    pub fn create_bucket<V, U>(
        &self,
        parent: V,
        bucket_id: U,
    ) -> super::builder::storage::CreateBucket
    where
        V: Into<String>,
        U: Into<String>,
    {
        self.storage
            .create_bucket()
            .set_parent(parent)
            .set_bucket_id(bucket_id)
    }

    /// Retrieves a list of buckets for a given project.
    ///
    /// # Parameters
    /// * `parent` - the project name. In `projects/{project_id}` format.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage_control::client::Storage;
    /// async fn example(client: &Storage) -> gax::Result<()> {
    ///     use gax::paginator::{ItemPaginator, Paginator};
    ///     let mut items = client
    ///         .list_buckets("projects/my-project")
    ///         .by_item();
    ///     while let Some(bucket) = items.next().await {
    ///         let bucket = bucket?;
    ///         println!("  {bucket:?}");
    ///     }
    ///     Ok(())
    /// }
    /// ```
    pub fn list_buckets<T: Into<String>>(&self, parent: T) -> super::builder::storage::ListBuckets {
        self.storage.list_buckets().set_parent(parent)
    }

    /// Permanently deletes an object and its metadata.
    ///
    /// # Parameters
    /// * `bucket` - the bucket name containing the object. In
    ///   `projects/_/buckets/{bucket_id}` format.
    /// * `object` - the object name.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage_control::client::Storage;
    /// async fn example(client: &Storage) -> gax::Result<()> {
    ///     client.delete_object("projects/_/buckets/my-bucket", "my-object").send().await?;
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
    pub fn delete_object<T, U>(&self, bucket: T, object: U) -> super::builder::storage::DeleteObject
    where
        T: Into<String>,
        U: Into<String>,
    {
        self.storage
            .delete_object()
            .set_bucket(bucket)
            .set_object(object)
    }

    /// Retrieves the list of objects for a given bucket.
    ///
    /// # Parameters
    /// * `parent` - the bucket name. In `projects/_/buckets/{bucket_id}` format.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage_control::client::Storage;
    /// async fn example(client: &Storage) -> gax::Result<()> {
    ///     use gax::paginator::{ItemPaginator, Paginator};
    ///     let mut items = client
    ///         .list_objects("projects/_/buckets/my-bucket")
    ///         .by_item();
    ///     while let Some(object) = items.next().await {
    ///         let object = object?;
    ///         println!("  {object:?}");
    ///     }
    ///     Ok(())
    /// }
    /// ```
    pub fn list_objects<T: Into<String>>(&self, parent: T) -> super::builder::storage::ListObjects {
        self.storage.list_objects().set_parent(parent)
    }

    /// Gets the IAM policy for a specified bucket.
    ///
    /// # Parameters
    /// * `resource` should be
    ///   * `projects/_/buckets/{bucket}` for a bucket,
    ///   * `projects/_/buckets/{bucket}/objects/{object}` for an object, or
    ///   * `projects/_/buckets/{bucket}/managedFolders/{managedFolder}` for a
    ///     managed folder.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage_control::client::Storage;
    /// async fn example(client: &Storage) -> gax::Result<()> {
    ///     let policy = client
    ///         .get_iam_policy("projects/_/buckets/my-bucket")
    ///         .send()
    ///         .await?;
    ///     println!("policy details={policy:?}");
    ///     Ok(())
    /// }
    /// ```
    pub fn get_iam_policy(
        &self,
        resource: impl Into<String>,
    ) -> super::builder::storage::GetIamPolicy {
        self.storage.get_iam_policy().set_resource(resource.into())
    }

    /// Updates the IAM policy for a specified bucket.
    ///
    /// This is not an update. The supplied policy will overwrite any existing
    /// IAM Policy. You should first get the current IAM policy with
    /// `get_iam_policy()` and then modify that policy before supplying it to
    /// `set_iam_policy()`.
    ///
    /// # Parameters
    /// * `resource` should be
    ///   * `projects/_/buckets/{bucket}` for a bucket,
    ///   * `projects/_/buckets/{bucket}/objects/{object}` for an object, or
    ///   * `projects/_/buckets/{bucket}/managedFolders/{managedFolder}` for a
    ///     managed folder.
    ///
    /// # Example
    ///
    /// ```
    /// # use google_cloud_storage_control::client::Storage;
    /// # use iam_v1::model::Policy;
    /// async fn example(client: &Storage, updated_policy: Policy) -> gax::Result<()> {
    ///     let policy = client
    ///         .set_iam_policy("projects/_/buckets/my-bucket")
    ///         .set_update_mask(wkt::FieldMask::default().set_paths(["bindings"]))
    ///         .set_policy(updated_policy)
    ///         .send()
    ///         .await?;
    ///     println!("policy details={policy:?}");
    ///     Ok(())
    /// }
    /// ```
    pub fn set_iam_policy(
        &self,
        resource: impl Into<String>,
    ) -> super::builder::storage::SetIamPolicy {
        self.storage.set_iam_policy().set_resource(resource.into())
    }

    /// Tests a set of permissions on the given bucket, object, or managed folder
    /// to see which, if any, are held by the caller.
    ///
    /// # Parameters
    /// * `resource` should be
    ///   * `projects/_/buckets/{bucket}` for a bucket,
    ///   * `projects/_/buckets/{bucket}/objects/{object}` for an object, or
    ///   * `projects/_/buckets/{bucket}/managedFolders/{managedFolder}` for a
    ///     managed folder.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage_control::client::Storage;
    /// async fn example(client: &Storage) -> gax::Result<()> {
    ///     let response = client
    ///         .test_iam_permissions("projects/_/buckets/my-bucket")
    ///         .set_permissions(["storage.buckets.get"])
    ///         .send()
    ///         .await?;
    ///     println!("response details={response:?}");
    ///     Ok(())
    /// }
    /// ```
    pub fn test_iam_permissions(
        &self,
        resource: impl Into<String>,
    ) -> super::builder::storage::TestIamPermissions {
        self.storage
            .test_iam_permissions()
            .set_resource(resource.into())
    }

    /// Creates a new folder.
    ///
    /// This operation is only applicable to a hierarchical namespace enabled
    /// bucket.
    ///
    /// # Parameters
    /// * `parent` - the bucket name. In `projects/_/buckets/{bucket_id}`
    ///   format.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage_control::client::Storage;
    /// async fn example(client: &Storage) -> gax::Result<()> {
    ///     let folder = client
    ///         .create_folder("projects/my-project/buckets/my-bucket")
    ///         .set_folder_id("my-folder/my-subfolder/")
    ///         .send()
    ///         .await?;
    ///     println!("folder details={folder:?}");
    ///     Ok(())
    /// }
    /// ```
    pub fn create_folder(
        &self,
        parent: impl Into<String>,
    ) -> super::builder::storage::CreateFolder {
        self.control.create_folder().set_parent(parent.into())
    }

    /// Returns metadata for the specified folder.
    ///
    /// This operation is only applicable to a hierarchical namespace enabled
    /// bucket.
    ///
    /// # Parameters
    /// * `name` - the folder name. In
    ///   `projects/_/buckets/{bucket_id}/folders/{folder}` format.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage_control::client::Storage;
    /// async fn example(client: &Storage) -> gax::Result<()> {
    ///     let folder = client
    ///         .get_folder("projects/_/buckets/my-bucket/folders/my-folder/my-subfolder/")
    ///         .send()
    ///         .await?;
    ///     println!("folder details={folder:?}");
    ///     Ok(())
    /// }
    /// ```
    pub fn get_folder(&self, name: impl Into<String>) -> super::builder::storage::GetFolder {
        self.control.get_folder().set_name(name.into())
    }

    /// Permanently deletes an empty folder.
    ///
    /// This operation is only applicable to a hierarchical namespace enabled
    /// bucket.
    ///
    /// # Parameters
    /// * `name` - the folder name. In
    ///   `projects/_/buckets/{bucket_id}/folders/{folder}` format.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage_control::client::Storage;
    /// async fn example(client: &Storage) -> gax::Result<()> {
    ///     client
    ///         .delete_folder("projects/_/buckets/my-bucket/folders/my-folder/my-subfolder/")
    ///         .send()
    ///         .await?;
    ///     Ok(())
    /// }
    /// ```
    pub fn delete_folder(&self, name: impl Into<String>) -> super::builder::storage::DeleteFolder {
        self.control.delete_folder().set_name(name.into())
    }

    /// Retrieves a list of folders.
    ///
    /// This operation is only applicable to a hierarchical namespace enabled
    /// bucket.
    ///
    /// # Parameters
    /// * `parent` - the bucket name. In `projects/_/buckets/{bucket_id}` format.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage_control::client::Storage;
    /// async fn example(client: &Storage) -> gax::Result<()> {
    ///     use gax::paginator::ItemPaginator as _;
    ///     let mut folders = client
    ///         .list_folders("projects/_/buckets/my-bucket")
    ///         .by_item();
    ///     while let Some(folder) = folders.next().await {
    ///         let folder = folder?;
    ///         println!("  {folder:?}");
    ///     }
    ///     Ok(())
    /// }
    /// ```
    pub fn list_folders(&self, parent: impl Into<String>) -> super::builder::storage::ListFolders {
        self.control.list_folders().set_parent(parent.into())
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
    /// # use google_cloud_storage_control::client::Storage;
    /// async fn example(client: &Storage) -> gax::Result<()> {
    ///     use lro::Poller as _;
    ///     let folder = client.rename_folder()
    ///         .set_name("projects/_/buckets/my-bucket/folders/my-folder/my-subfolder/")
    ///         .set_destination_folder_id("my-folder/my-renamed-subfolder/")
    ///         .poller()
    ///         .until_done()
    ///         .await?;
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
    pub fn rename_folder(&self) -> super::builder::storage::RenameFolder {
        self.control.rename_folder()
    }

    /// Creates a new client from the provided stub.
    ///
    /// The most common case for calling this function is in tests mocking the
    /// client's behavior.
    pub fn from_stub<T>(stub: T) -> Self
    where
        T: super::stub::Storage + 'static,
    {
        let stub = std::sync::Arc::new(stub);
        Self {
            storage: super::generated::gapic::client::Storage::from_stub(stub.clone()),
            control: super::generated::gapic_control::client::StorageControl::from_stub(stub),
        }
    }

    pub(crate) async fn new(config: gaxi::options::ClientConfig) -> crate::Result<Self> {
        let storage = super::generated::gapic::client::Storage::new(config.clone()).await?;
        let control = super::generated::gapic_control::client::StorageControl::new(config).await?;
        Ok(Self { storage, control })
    }
}

/// A builder for [Storage].
///
/// ```
/// # tokio_test::block_on(async {
/// # use google_cloud_storage_control::*;
/// # use client::ClientBuilder;
/// # use client::Storage;
/// let builder : ClientBuilder = Storage::builder();
/// let client = builder
///     .with_endpoint("https://storage.googleapis.com")
///     .build().await?;
/// # gax::Result::<()>::Ok(()) });
/// ```
pub type ClientBuilder =
    gax::client_builder::ClientBuilder<client_builder::Factory, gaxi::options::Credentials>;

pub(crate) mod client_builder {
    use super::Storage;
    pub struct Factory;
    impl gax::client_builder::internal::ClientFactory for Factory {
        type Client = Storage;
        type Credentials = gaxi::options::Credentials;
        async fn build(self, config: gaxi::options::ClientConfig) -> gax::Result<Self::Client> {
            Self::Client::new(config).await
        }
    }
}
