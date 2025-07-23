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
/// # use google_cloud_storage::client::StorageControl;
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
    storage: crate::generated::gapic::client::StorageControl,
    control: crate::generated::gapic_control::client::StorageControl,
}

include!("../generated/combined/client.rs.in");

/// A builder for [StorageControl].
///
/// ```
/// # tokio_test::block_on(async {
/// # use google_cloud_storage::*;
/// # use builder::storage_control::ClientBuilder;
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
mod tests {
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
