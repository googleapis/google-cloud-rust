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
/// # async fn sample() -> anyhow::Result<()> {
/// # use google_cloud_storage::client::StorageControl;
/// let client = StorageControl::builder().build().await?;
/// // use `client` to make requests to Cloud Storage.
/// # Ok(()) }
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
    pub(crate) storage: crate::generated::gapic::client::StorageControl,
    pub(crate) control: crate::generated::gapic_control::client::StorageControl,
}

// Note that the `impl` is defined in `generated/client.rs`

impl StorageControl {
    /// Updates the IAM policy for a resource using an Optimistic Concurrency Control (OCC) loop.
    ///
    /// This method safely handles concurrent IAM policy updates by automatically retrying
    /// when conflicts are detected. It uses the policy's `etag` to prevent race conditions
    /// and overwrites from concurrent modifications.
    ///
    /// # Arguments
    ///
    /// * `resource` - The resource name (e.g., `"projects/_/buckets/my-bucket"`)
    /// * `updater` - A closure that modifies the policy. Return `None` to cancel the operation.
    ///
    /// # Returns
    ///
    /// The updated IAM policy after successful application.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The updater function returns an error
    /// - Maximum retry attempts are exhausted
    /// - Maximum retry duration is exceeded
    /// - A non-retryable error occurs (e.g., permission denied)
    ///
    /// # Examples
    ///
    /// Add an IAM member to a bucket:
    /// ```no_run
    /// # use google_cloud_storage::client::StorageControl;
    /// # use iam_v1::model::Binding;
    /// # async fn example(client: &StorageControl) -> anyhow::Result<()> {
    /// let policy = client
    ///     .update_iam_policy_with_retry(
    ///         "projects/_/buckets/my-bucket",
    ///         |mut policy| {
    ///             policy.bindings.push(
    ///                 Binding::new()
    ///                     .set_role("roles/storage.admin")
    ///                     .set_members(["user:alice@example.com"])
    ///             );
    ///             Ok(Some(policy))
    ///         },
    ///     )
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Conditionally add a binding only if it doesn't exist:
    /// ```rust
    /// # use google_cloud_storage::client::StorageControl;
    /// # async fn example(client: &StorageControl) -> anyhow::Result<()> {
    /// let policy = client
    ///     .update_iam_policy_with_retry(
    ///         "projects/_/buckets/my-bucket",
    ///         |mut policy| {
    ///             let member = "user:bob@example.com";
    ///             let role = "roles/storage.viewer";
    ///             
    ///             // Check if binding already exists
    ///             let exists = policy.bindings.iter().any(|b| {
    ///                 b.role == role && b.members.contains(&member.to_string())
    ///             });
    ///             
    ///             if !exists {
    ///                 policy.bindings.push(
    ///                     iam_v1::model::Binding::new()
    ///                         .set_role(role)
    ///                         .set_members([member])
    ///                 );
    ///                 Ok(Some(policy))
    ///             } else {
    ///                 // Already exists, cancel operation
    ///                 Ok(None)
    ///             }
    ///         },
    ///     )
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn update_iam_policy_with_retry<F>(
        &self,
        resource: impl Into<String>,
        updater: F,
    ) -> crate::Result<iam_v1::model::Policy>
    where
        F: FnMut(iam_v1::model::Policy) -> crate::Result<Option<iam_v1::model::Policy>>
            + Send
            + 'static,
    {
        crate::iam_occ::update_iam_policy_with_occ(
            self,
            resource,
            Box::new(updater),
            crate::iam_occ::OccConfig::default(),
        )
        .await
    }

    /// Updates the IAM policy with a custom OCC configuration.
    ///
    /// This is similar to [`update_iam_policy_with_retry`][Self::update_iam_policy_with_retry],
    /// but allows customizing the retry behavior (max attempts, timeout, backoff strategy).
    ///
    /// # Arguments
    ///
    /// * `resource` - The resource name
    /// * `updater` - A closure that modifies the policy
    /// * `config` - Custom OCC configuration
    ///
    /// # Examples
    ///
    /// Use custom retry configuration:
    /// ```rust
    /// # use google_cloud_storage::client::StorageControl;
    /// # use google_cloud_storage::iam_occ::OccConfig;
    /// # use std::time::Duration;
    /// # async fn example(client: &StorageControl) -> anyhow::Result<()> {
    /// let config = OccConfig {
    ///     max_attempts: 5,
    ///     max_duration: Duration::from_secs(10),
    ///     ..Default::default()
    /// };
    ///
    /// let policy = client
    ///     .update_iam_policy_with_retry_config(
    ///         "projects/_/buckets/my-bucket",
    ///         |mut policy| {
    ///             // Update policy...
    ///             Ok(Some(policy))
    ///         },
    ///         config,
    ///     )
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn update_iam_policy_with_retry_config<F>(
        &self,
        resource: impl Into<String>,
        updater: F,
        config: crate::iam_occ::OccConfig,
    ) -> crate::Result<iam_v1::model::Policy>
    where
        F: FnMut(iam_v1::model::Policy) -> crate::Result<Option<iam_v1::model::Policy>>
            + Send
            + 'static,
    {
        crate::iam_occ::update_iam_policy_with_occ(self, resource, Box::new(updater), config).await
    }
}

/// A builder for [StorageControl].
///
/// ```
/// # async fn sample() -> anyhow::Result<()> {
/// # use google_cloud_storage::*;
/// # use builder::storage_control::ClientBuilder;
/// # use client::StorageControl;
/// let builder : ClientBuilder = StorageControl::builder();
/// let client = builder
///     .with_endpoint("https://storage.googleapis.com")
///     .build().await?;
/// # Ok(()) }
/// ```
pub type ClientBuilder =
    gax::client_builder::ClientBuilder<client_builder::Factory, gaxi::options::Credentials>;

pub(crate) mod client_builder {
    use super::StorageControl;
    use std::sync::Arc;

    pub struct Factory;
    impl gax::client_builder::internal::ClientFactory for Factory {
        type Client = StorageControl;
        type Credentials = gaxi::options::Credentials;
        async fn build(
            self,
            mut config: gaxi::options::ClientConfig,
        ) -> gax::client_builder::Result<Self::Client> {
            if config.retry_policy.is_none() {
                config.retry_policy = Some(Arc::new(crate::retry_policy::storage_default()));
            }
            if config.backoff_policy.is_none() {
                config.backoff_policy = Some(Arc::new(crate::backoff_policy::default()));
            }
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
            .with_credentials(auth::credentials::anonymous::Builder::new().build())
            .build()
            .await?;
        Ok(())
    }
}
