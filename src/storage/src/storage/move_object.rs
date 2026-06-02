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

use crate::Result;
use crate::model::{MoveObjectRequest, Object};
use crate::storage::request_options::RequestOptions;
use std::sync::Arc;

/// Request builder for [Storage::move_object][crate::client::Storage::move_object] calls.
#[derive(Clone, Debug)]
pub struct MoveObject<S>
where
    S: crate::storage::stub::Storage + 'static,
{
    stub: Arc<S>,
    request: MoveObjectRequest,
    options: RequestOptions,
}

impl<S> MoveObject<S>
where
    S: crate::storage::stub::Storage + 'static,
{
    pub(crate) fn new(
        stub: Arc<S>,
        bucket: impl Into<String>,
        source_object: impl Into<String>,
        destination_object: impl Into<String>,
        options: RequestOptions,
    ) -> Self {
        let request = MoveObjectRequest {
            bucket: bucket.into(),
            source_object: source_object.into(),
            destination_object: destination_object.into(),
            ..Default::default()
        };
        Self {
            stub,
            request,
            options,
        }
    }

    // Preconditions

    /// Set a [request precondition] on the source object generation to match.
    ///
    /// With this precondition the request fails if the source object's current
    /// generation does not match the provided value.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let response = client
    ///     .move_object("projects/_/buckets/my-bucket", "source.txt", "dest.txt")
    ///     .set_if_source_generation_match(123456)
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    ///
    /// [request precondition]: https://cloud.google.com/storage/docs/request-preconditions
    pub fn set_if_source_generation_match(mut self, v: i64) -> Self {
        self.request.if_source_generation_match = Some(v);
        self
    }

    /// Set a [request precondition] on the source object generation to not match.
    ///
    /// With this precondition the request fails if the source object's current
    /// generation matches the provided value.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let response = client
    ///     .move_object("projects/_/buckets/my-bucket", "source.txt", "dest.txt")
    ///     .set_if_source_generation_not_match(123456)
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    ///
    /// [request precondition]: https://cloud.google.com/storage/docs/request-preconditions
    pub fn set_if_source_generation_not_match(mut self, v: i64) -> Self {
        self.request.if_source_generation_not_match = Some(v);
        self
    }

    /// Set a [request precondition] on the source object metageneration to match.
    ///
    /// With this precondition the request fails if the source object's current
    /// metageneration does not match the provided value.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let response = client
    ///     .move_object("projects/_/buckets/my-bucket", "source.txt", "dest.txt")
    ///     .set_if_source_metageneration_match(1)
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    ///
    /// [request precondition]: https://cloud.google.com/storage/docs/request-preconditions
    pub fn set_if_source_metageneration_match(mut self, v: i64) -> Self {
        self.request.if_source_metageneration_match = Some(v);
        self
    }

    /// Set a [request precondition] on the source object metageneration to not match.
    ///
    /// With this precondition the request fails if the source object's current
    /// metageneration matches the provided value.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let response = client
    ///     .move_object("projects/_/buckets/my-bucket", "source.txt", "dest.txt")
    ///     .set_if_source_metageneration_not_match(1)
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    ///
    /// [request precondition]: https://cloud.google.com/storage/docs/request-preconditions
    pub fn set_if_source_metageneration_not_match(mut self, v: i64) -> Self {
        self.request.if_source_metageneration_not_match = Some(v);
        self
    }

    /// Set a [request precondition] on the destination object generation to match.
    ///
    /// With this precondition the request fails if the destination object's current
    /// generation does not match the provided value. A common value is `0`, which
    /// prevents the move from succeeding if a destination object already exists.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let response = client
    ///     .move_object("projects/_/buckets/my-bucket", "source.txt", "dest.txt")
    ///     .set_if_generation_match(0)
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    ///
    /// [request precondition]: https://cloud.google.com/storage/docs/request-preconditions
    pub fn set_if_generation_match(mut self, v: i64) -> Self {
        self.request.if_generation_match = Some(v);
        self
    }

    /// Set a [request precondition] on the destination object generation to not match.
    ///
    /// With this precondition the request fails if the destination object's current
    /// generation matches the provided value.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let response = client
    ///     .move_object("projects/_/buckets/my-bucket", "source.txt", "dest.txt")
    ///     .set_if_generation_not_match(0)
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    ///
    /// [request precondition]: https://cloud.google.com/storage/docs/request-preconditions
    pub fn set_if_generation_not_match(mut self, v: i64) -> Self {
        self.request.if_generation_not_match = Some(v);
        self
    }

    /// Set a [request precondition] on the destination object metageneration to match.
    ///
    /// With this precondition the request fails if the destination object's current
    /// metageneration does not match the provided value.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let response = client
    ///     .move_object("projects/_/buckets/my-bucket", "source.txt", "dest.txt")
    ///     .set_if_metageneration_match(1)
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    ///
    /// [request precondition]: https://cloud.google.com/storage/docs/request-preconditions
    pub fn set_if_metageneration_match(mut self, v: i64) -> Self {
        self.request.if_metageneration_match = Some(v);
        self
    }

    /// Set a [request precondition] on the destination object metageneration to not match.
    ///
    /// With this precondition the request fails if the destination object's current
    /// metageneration matches the provided value.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let response = client
    ///     .move_object("projects/_/buckets/my-bucket", "source.txt", "dest.txt")
    ///     .set_if_metageneration_not_match(1)
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    ///
    /// [request precondition]: https://cloud.google.com/storage/docs/request-preconditions
    pub fn set_if_metageneration_not_match(mut self, v: i64) -> Self {
        self.request.if_metageneration_not_match = Some(v);
        self
    }

    // Common options

    /// Configure the idempotency for this move request.
    ///
    /// By default, the client library treats move requests without preconditions
    /// as non-idempotent. Atomic moves may succeed multiple times, but retrying
    /// them without preconditions could cause unforeseen outcomes.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let response = client
    ///     .move_object("projects/_/buckets/my-bucket", "source.txt", "dest.txt")
    ///     .with_idempotency(true)
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn with_idempotency(mut self, v: bool) -> Self {
        self.options.idempotency = Some(v);
        self
    }

    /// The retry policy used for this request.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # use google_cloud_storage::retry_policy::RetryableErrors;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// use std::time::Duration;
    /// use google_cloud_gax::retry_policy::RetryPolicyExt;
    /// let response = client
    ///     .move_object("projects/_/buckets/my-bucket", "source.txt", "dest.txt")
    ///     .with_retry_policy(
    ///         RetryableErrors
    ///             .with_attempt_limit(5)
    ///             .with_time_limit(Duration::from_secs(90)),
    ///     )
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn with_retry_policy<V: Into<google_cloud_gax::retry_policy::RetryPolicyArg>>(
        mut self,
        v: V,
    ) -> Self {
        self.options.retry_policy = v.into().into();
        self
    }

    /// The backoff policy used for this request.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// use std::time::Duration;
    /// use google_cloud_gax::exponential_backoff::ExponentialBackoff;
    /// let response = client
    ///     .move_object("projects/_/buckets/my-bucket", "source.txt", "dest.txt")
    ///     .with_backoff_policy(ExponentialBackoff::default())
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn with_backoff_policy<V: Into<google_cloud_gax::backoff_policy::BackoffPolicyArg>>(
        mut self,
        v: V,
    ) -> Self {
        self.options.backoff_policy = v.into().into();
        self
    }

    /// The retry throttler used for this request.
    ///
    /// Most of the time you want to use the same throttler for all the requests
    /// in a client, and even the same throttler for many clients. Rarely it
    /// may be necessary to use a custom throttler for some subset of the
    /// requests.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let response = client
    ///     .move_object("projects/_/buckets/my-bucket", "source.txt", "dest.txt")
    ///     .with_retry_throttler(adhoc_throttler())
    ///     .send()
    ///     .await?;
    /// fn adhoc_throttler() -> google_cloud_gax::retry_throttler::SharedRetryThrottler {
    ///     # panic!();
    /// }
    /// # Ok(()) }
    /// ```
    pub fn with_retry_throttler<V: Into<google_cloud_gax::retry_throttler::RetryThrottlerArg>>(
        mut self,
        v: V,
    ) -> Self {
        self.options.retry_throttler = v.into().into();
        self
    }

    /// Sends the move object request to the server.
    pub async fn send(self) -> Result<Object> {
        self.stub.move_object(self.request, self.options).await
    }
}
