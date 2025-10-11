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

//! Optimistic Concurrency Control (OCC) loop for safe IAM policy updates.
//!
//! This module provides functions for safely updating IAM policies in the presence
//! of concurrent modifications. It implements an OCC loop algorithm using etag to detect
//! conflicts and automatically retry on concurrent changes.
//!
//! # Algorithm
//!
//! 1. Get current policy with etag (`get_iam_policy`)
//! 2. Apply modifications via user callback
//! 3. Attempt to set updated policy with etag (`set_iam_policy`)
//! 4. If ABORTED error (concurrent change) → retry with backoff
//! 5. If other error → immediate failure
//!
//! # Examples
//!
//! Basic usage:
//! ```no_run
//! # use google_cloud_storage::client::StorageControl;
//! # use iam_v1::model::Binding;
//! # async fn example(client: &StorageControl) -> anyhow::Result<()> {
//! use google_cloud_storage::iam_occ::*;
//!
//! let policy = update_iam_policy_with_occ(
//!     client,
//!     "projects/_/buckets/my-bucket",
//!     Box::new(|mut policy| {
//!         policy.bindings.push(
//!             Binding::new()
//!                 .set_role("roles/storage.admin")
//!                 .set_members(["user:alice@example.com"])
//!         );
//!         Ok(Some(policy))
//!     }),
//!     OccConfig::default(),
//! ).await?;
//! # Ok(())
//! # }
//! ```

use crate::client::StorageControl;
use crate::{Error, Result};
use iam_v1::model::Policy;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Callback function for updating an IAM policy.
///
/// Takes the current [`Policy`] and returns the updated version.
/// Returning `None` cancels the operation without error.
///
/// # Parameters
/// * `Policy` - current policy to update
///
/// # Returns
/// * `Ok(Some(Policy))` - updated policy to apply
/// * `Ok(None)` - cancel operation (not an error)
/// * `Err(Error)` - error during update
pub type IamUpdater<'a> = Box<dyn (FnMut(Policy) -> Result<Option<Policy>>) + Send + 'a>;

/// Configuration for the OCC loop retry mechanism.
///
/// Controls retry loop behavior: maximum number of attempts,
/// maximum execution time, and backoff strategy between attempts.
///
/// # Examples
///
/// Using default configuration:
/// ```
/// use google_cloud_storage::iam_occ::OccConfig;
/// let config = OccConfig::default();
/// ```
///
/// Custom configuration:
/// ```
/// use google_cloud_storage::iam_occ::OccConfig;
/// use gax::exponential_backoff::ExponentialBackoffBuilder;
/// use std::time::Duration;
///
/// let backoff = ExponentialBackoffBuilder::new()
///     .with_initial_delay(Duration::from_millis(100))
///     .with_maximum_delay(Duration::from_secs(5))
///     .build()
///     .expect("valid backoff config");
///
/// let config = OccConfig {
///     max_attempts: 5,
///     max_duration: Duration::from_secs(10),
///     backoff_policy: std::sync::Arc::new(backoff),
/// };
/// ```
#[derive(Clone, Debug)]
pub struct OccConfig {
    /// Maximum number of retry attempts.
    ///
    /// Default: 10
    pub max_attempts: u32,

    /// Maximum total time for the retry loop.
    ///
    /// Default: 30 seconds
    pub max_duration: Duration,

    /// Backoff policy for delays between retry attempts.
    ///
    /// Default: exponential backoff with `100ms` initial delay
    pub backoff_policy: Arc<dyn gax::backoff_policy::BackoffPolicy>,
}

impl Default for OccConfig {
    fn default() -> Self {
        let backoff = gax::exponential_backoff::ExponentialBackoffBuilder::new()
            .with_initial_delay(Duration::from_millis(100))
            .with_maximum_delay(Duration::from_secs(32))
            .with_scaling(2.0)
            .build()
            .expect("default backoff config is valid");

        Self {
            max_attempts: 10,
            max_duration: Duration::from_secs(30),
            backoff_policy: Arc::new(backoff),
        }
    }
}

/// Internal state of the OCC retry loop.
///
/// Tracks attempt count, loop start time, and last error.
#[derive(Debug)]
struct OccLoopState {
    attempt_count: u32,
    start_time: Instant,
    last_error: Option<Error>,
}

impl OccLoopState {
    fn new() -> Self {
        Self {
            attempt_count: 0,
            start_time: Instant::now(),
            last_error: None,
        }
    }

    fn elapsed(&self) -> Duration {
        self.start_time.elapsed()
    }
}

/// Trait for IAM policy operations.
///
/// This trait abstracts the IAM policy get/set operations to enable testing
/// with mocks. In Phase 2, this will be moved to the gax layer as a public API.
#[cfg_attr(test, mockall::automock)]
#[async_trait::async_trait]
pub(crate) trait IamPolicyOperations {
    /// Gets the current IAM policy for a resource.
    ///
    /// # Arguments
    /// * `resource` - The resource name (e.g., "projects/_/buckets/my-bucket")
    ///
    /// # Returns
    /// The current policy with etag
    async fn get_iam_policy(&self, resource: &str) -> Result<Policy>;

    /// Sets the IAM policy for a resource.
    ///
    /// # Arguments
    /// * `resource` - The resource name
    /// * `policy` - The policy to set (must include etag for OCC)
    ///
    /// # Returns
    /// The updated policy
    async fn set_iam_policy(&self, resource: &str, policy: Policy) -> Result<Policy>;
}

/// Implementation of IamPolicyOperations for StorageControl.
///
/// This allows the OCC loop to work with the concrete StorageControl client
/// while remaining testable through the trait abstraction.
#[async_trait::async_trait]
impl IamPolicyOperations for StorageControl {
    async fn get_iam_policy(&self, resource: &str) -> Result<Policy> {
        self.get_iam_policy().set_resource(resource).send().await
    }

    async fn set_iam_policy(&self, resource: &str, policy: Policy) -> Result<Policy> {
        self.set_iam_policy()
            .set_resource(resource)
            .set_policy(policy)
            .send()
            .await
    }
}

/// Generic OCC loop implementation that works with any IAM policy operations client.
///
/// This is the internal implementation that supports both real clients and mocks
/// for testing. The public API wraps this function with the concrete StorageControl type.
///
/// # Type Parameters
///
/// * `C` - Client type that implements IamPolicyOperations
///
/// # Arguments
///
/// * `client` - Client implementing IAM policy operations
/// * `resource` - Resource name (e.g., "projects/_/buckets/my-bucket")
/// * `updater` - Function to update the policy
/// * `config` - OCC configuration (attempts, duration, backoff)
async fn update_iam_policy_with_occ_impl<C>(
    client: &C,
    resource: impl Into<String>,
    mut updater: IamUpdater<'_>,
    config: OccConfig,
) -> Result<Policy>
where
    C: IamPolicyOperations,
{
    let resource = resource.into();
    let mut state = OccLoopState::new();

    loop {
        if state.attempt_count >= config.max_attempts {
            return Err(Error::exhausted(format!(
                "OCC loop exceeded maximum attempts ({})",
                config.max_attempts
            )));
        }

        let elapsed = state.elapsed();
        if elapsed >= config.max_duration {
            return Err(Error::exhausted(format!(
                "OCC loop exceeded maximum duration ({:?})",
                config.max_duration
            )));
        }

        let current_policy = match client.get_iam_policy(&resource).await {
            Ok(response) => response,
            Err(e) if !is_retryable_error(&e) => {
                return Err(e);
            }
            Err(e) => {
                state.attempt_count += 1;
                state.last_error = Some(e);
                apply_backoff(&config, &state).await;
                continue;
            }
        };

        let updated_policy = match updater(current_policy) {
            Ok(Some(p)) => p,
            Ok(None) => {
                return Err(Error::exhausted("Policy update cancelled by updater"));
            }
            Err(e) => {
                return Err(e);
            }
        };

        match client.set_iam_policy(&resource, updated_policy).await {
            Ok(response) => {
                return Ok(response);
            }
            Err(e) if is_aborted_error(&e) => {
                tracing::debug!(
                    attempt = state.attempt_count + 1,
                    elapsed_ms = state.elapsed().as_millis(),
                    "IAM policy update aborted due to concurrent change, retrying..."
                );
                state.attempt_count += 1;
                state.last_error = Some(e);
                apply_backoff(&config, &state).await;
                continue;
            }
            Err(e) => {
                return Err(e);
            }
        }
    }
}

/// Updates an IAM policy using an OCC loop for safe concurrent modifications.
///
/// This function implements the Optimistic Concurrency Control pattern for safely
/// updating IAM policies. It automatically handles concurrent changes through
/// a retry loop with exponential backoff.
///
/// # Algorithm
///
/// 1. Get current policy with etag
/// 2. Call updater callback to modify policy
/// 3. Set updated policy with etag to prevent race conditions
/// 4. If ABORTED (concurrent change) → retry with backoff
/// 5. If other error → immediate failure
///
/// # Arguments
///
/// * `client` - Storage control client for IAM operations
/// * `resource` - Resource name (e.g., "projects/_/buckets/my-bucket")
/// * `updater` - Function to update the policy
/// * `config` - OCC configuration (attempts, duration, backoff)
///
/// # Returns
///
/// The updated [`Policy`] after successful application of changes.
///
/// # Errors
///
/// * If the updater function returns an error
/// * If retry attempts or time limit are exhausted
/// * If the service returns a non-ABORTED error (not retryable)
/// * If the updater returns `None` (cancelled)
///
/// # Examples
///
/// Adding an IAM member:
/// ```no_run
/// # use google_cloud_storage::client::StorageControl;
/// # use iam_v1::model::Binding;
/// # async fn example(client: &StorageControl) -> anyhow::Result<()> {
/// use google_cloud_storage::iam_occ::*;
///
/// let policy = update_iam_policy_with_occ(
///     client,
///     "projects/_/buckets/my-bucket",
///     Box::new(|mut policy| {
///         policy.bindings.push(
///             Binding::new()
///                 .set_role("roles/storage.admin")
///                 .set_members(["user:alice@example.com"])
///         );
///         Ok(Some(policy))
///     }),
///     OccConfig::default(),
/// ).await?;
/// # Ok(())
/// # }
/// ```
///
/// Conditional update:
/// ```no_run
/// # use google_cloud_storage::client::StorageControl;
/// # use iam_v1::model::Binding;
/// # async fn example(client: &StorageControl) -> anyhow::Result<()> {
/// use google_cloud_storage::iam_occ::*;
///
/// let policy = update_iam_policy_with_occ(
///     client,
///     "projects/_/buckets/my-bucket",
///     Box::new(|mut policy| {
///         // Only if member doesn't already exist
///         let member = "user:bob@example.com";
///         let role = "roles/storage.viewer";
///
///         let already_exists = policy.bindings.iter().any(|b| {
///             b.role == role && b.members.contains(&member.to_string())
///         });
///
///         if !already_exists {
///             policy.bindings.retain(|b| b.role != role);
///             policy.bindings.push(
///                 Binding::new()
///                     .set_role(role)
///                     .set_members([member])
///             );
///             Ok(Some(policy))
///         } else {
///             // Member already exists - cancel operation
///             Ok(None)
///         }
///     }),
///     OccConfig::default(),
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub async fn update_iam_policy_with_occ(
    client: &StorageControl,
    resource: impl Into<String>,
    updater: IamUpdater<'_>,
    config: OccConfig,
) -> Result<Policy> {
    update_iam_policy_with_occ_impl(client, resource, updater, config).await
}

/// Checks if an error is ABORTED (concurrent change).
///
/// ABORTED errors indicate that a concurrent policy modification occurred
/// between the `get_iam_policy` and `set_iam_policy` operations.
fn is_aborted_error(error: &Error) -> bool {
    error
        .status()
        .map(|status| status.code == gax::error::rpc::Code::Aborted)
        .unwrap_or(false)
}

/// Checks if an error is retryable.
///
/// Retryable errors include transient failures such as unavailable,
/// deadline exceeded, and internal errors.
fn is_retryable_error(error: &Error) -> bool {
    error
        .status()
        .map(|status| {
            matches!(
                status.code,
                gax::error::rpc::Code::Unavailable
                    | gax::error::rpc::Code::DeadlineExceeded
                    | gax::error::rpc::Code::Internal
            )
        })
        .unwrap_or_else(|| error.is_timeout())
}

/// Applies backoff delay before the next retry attempt.
async fn apply_backoff(config: &OccConfig, state: &OccLoopState) {
    let retry_state = gax::retry_state::RetryState::new(true).set_start(state.start_time);
    let delay = config.backoff_policy.on_failure(&retry_state);

    tracing::debug!(
        delay_ms = delay.as_millis(),
        attempt = state.attempt_count,
        "Applying backoff before retry"
    );

    tokio::time::sleep(delay).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use gax::error::rpc::{Code, Status};

    #[test]
    fn test_occ_config_default() {
        let config = OccConfig::default();
        assert_eq!(config.max_attempts, 10);
        assert_eq!(config.max_duration, Duration::from_secs(30));
    }

    #[test]
    fn test_occ_loop_state_new() {
        let state = OccLoopState::new();
        assert_eq!(state.attempt_count, 0);
        assert!(state.last_error.is_none());
        assert!(state.elapsed() < Duration::from_millis(100));
    }

    // Success on first attempt
    #[tokio::test]
    async fn test_occ_success_first_attempt() {
        let mut mock = MockIamPolicyOperations::new();

        mock.expect_get_iam_policy()
            .times(1)
            .returning(|_| Ok(Policy::default()));

        mock.expect_set_iam_policy()
            .times(1)
            .returning(|_, p| Ok(p));

        let result = update_iam_policy_with_occ_impl(
            &mock,
            "test-resource",
            Box::new(|p| Ok(Some(p))),
            OccConfig::default(),
        )
        .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_occ_retry_on_aborted() {
        let mut mock = MockIamPolicyOperations::new();
        let mut seq = mockall::Sequence::new();

        // First attempt: get succeeds, set returns ABORTED
        mock.expect_get_iam_policy()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_| Ok(Policy::default()));

        mock.expect_set_iam_policy()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_, _| {
                Err(Error::service(
                    Status::default()
                        .set_code(Code::Aborted)
                        .set_message("concurrent change"),
                ))
            });

        // Second attempt: both succeed
        mock.expect_get_iam_policy()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_| Ok(Policy::default()));

        mock.expect_set_iam_policy()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_, p| Ok(p));

        let result = update_iam_policy_with_occ_impl(
            &mock,
            "test-resource",
            Box::new(|p| Ok(Some(p))),
            OccConfig::default(),
        )
        .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_occ_fail_on_permission_denied() {
        let mut mock = MockIamPolicyOperations::new();

        mock.expect_get_iam_policy()
            .times(1)
            .returning(|_| Ok(Policy::default()));

        mock.expect_set_iam_policy().times(1).returning(|_, _| {
            Err(Error::service(
                Status::default()
                    .set_code(Code::PermissionDenied)
                    .set_message("permission denied"),
            ))
        });

        let result = update_iam_policy_with_occ_impl(
            &mock,
            "test-resource",
            Box::new(|p| Ok(Some(p))),
            OccConfig::default(),
        )
        .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.status()
                .map(|s| s.code == Code::PermissionDenied)
                .unwrap_or(false)
        );
    }

    #[tokio::test]
    async fn test_occ_respects_max_attempts() {
        let mut mock = MockIamPolicyOperations::new();

        // Always return ABORTED
        mock.expect_get_iam_policy()
            .times(3)
            .returning(|_| Ok(Policy::default()));

        mock.expect_set_iam_policy().times(3).returning(|_, _| {
            Err(Error::service(
                Status::default()
                    .set_code(Code::Aborted)
                    .set_message("concurrent change"),
            ))
        });

        let config = OccConfig {
            max_attempts: 3,
            ..Default::default()
        };

        let result = update_iam_policy_with_occ_impl(
            &mock,
            "test-resource",
            Box::new(|p| Ok(Some(p))),
            config,
        )
        .await;

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("maximum attempts"));
    }

    #[tokio::test]
    async fn test_occ_respects_max_duration() {
        let mut mock = MockIamPolicyOperations::new();

        // Always return ABORTED to force retries
        mock.expect_get_iam_policy()
            .returning(|_| Ok(Policy::default()));

        mock.expect_set_iam_policy().returning(|_, _| {
            Err(Error::service(
                Status::default()
                    .set_code(Code::Aborted)
                    .set_message("concurrent change"),
            ))
        });

        let config = OccConfig {
            max_duration: Duration::from_millis(100),
            ..Default::default()
        };

        let result = update_iam_policy_with_occ_impl(
            &mock,
            "test-resource",
            Box::new(|p| Ok(Some(p))),
            config,
        )
        .await;

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("maximum duration"));
    }

    #[tokio::test]
    async fn test_updater_can_cancel() {
        let mut mock = MockIamPolicyOperations::new();

        mock.expect_get_iam_policy()
            .times(1)
            .returning(|_| Ok(Policy::default()));

        // set_iam_policy should NOT be called
        mock.expect_set_iam_policy().times(0);

        let result = update_iam_policy_with_occ_impl(
            &mock,
            "test-resource",
            Box::new(|_| Ok(None)), // Cancel!
            OccConfig::default(),
        )
        .await;

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("cancelled"));
    }
}
