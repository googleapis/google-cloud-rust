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

use crate::builder::job_service::InsertJob;
use crate::model::Job;
use google_cloud_gax::backoff_policy::BackoffPolicy;
use google_cloud_gax::error::Error as GaxError;
use google_cloud_gax::error::rpc::{Code, Status};
use google_cloud_gax::exponential_backoff::ExponentialBackoff;
use google_cloud_gax::retry_state::RetryState;
use google_cloud_lro::Poller;

impl google_cloud_lro::internal::DiscoveryOperation for Job {
    fn name(&self) -> Option<&String> {
        self.job_reference.as_ref().map(|r| &r.job_id)
    }

    fn done(&self) -> bool {
        self.status
            .as_ref()
            .map(|s| s.state == "DONE")
            .unwrap_or(false)
    }

    fn error(&self) -> Option<Status> {
        self.status.as_ref().and_then(|s| {
            s.error_result.as_ref().map(|e| {
                Status::default()
                    .set_code(Code::Unknown)
                    .set_message(e.message.clone())
            })
        })
    }
}

/// Determines if a BigQuery job failure reason is transient and eligible for
/// job-level retry.
///
/// Returns `true` for retryable reasons (`jobBackendError`,
/// `jobInternalError`, `jobRateLimitExceeded`, `tableUnavailable`) per
/// BigQuery error handling specification.
#[allow(dead_code)]
pub(crate) fn is_retryable_job_error(reason: &str) -> bool {
    matches!(
        reason,
        "jobBackendError" | "jobInternalError" | "jobRateLimitExceeded" | "tableUnavailable"
    )
}

/// Prepares a `Job` instance for retry by assigning a new synthetic job ID
/// and clearing existing execution status.
///
/// To preserve idempotency and avoid job execution collisions, each job-level
/// retry must use a unique job ID while retaining original reference details
/// (project ID, location) and configuration settings.
#[allow(dead_code)]
pub(crate) fn prepare_job_for_retry(mut job: Job) -> Job {
    job.job_reference.get_or_insert_default().job_id = uuid::Uuid::new_v4().to_string();
    job.status = None;
    job
}

/// Configuration policy for BigQuery job-level retries.
#[derive(Debug)]
pub(crate) struct JobRetryPolicy {
    /// Maximum number of general job-level attempts for retryable job errors.
    pub job_level_attempt_limit: u32,
    /// Backoff strategy between retry attempts.
    pub backoff: ExponentialBackoff,
}

impl Default for JobRetryPolicy {
    fn default() -> Self {
        Self {
            job_level_attempt_limit: 3,
            backoff: ExponentialBackoff::default(),
        }
    }
}

/// Errors returned by the JobPoller.
#[derive(Debug, thiserror::Error)]
pub enum JobPollerError {
    /// An error occurred during the RPC or LRO polling.
    #[error(transparent)]
    Rpc(#[from] GaxError),
    /// The job completed, but the BigQuery service reported an error in `status.error_result`.
    #[error("BigQuery job failed ({}): {}", .0.reason, .0.message)]
    Job(crate::model::ErrorProto),
}

/// A poller that monitors the status of an inserted BigQuery job and handles retries.
#[derive(Debug)]
pub struct JobPoller {
    policy: JobRetryPolicy,
    builder: InsertJob,
}

impl JobPoller {
    pub(crate) fn new(builder: InsertJob) -> Self {
        Self {
            policy: JobRetryPolicy::default(),
            builder,
        }
    }

    /// Sets the maximum number of job-level attempts.
    pub fn with_attempt_limit(mut self, limit: u32) -> Self {
        self.policy.job_level_attempt_limit = limit;
        self
    }

    /// Sets the exponential backoff policy for job-level retries.
    pub fn with_job_retry_backoff(mut self, backoff: ExponentialBackoff) -> Self {
        self.policy.backoff = backoff;
        self
    }

    /// Polls the job until it is done, returning the final Job status.
    pub async fn until_done(self) -> Result<Job, JobPollerError> {
        let mut attempts = 0_u32;
        let mut builder = self.builder;
        let backoff = self.policy.backoff;
        let start_time = std::time::Instant::now();

        loop {
            // NOTE: the client library intercepts errors and retries internally
            // according to the policies set on `builder`.
            let job_result = builder.clone().poller().until_done().await?;
            attempts += 1;

            if let Some(status) = &job_result.status
                && let Some(err) = &status.error_result
            {
                if is_retryable_job_error(&err.reason)
                    && attempts < self.policy.job_level_attempt_limit
                {
                    let retry_job = prepare_job_for_retry(job_result);
                    builder = builder.set_job(retry_job);

                    let retry_state = RetryState::new(true)
                        .set_start(start_time)
                        .set_attempt_count(attempts);
                    let delay = backoff.on_failure(&retry_state);
                    tokio::time::sleep(delay).await;
                    continue;
                }
                return Err(JobPollerError::Job(err.clone()));
            }
            return Ok(job_result);
        }
    }
}

impl InsertJob {
    /// Returns a `JobPoller`, which can retry on [job-level errors].
    ///
    /// If the job fails with an internal error, the `JobPoller` will retry the
    /// `InsertJob` operation. Note that the client library will supply a
    /// synthetic job ID for any retries.
    ///
    /// ```no_run
    /// # async fn example(builder: google_cloud_bigquery_v2::builder::job_service::InsertJob) -> Result<(), Box<dyn std::error::Error>> {
    /// let job = builder.into_job_poller().until_done().await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// [job-level errors]: https://docs.cloud.google.com/bigquery/docs/error-messages#errortable
    pub fn into_job_poller(self) -> JobPoller {
        JobPoller::new(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{
        ErrorProto, JobConfiguration, JobConfigurationQuery, JobReference, JobStatus,
    };
    use google_cloud_lro::internal::DiscoveryOperation;

    #[test]
    fn name_none() {
        let job = Job::default();
        assert_eq!(job.name(), None);
    }

    #[test]
    fn name_some() {
        let job = Job::new().set_job_reference(JobReference::new().set_job_id("test-id"));
        assert_eq!(job.name().map(|s| s.as_str()), Some("test-id"));
    }

    #[test]
    fn done_none() {
        let job = Job::default();
        assert!(!job.done());
    }

    #[test]
    fn done_false() {
        let job = Job::new().set_status(JobStatus::new().set_state("RUNNING"));
        assert!(!job.done());
    }

    #[test]
    fn done_true() {
        let job = Job::new().set_status(JobStatus::new().set_state("DONE"));
        assert!(job.done());
    }

    #[test]
    fn error_none() {
        let job = Job::default();
        assert!(job.error().is_none());

        let job_no_error = Job::new().set_status(JobStatus::new().set_state("DONE"));
        assert!(job_no_error.error().is_none());
    }

    #[test]
    fn error_some() {
        let job = Job::new()
            .set_status(JobStatus::new().set_error_result(ErrorProto::new().set_message("failed")));
        let err = job.error().expect("should have error");
        assert_eq!(err.code, Code::Unknown);
        assert_eq!(err.message, "failed");
    }

    #[test]
    fn retryable_job_errors() {
        assert!(is_retryable_job_error("jobBackendError"));
        assert!(is_retryable_job_error("jobInternalError"));
        assert!(is_retryable_job_error("jobRateLimitExceeded"));
        assert!(is_retryable_job_error("tableUnavailable"));

        assert!(!is_retryable_job_error("invalidQuery"));
        assert!(!is_retryable_job_error("accessDenied"));
        assert!(!is_retryable_job_error("notFound"));
        assert!(!is_retryable_job_error("backendError"));
        assert!(!is_retryable_job_error(""));
    }

    #[test]
    fn job_retry_policy_defaults() {
        let policy = JobRetryPolicy::default();
        assert_eq!(policy.job_level_attempt_limit, 3);
    }

    #[test]
    fn prepare_job_for_retry_generates_new_id_and_resets_status() {
        let original_job = Job::new()
            .set_job_reference(
                JobReference::new()
                    .set_project_id("test-project")
                    .set_job_id("original-job-id")
                    .set_location("US"),
            )
            .set_status(
                JobStatus::new().set_state("DONE").set_error_result(
                    ErrorProto::new()
                        .set_reason("jobBackendError")
                        .set_message("backend failed"),
                ),
            );

        let retried_job = prepare_job_for_retry(original_job);

        assert!(retried_job.status.is_none());

        let ref_data = retried_job
            .job_reference
            .expect("should have job reference");
        assert_eq!(ref_data.project_id, "test-project");
        assert_eq!(ref_data.location.as_deref(), Some("US"));
        assert_ne!(ref_data.job_id, "original-job-id");
        assert!(uuid::Uuid::parse_str(&ref_data.job_id).is_ok());
    }

    #[test]
    fn prepare_job_for_retry_handles_none_job_reference() {
        let original_job = Job::new().set_status(JobStatus::new().set_state("DONE"));

        let retried_job = prepare_job_for_retry(original_job);
        assert!(retried_job.status.is_none());

        let ref_data = retried_job
            .job_reference
            .expect("should create job reference when missing");
        assert!(uuid::Uuid::parse_str(&ref_data.job_id).is_ok());
    }

    #[test]
    fn prepare_job_for_retry_preserves_job_configuration_and_metadata() {
        let original_job = Job::new()
            .set_job_reference(
                JobReference::new()
                    .set_project_id("my-project")
                    .set_job_id("initial-id")
                    .set_location("EU"),
            )
            .set_configuration(
                JobConfiguration::new()
                    .set_query(JobConfigurationQuery::new().set_query("SELECT 42"))
                    .set_labels([("env".to_string(), "test".to_string())]),
            )
            .set_user_email("user@example.com")
            .set_status(
                JobStatus::new().set_state("DONE").set_error_result(
                    ErrorProto::new()
                        .set_reason("jobInternalError")
                        .set_message("internal error"),
                ),
            );

        let retried = prepare_job_for_retry(original_job);

        // Status must be reset to None for retry submission
        assert!(retried.status.is_none());

        // Configuration and user_email must be preserved
        assert_eq!(
            retried
                .configuration
                .as_ref()
                .and_then(|c| c.query.as_ref())
                .map(|q| q.query.as_str()),
            Some("SELECT 42")
        );
        assert_eq!(
            retried
                .configuration
                .as_ref()
                .and_then(|c| c.labels.get("env").map(|s| s.as_str())),
            Some("test")
        );
        assert_eq!(retried.user_email.as_str(), "user@example.com");

        // JobReference metadata preserved, but job_id replaced with a new valid UUID
        let ref_data = retried.job_reference.expect("must have reference");
        assert_eq!(ref_data.project_id, "my-project");
        assert_eq!(ref_data.location.as_deref(), Some("EU"));
        assert_ne!(ref_data.job_id, "initial-id");
        assert!(uuid::Uuid::parse_str(&ref_data.job_id).is_ok());
    }

    #[test]
    fn custom_retry_policy_builder() {
        let mut policy = JobRetryPolicy::default();
        assert_eq!(policy.job_level_attempt_limit, 3);

        policy.job_level_attempt_limit = 5;
        assert_eq!(policy.job_level_attempt_limit, 5);
    }
}
