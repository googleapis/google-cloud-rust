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

use crate::model::Job;
use google_cloud_gax::error::rpc::{Code, Status};

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

/// Determines if a BigQuery job failure reason is transient and eligible for job-level retry.
///
/// Returns `true` for retryable reasons (`jobBackendError`, `jobInternalError`, `jobRateLimitExceeded`,
/// `tableUnavailable`) per BigQuery error handling specification.
pub(crate) fn is_retryable_job_error(reason: &str) -> bool {
    matches!(
        reason,
        "jobBackendError" | "jobInternalError" | "jobRateLimitExceeded" | "tableUnavailable"
    )
}

/// Prepares a `Job` instance for retry by assigning a new synthetic job ID
/// and clearing existing execution status.
///
/// To preserve idempotency and avoid job execution collisions, each job-level retry must
/// use a unique job ID while retaining original reference details (project ID, location)
/// and configuration settings.
pub(crate) fn prepare_job_for_retry(mut job: Job) -> Job {
    let existing_ref = job.job_reference.unwrap_or_default();
    job.job_reference = Some(crate::model::JobReference {
        job_id: uuid::Uuid::new_v4().to_string(),
        ..existing_ref
    });
    job.status = None;
    job
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ErrorProto, Job, JobReference, JobStatus};
    use google_cloud_lro::internal::DiscoveryOperation;

    #[test]
    fn name_none() {
        let job = Job::default();
        assert_eq!(job.name(), None);
    }

    #[test]
    fn name_some() {
        let job = Job {
            job_reference: Some(JobReference {
                job_id: "test-id".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(job.name().map(|s| s.as_str()), Some("test-id"));
    }

    #[test]
    fn done_none() {
        let job = Job::default();
        assert!(!job.done());
    }

    #[test]
    fn done_false() {
        let job = Job {
            status: Some(JobStatus {
                state: "RUNNING".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(!job.done());
    }

    #[test]
    fn done_true() {
        let job = Job {
            status: Some(JobStatus {
                state: "DONE".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(job.done());
    }

    #[test]
    fn error_none() {
        let job = Job::default();
        assert!(job.error().is_none());

        let job_no_error = Job {
            status: Some(JobStatus {
                state: "DONE".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(job_no_error.error().is_none());
    }

    #[test]
    fn error_some() {
        let job = Job {
            status: Some(JobStatus {
                state: "DONE".to_string(),
                error_result: Some(ErrorProto {
                    message: "test error".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        let err = job.error().expect("should have error");
        assert_eq!(err.code, Code::Unknown);
        assert_eq!(err.message, "test error");
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
        assert_eq!(policy.job_level_retry_limit, 3);
    }

    #[test]
    fn prepare_job_for_retry_generates_new_id_and_resets_status() {
        let original_job = Job {
            job_reference: Some(JobReference {
                project_id: "test-project".to_string(),
                job_id: "original-job-id".to_string(),
                location: Some("US".to_string()),
                ..Default::default()
            }),
            status: Some(JobStatus {
                state: "DONE".to_string(),
                error_result: Some(ErrorProto {
                    reason: "jobBackendError".to_string(),
                    message: "backend failed".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };

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
        let original_job = Job {
            job_reference: None,
            status: Some(JobStatus {
                state: "DONE".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };

        let retried_job = prepare_job_for_retry(original_job);
        assert!(retried_job.status.is_none());

        let ref_data = retried_job
            .job_reference
            .expect("should create job reference when missing");
        assert!(uuid::Uuid::parse_str(&ref_data.job_id).is_ok());
    }

    #[test]
    fn prepare_job_for_retry_preserves_job_configuration_and_metadata() {
        use crate::model::{JobConfiguration, JobConfigurationQuery};

        let original_job = Job {
            job_reference: Some(JobReference {
                project_id: "my-project".to_string(),
                job_id: "initial-id".to_string(),
                location: Some("EU".to_string()),
                ..Default::default()
            }),
            configuration: Some(JobConfiguration {
                query: Some(JobConfigurationQuery {
                    query: "SELECT 42".to_string(),
                    ..Default::default()
                }),
                labels: std::collections::HashMap::from([("env".to_string(), "test".to_string())]),
                ..Default::default()
            }),
            user_email: "user@example.com".to_string(),
            status: Some(JobStatus {
                state: "DONE".to_string(),
                error_result: Some(ErrorProto {
                    reason: "jobInternalError".to_string(),
                    message: "internal error".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };

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
        assert_eq!(policy.job_level_retry_limit, 3);

        policy.job_level_retry_limit = 5;
        assert_eq!(policy.job_level_retry_limit, 5);
    }
}

use crate::builder::job_service::InsertJob;
use google_cloud_gax::exponential_backoff::ExponentialBackoff;
use google_cloud_lro::Poller;

/// Configuration policy for BigQuery job-level retries.
#[derive(Debug)]
pub(crate) struct JobRetryPolicy {
    /// Maximum number of job-level retry attempts for retryable job errors.
    pub job_level_retry_limit: usize,
    /// Backoff strategy between retry attempts.
    pub backoff: ExponentialBackoff,
}

impl Default for JobRetryPolicy {
    fn default() -> Self {
        Self {
            job_level_retry_limit: 3,
            backoff: ExponentialBackoff::default(),
        }
    }
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

    /// Sets the maximum number of job-level retry attempts.
    pub fn with_job_retry_limit(mut self, limit: usize) -> Self {
        self.policy.job_level_retry_limit = limit;
        self
    }

    /// Sets the exponential backoff policy for job-level retries.
    pub fn with_job_retry_backoff(mut self, backoff: ExponentialBackoff) -> Self {
        self.policy.backoff = backoff;
        self
    }

    /// Polls the job until it is done, returning the final Job status.
    pub async fn until_done(self) -> google_cloud_gax::Result<Job> {
        let mut attempts = 0;
        let mut builder = self.builder;
        let backoff = self.policy.backoff;
        let start_time = std::time::Instant::now();
        use google_cloud_gax::backoff_policy::BackoffPolicy;

        loop {
            attempts += 1;

            let job_result = builder.clone().poller().until_done().await?;

            if let Some(status) = &job_result.status
                && let Some(err) = &status.error_result
                && is_retryable_job_error(&err.reason)
                && attempts < self.policy.job_level_retry_limit
            {
                let retry_job = prepare_job_for_retry(job_result);
                builder = builder.set_job(retry_job);

                let retry_state = google_cloud_gax::retry_state::RetryState::new(true)
                    .set_start(start_time)
                    .set_attempt_count(attempts as u32);
                let delay = backoff.on_failure(&retry_state);
                tokio::time::sleep(delay).await;
                continue;
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
