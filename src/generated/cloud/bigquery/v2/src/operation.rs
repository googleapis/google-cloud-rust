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

#[derive(Debug)]
pub(crate) struct JobRetryPolicy {
    pub job_level_retry_limit: usize,
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
        // Scaffolding: just pass through to standard poller for now
        self.builder.poller().until_done().await
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
