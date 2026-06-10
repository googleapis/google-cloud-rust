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

use crate::builder::job_service::InsertJob;

use crate::model::Job;
use google_cloud_lro::Poller;
use google_cloud_lro::internal::DiscoveryOperation;

impl DiscoveryOperation for Job {
    fn done(&self) -> bool {
        self.status
            .as_ref()
            .map(|s| s.state.as_str())
            .unwrap_or_default()
            == "DONE"
    }

    fn name(&self) -> Option<&String> {
        self.job_reference.as_ref().map(|r| &r.job_id)
    }
}

/// Extension trait for [`InsertJob`] to support Long-Running Operation (LRO) polling.
///
/// # Example
/// ```no_run
/// use google_cloud_bigquery_v2::client::JobService;
/// use google_cloud_bigquery_v2::model::{Job, JobConfiguration, JobConfigurationQuery};
/// use google_cloud_bigquery_v2::operation::InsertJobBuilderExt;
/// use google_cloud_lro::Poller;
///
/// async fn example(client: JobService, project_id: &str) -> Result<(), google_cloud_gax::error::Error> {
///     let mut poller = client
///         .insert_job()
///         .set_project_id(project_id)
///         .set_job(
///             Job::new().set_configuration(
///                 JobConfiguration::new().set_query(
///                     JobConfigurationQuery::new().set_query("SELECT 1")
///                 )
///             )
///         )
///         .poller();
///     
///     // Wait for the job to complete
///     let job = poller.until_done().await?;
///     
///     // Check for BigQuery specific errors inside the payload
///     if let Some(status) = job.status {
///         if let Some(err) = status.error_result {
///             println!("Job failed with: {}", err.message);
///         }
///     }
///     Ok(())
/// }
/// ```
pub trait InsertJobBuilderExt {
    /// Returns a poller to monitor the status of the inserted job.
    fn poller(self) -> impl Poller<Job, Job>;
}

impl InsertJobBuilderExt for InsertJob {
    fn poller(self) -> impl Poller<Job, Job> {
        let req = &self.0.request;
        let project_id = req.project_id.clone();
        let location = req
            .job
            .as_ref()
            .and_then(|j| j.job_reference.as_ref())
            .and_then(|r| r.location.clone());

        let stub = self.0.stub.clone();

        let start = move || {
            let req = self;
            async move { req.send().await }
        };

        let query = move |name: String| {
            let stub = stub.clone();
            let project_id = project_id.clone();
            let location = location.clone();
            async move {
                let mut b = crate::builder::job_service::GetJob::new(stub)
                    .set_project_id(project_id)
                    .set_job_id(name);
                if let Some(loc) = location {
                    b = b.set_location(loc);
                }
                let mut options = google_cloud_gax::options::RequestOptions::default();
                options.set_retry_policy(google_cloud_gax::retry_policy::NeverRetry);
                b.with_options(options).send().await
            }
        };

        let polling_error_policy =
            std::sync::Arc::new(google_cloud_gax::polling_error_policy::Aip194Strict);
        let polling_backoff_policy = std::sync::Arc::new(
            google_cloud_gax::exponential_backoff::ExponentialBackoff::default(),
        );

        google_cloud_lro::internal::new_discovery_poller(
            polling_error_policy,
            polling_backoff_policy,
            start,
            query,
        )
    }
}
