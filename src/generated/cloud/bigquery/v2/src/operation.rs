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

use crate::builder::job_service::{GetQueryResults, InsertJob};
use crate::client::JobService;
use crate::model::{GetQueryResultsResponse, Job};
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

impl DiscoveryOperation for GetQueryResultsResponse {
    fn done(&self) -> bool {
        self.job_complete == Some(true)
    }

    fn name(&self) -> Option<&String> {
        self.job_reference.as_ref().map(|r| &r.job_id)
    }
}

pub trait InsertJobBuilderExt {
    fn poller(
        self,
        client: &JobService,
        project_id: impl Into<String>,
        location: Option<String>,
    ) -> impl Poller<Job, Job>;
}

impl InsertJobBuilderExt for InsertJob {
    fn poller(
        self,
        client: &JobService,
        project_id: impl Into<String>,
        location: Option<String>,
    ) -> impl Poller<Job, Job> {
        let client_clone = client.clone();
        let project_id = project_id.into();

        let start = move || {
            let req = self;
            async move { req.send().await }
        };

        let query = move |name: String| {
            let client = client_clone.clone();
            let project_id = project_id.clone();
            let location = location.clone();
            async move {
                let mut b = client.get_job().set_project_id(project_id).set_job_id(name);
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

pub trait GetQueryResultsBuilderExt {
    fn poller(
        self,
        client: &JobService,
        project_id: impl Into<String>,
        location: Option<String>,
    ) -> impl Poller<GetQueryResultsResponse, GetQueryResultsResponse>;
}

impl GetQueryResultsBuilderExt for GetQueryResults {
    fn poller(
        self,
        client: &JobService,
        project_id: impl Into<String>,
        location: Option<String>,
    ) -> impl Poller<GetQueryResultsResponse, GetQueryResultsResponse> {
        let client_clone = client.clone();
        let project_id = project_id.into();

        let start = move || {
            let req = self;
            async move { req.send().await }
        };

        let query = move |name: String| {
            let client = client_clone.clone();
            let project_id = project_id.clone();
            let location = location.clone();
            async move {
                let mut b = client
                    .get_query_results()
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
