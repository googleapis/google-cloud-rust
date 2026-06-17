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

// TODO(#5592): remove after marking query structs public.
#![allow(dead_code, unused_imports)]

pub(crate) mod execution;
mod job_reference;
mod query_handle;
mod row;
mod run_query;
mod schema;

pub(crate) use job_reference::JobReference;
pub(crate) use query_handle::Query;
pub(crate) use row::Row;
pub(crate) use schema::Schema;

pub use run_query::{RunQuery, RunQueryRequest};

/// Result type for query execution.
pub type Result<T> = std::result::Result<T, crate::error::QueryError>;

#[cfg(test)]
pub(crate) mod tests {
    use google_cloud_bigquery_v2::Result;
    use google_cloud_bigquery_v2::client::JobService;
    use google_cloud_bigquery_v2::model::{
        GetQueryResultsRequest, GetQueryResultsResponse, InsertJobRequest, Job, PostQueryRequest,
        QueryResponse,
    };
    use google_cloud_gax::options::RequestOptions;
    use google_cloud_gax::polling_backoff_policy::PollingBackoffPolicy;
    use google_cloud_gax::polling_state::PollingState;
    use google_cloud_gax::response::Response;
    use std::sync::Arc;

    mockall::mock! {
        #[derive(Debug)]
        pub JobService {}
        impl google_cloud_bigquery_v2::stub::JobService for JobService {
            async fn insert_job(
                &self,
                req: InsertJobRequest,
                options: RequestOptions,
            ) -> Result<Response<Job>>;
            async fn query(
                &self,
                req: PostQueryRequest,
                options: RequestOptions,
            ) -> Result<Response<QueryResponse>>;
            async fn get_query_results(
                &self,
                req: GetQueryResultsRequest,
                options: RequestOptions,
            ) -> Result<Response<GetQueryResultsResponse>>;
        }
    }

    mockall::mock! {
        #[derive(Debug)]
        pub BackoffPolicy {}
        impl PollingBackoffPolicy for BackoffPolicy {
            fn wait_period(&self, _state: &PollingState) -> std::time::Duration;
        }
    }

    pub(crate) fn create_job_service(mock: MockJobService) -> Arc<JobService> {
        Arc::new(JobService::from_stub::<MockJobService>(Arc::new(mock)))
    }

    pub(crate) fn create_test_backoff_policy() -> MockBackoffPolicy {
        MockBackoffPolicy::new()
    }
}
