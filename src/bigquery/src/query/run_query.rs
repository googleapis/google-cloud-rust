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

use crate::error::QueryError;
use crate::query::{Query, Result};
use google_cloud_bigquery_v2::client::JobService;
use google_cloud_bigquery_v2::model::{
    Job, JobConfiguration, JobConfigurationQuery, PostQueryRequest, QueryRequest,
};
use std::sync::Arc;

/// A unified request builder for configuring and running a SQL query.
/// It automatically routes to either `jobs.query` (fast path) or `jobs.insert` (job path)
/// depending on the configured fields.
#[derive(Clone)]
pub struct RunQuery {
    pub(crate) job_service: Arc<JobService>,
    pub(crate) request: RunQueryRequest,
    pub(crate) project_id: Option<String>,
}

impl RunQuery {
    /// Creates a new `RunQuery` builder for the given SQL query.
    pub fn new(job_service: Arc<JobService>, sql: String) -> Self {
        Self {
            job_service,
            request: RunQueryRequest::default()
                .set_query(sql)
                .set_use_legacy_sql(wkt::BoolValue::from(false)),
            project_id: None,
        }
    }

    /// Sets the project ID to override the default client project ID.
    pub fn with_project_id<S: Into<String>>(mut self, project_id: S) -> Self {
        self.project_id = Some(project_id.into());
        self
    }

    /// Executes the SQL query, routing internally to `jobs.query` (fast path)
    /// or `jobs.insert` (job path) depending on configured fields.
    pub async fn run(self) -> Result<Query> {
        let project_id = self.project_id.ok_or(QueryError::MissingProjectId)?;

        if self.request.force_job_path() {
            // Route to jobs.insert
            let job_config: JobConfiguration = self.request.into();
            let _job = Job::new().set_configuration(job_config);

            // TODO(#5844): implement jobs.insert query execution
            unimplemented!("jobs.insert query execution not yet implemented");
        } else {
            // Route to jobs.query
            let query_request: QueryRequest = self.request.into();
            let query_request = query_request.set_format_options(
                google_cloud_bigquery_v2::model::DataFormatOptions::new()
                    .set_use_int64_timestamp(true),
            );
            let _req = PostQueryRequest::new()
                .set_project_id(project_id)
                .set_query_request(query_request);

            // TODO(#5844): implement jobs.query query execution
            unimplemented!("jobs.query query execution not yet implemented");
        }
    }
}

include!("../generated/run_query_builder.rs");
include!("../generated/run_query_request.rs");

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[derive(Debug)]
    struct MockJobService;
    impl google_cloud_bigquery_v2::stub::JobService for MockJobService {}

    fn create_job_service() -> Arc<JobService> {
        Arc::new(JobService::from_stub::<MockJobService>(Arc::new(
            MockJobService,
        )))
    }

    #[test]
    fn test_new() {
        let job_service = create_job_service();
        let sql = "SELECT 1".to_string();
        let run_query = RunQuery::new(job_service, sql.clone());
        assert_eq!(run_query.request.query, sql);
        assert_eq!(
            run_query.request.use_legacy_sql,
            Some(wkt::BoolValue::from(false))
        );
        assert_eq!(run_query.project_id, None);
    }

    #[test]
    fn test_with_project_id() {
        let job_service = create_job_service();
        let run_query =
            RunQuery::new(job_service, "SELECT 1".to_string()).with_project_id("my-project");
        assert_eq!(run_query.project_id.unwrap(), "my-project");
    }

    #[tokio::test]
    async fn test_run_missing_project_id() {
        let job_service = create_job_service();
        let run_query = RunQuery::new(job_service, "SELECT 1".to_string());
        let res = run_query.run().await;
        assert!(matches!(res, Err(QueryError::MissingProjectId)));
    }

    #[tokio::test]
    #[should_panic(expected = "jobs.insert query execution not yet implemented")]
    async fn test_run_panics_jobs_insert() {
        let job_service = create_job_service();
        let run_query = RunQuery::new(job_service, "SELECT 1".to_string())
            .with_project_id("my-project")
            .set_allow_large_results(true);
        let _ = run_query.run().await;
    }

    #[tokio::test]
    #[should_panic(expected = "jobs.query query execution not yet implemented")]
    async fn test_run_panics_jobs_query() {
        let job_service = create_job_service();
        let run_query =
            RunQuery::new(job_service, "SELECT 1".to_string()).with_project_id("my-project");
        let _ = run_query.run().await;
    }

    #[test]
    fn test_force_job_path() {
        let job_service = create_job_service();
        let mut run_query = RunQuery::new(job_service, "SELECT 1".to_string());
        assert!(!run_query.request.force_job_path());

        // Set allow_large_results
        run_query = run_query.set_allow_large_results(true);
        assert!(run_query.request.force_job_path());
    }

    #[test]
    fn test_request_conversions() {
        let req = RunQueryRequest::default()
            .set_query("SELECT 1".to_string())
            .set_dry_run(true)
            .set_use_legacy_sql(true);

        let query_request: google_cloud_bigquery_v2::model::QueryRequest = req.clone().into();
        assert_eq!(query_request.query, "SELECT 1");
        assert!(query_request.dry_run);
        assert_eq!(
            query_request.use_legacy_sql,
            Some(wkt::BoolValue::from(true))
        );

        let job_config: google_cloud_bigquery_v2::model::JobConfiguration = req.into();
        let job_query = job_config.query.as_ref().unwrap();
        assert_eq!(job_query.query, "SELECT 1");
        assert_eq!(job_query.use_legacy_sql, Some(wkt::BoolValue::from(true)));
    }
}
