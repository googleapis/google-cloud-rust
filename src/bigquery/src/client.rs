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

use crate::ClientBuilderResult as BuilderResult;
use crate::client_builder::ClientBuilder;
use crate::query::RunQuery;
use google_cloud_bigquery_v2::client::JobService;
use std::sync::Arc;

/// A high-level BigQuery client for executing queries and managing jobs.
#[derive(Clone, Debug)]
pub struct BigQuery {
    #[allow(dead_code)]
    pub(crate) job_service: Arc<JobService>,
}

impl BigQuery {
    /// Convenient entrypoint to return a fresh configuration builder.
    pub fn builder() -> ClientBuilder {
        ClientBuilder::new()
    }

    pub(crate) async fn new(builder: ClientBuilder) -> BuilderResult<Self> {
        let mut job_service_builder = JobService::builder();
        if let Some(creds) = builder.config.cred {
            job_service_builder = job_service_builder.with_credentials(creds);
        }
        if let Some(endpoint) = builder.config.endpoint {
            job_service_builder = job_service_builder.with_endpoint(endpoint);
        }
        if let Some(universe_domain) = builder.config.universe_domain {
            job_service_builder = job_service_builder.with_universe_domain(universe_domain);
        }
        if builder.config.tracing {
            job_service_builder = job_service_builder.with_tracing();
        }
        if let Some(retry_policy) = builder.config.retry_policy {
            job_service_builder = job_service_builder.with_retry_policy(retry_policy);
        }
        if let Some(backoff_policy) = builder.config.backoff_policy {
            job_service_builder = job_service_builder.with_backoff_policy(backoff_policy);
        }
        job_service_builder =
            job_service_builder.with_retry_throttler(builder.config.retry_throttler);
        let job_service = Arc::new(job_service_builder.build().await?);

        Ok(BigQuery { job_service })
    }

    /// Prepares a SQL query execution by returning a unified `RunQuery` request builder.
    /// This builder internally routes to either `jobs.query` (fast path) or `jobs.insert` (job path)
    /// depending on the fields configured.
    pub fn query<S: Into<String>>(&self, sql: S) -> RunQuery {
        RunQuery::new(self.job_service.clone(), sql.into())
    }
}

#[cfg(test)]
mod tests {
    use super::BigQuery;
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;

    #[tokio::test]
    async fn test_bigquery_builder() -> anyhow::Result<()> {
        let _client = BigQuery::builder()
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;
        Ok(())
    }
}
