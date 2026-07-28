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
///
/// # Configuration
///
/// To construct a `BigQuery` client with custom configuration—such as non-default credentials, specific API endpoints,
/// or universe domain settings—use [`BigQuery::builder()`] to obtain a [`ClientBuilder`].
///
/// # Pooling and Cloning
///
/// A `BigQuery` instance wraps an internal REST service stub behind an atomic reference counted pointer ([`Arc`](std::sync::Arc)).
/// Because the underlying connection pools and authorization state are maintained within this shared stub, **cloning a `BigQuery` client is cheap**.
///
/// You do not need to wrap `BigQuery` in an additional `Arc` when passing it across threads or sharing it across asynchronous Tokio tasks.
///
/// # Example: Basic Setup and Query Execution
///
/// ```
/// # use google_cloud_bigquery::client::BigQuery;
/// # async fn sample() -> anyhow::Result<()> {
/// let client = BigQuery::builder().build().await?;
/// let mut rows = client
///     .query("SELECT name, count FROM `bigquery-public-data.usa_names.usa_1910_2013` WHERE state = 'WA' ORDER BY count DESC LIMIT 5")
///     .with_project_id("my-project-id")
///     .run()
///     .await?
///     .until_done()
///     .await?
///     .read();
///
/// while let Some(row) = rows.next().await.transpose()? {
///     let name: String = row.get("name");
///     let count: i64 = row.get("count");
///     println!("{name}: {count}");
/// }
/// # Ok(()) }
/// ```
#[derive(Clone, Debug)]
pub struct BigQuery {
    job_service: Arc<JobService>,
}

impl BigQuery {
    /// Returns a new [`ClientBuilder`] for configuring and instantiating a [`BigQuery`] client.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_bigquery::client::BigQuery;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let client = BigQuery::builder()
    ///     .with_endpoint("https://bigquery.googleapis.com")
    ///     .build()
    ///     .await?;
    /// # Ok(()) }
    /// ```
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

    /// Creates a [`RunQuery`] request builder to configure and execute a SQL query.
    ///
    /// When you invoke `.run()` on the returned builder, the client automatically inspects your request configuration
    /// to determine the most efficient execution path:
    ///
    /// - **Fast Query Path (`jobs.query`)**: If the query executes standard SQL with basic options, the client sends a synchronous
    ///   `jobs.query` request. If the query runs fast enough, the initial result rows are returned in the response.
    /// - **Job Path (`jobs.insert`)**: If you configure execution options—such as custom destination tables, dry runs,
    ///   or legacy SQL syntax—the client automatically routes to `jobs.insert` to create a Query Job.
    ///
    /// In either case, execution returns a consistent handle that can be polled and read uniformly.
    ///
    /// # Example: Executing a Query
    ///
    /// ```
    /// # use google_cloud_bigquery::client::BigQuery;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let client = BigQuery::builder().build().await?;
    /// let query_handle = client
    ///     .query("SELECT 1 AS num")
    ///     .with_project_id("my-project-id")
    ///     .run()
    ///     .await?;
    /// # Ok(()) }
    /// ```
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
