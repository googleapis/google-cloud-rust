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
/// To configure a `BigQuery` client, use the `with_*` methods on the [`ClientBuilder`][crate::builder::bigquery::ClientBuilder] returned
/// by [`BigQuery::builder()`]. The default configuration uses Application Default Credentials (ADC)
/// and connects to the global default endpoint, which works for most applications.
///
/// Common configuration customizations include:
///
/// - [`with_endpoint()`][crate::builder::bigquery::ClientBuilder::with_endpoint]: Overrides the default API endpoint (`https://bigquery.googleapis.com`). Useful when testing against mock servers or running in restricted network environments (for example, with VPC Service Controls).
/// - [`with_credentials()`][crate::builder::bigquery::ClientBuilder::with_credentials]: Overrides the default Application Default Credentials with explicit or custom authentication credentials.
///
/// # Pooling and Cloning
///
/// `BigQuery` holds an internal gRPC/HTTP client and connection pool wrapped in an [`Arc`].
/// You should create a single `BigQuery` client instance upon application initialization and reuse it across multiple tasks or requests.
/// Cloning a `BigQuery` instance is cheap and does not duplicate underlying connections or thread pools, so you do not need to wrap `BigQuery` in an additional `Arc`.
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

    /// Creates a request builder to configure and execute a SQL query.
    ///
    /// This method returns a [`RunQuery`] builder. You can chain additional configuration methods
    /// (such as setting the project ID, positional or named parameters, query location, and maximum result buffer sizes)
    /// before calling [`RunQuery::run()`].
    ///
    /// The [`RunQuery`] builder automatically decides whether to route your request via the fast path ([`jobs.query`][jobs_query])
    /// or the background job creation path ([`jobs.insert`][jobs_insert]). If the query configuration uses only options supported
    /// by the fast path, the client library uses [`jobs.query`][jobs_query] for lower latency. If advanced options (such as destination
    /// tables or allowing large results) are configured, the client automatically falls back to creating an asynchronous job.
    ///
    /// [jobs_query]: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query
    /// [jobs_insert]: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/insert
    ///
    /// # Example
    ///
    /// ```
    /// # async fn sample() -> anyhow::Result<()> {
    /// use google_cloud_bigquery::client::BigQuery;
    ///
    /// let client = BigQuery::builder().build().await?;
    ///
    /// // Execute a query and read the resulting rows.
    /// let mut rows = client
    ///     .query("SELECT name, count FROM `my-project.my_dataset.stats` LIMIT 50")
    ///     .with_project_id("my-project-id")
    ///     .set_location("US")
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
    /// # Ok(())
    /// # }
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
