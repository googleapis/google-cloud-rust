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

//! Contains the BigQuery client and related types.

use tokio::time::Duration;

/// Implements a Query client for the BigQuery API.
///
/// # Example
/// ```
/// # tokio_test::block_on(async {
/// # use google_cloud_bigquery::client::QueryClient;
/// let client = QueryClient::builder().build().await?;
/// // use `client` to run queries to BigQuery.
/// # gax::Result::<()>::Ok(()) });
/// ```
///
/// [with_endpoint()]: super::builder::bigquery::ClientBuilder::with_endpoint
/// [with_credentials()]: super::builder::bigquery::ClientBuilder::with_credentials
/// [Private Google Access with VPC Service Controls]: https://cloud.google.com/vpc-service-controls/docs/private-connectivity
/// [Application Default Credentials]: https://cloud.google.com/docs/authentication#adc
#[derive(Clone, Debug)]
pub struct QueryClient {
    job_service: bigquery_v2::client::JobService,
}

impl QueryClient {
    /// Returns a builder for [QueryClient].
    ///
    /// ```no_run
    /// # tokio_test::block_on(async {
    /// # use google_cloud_bigquery::client::QueryClient;
    /// let client = QueryClient::builder().build().await?;
    /// # gax::Result::<()>::Ok(()) });
    /// ```
    pub fn builder() -> ClientBuilder {
        gax::client_builder::internal::new_builder(client_builder::Factory)
    }

    /// Start a QueryRequest and returns a QueryResult handler to monitor progress.
    ///
    /// # Parameters
    /// * `sql` - Query to run.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_bigquery::client::QueryClient;
    /// async fn example(client: &QueryClient) -> gax::Result<()> {
    ///     client.start_query("SELECT 17 as foo").send().await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn start_query(&self, project_id: &str, sql: String) -> gax::Result<QueryResult> {
        let res = self
            .job_service
            .query(project_id)
            .set_query_request(bigquery_v2::model::QueryRequest::new().set_query(sql))
            .send()
            .await?;

        let mut query_result: QueryResult<'_> = QueryResult {
            inner: self,
            project_id: project_id.to_owned(),
            query_id: None,
            page_token: String::default(),
            job_id: String::default(),
            location: String::default(),
            completed: false,
            cached_rows: Vec::new(),
        };
        if let Some(job_complete) = res.job_complete {
            if job_complete {
                if let Some(job_ref) = res.job_reference {
                    query_result.job_id = job_ref.job_id.to_owned();
                    if let Some(location) = job_ref.location {
                        query_result.location = location;
                    }
                }
                query_result.completed = job_complete;
            }
            query_result.page_token = res.page_token;
            query_result.cached_rows = res.rows;
        }

        Ok(query_result)
    }

    pub(crate) async fn new(_config: gaxi::options::ClientConfig) -> gax::Result<Self> {
        let job_service = bigquery_v2::client::JobService::builder().build().await?;
        Ok(Self { job_service })
    }
}

pub struct QueryResult<'a> {
    pub(crate) inner: &'a QueryClient,
    pub project_id: String,
    pub location: String,
    pub page_token: String,
    pub job_id: String,
    pub query_id: Option<String>,
    pub completed: bool,
    pub cached_rows: std::vec::Vec<wkt::Struct>,
}

impl QueryResult<'_> {
    /*pub async fn paginator(&mut self) -> impl gax::paginator::Paginator<wkt::Struct, gax::error::Error> {
        let token = self.page_token.clone();
        let execute = move |token: String| {
            let res = self.get_query_results()
                .set_page_token(self.page_token)
                .send()
                .await?
            res.rows
        };
        gax::paginator::internal::new_paginator(token, execute);
    }*/

    pub async fn wait(&mut self) -> gax::Result<()> {
        if self.completed {
            return Ok(());
        }
        let sleep = async |d| tokio::time::sleep(d).await;
        loop {
            let complete = self.poll_job().await?;
            if complete {
                return Ok(());
            }
            sleep(Duration::from_millis(300)).await;
        }
    }

    pub(crate) fn get_query_results(&self) -> bigquery_v2::builder::job_service::GetQueryResults {
        self.inner
            .job_service
            .get_query_results(self.project_id.to_owned().as_str(), self.job_id.as_str())
    }

    pub(crate) async fn poll_job(&mut self) -> gax::Result<bool> {
        let res = self.get_query_results().set_max_results(0).send().await?;

        if let Some(job_complete) = res.job_complete {
            if job_complete {
                self.completed = job_complete;
                return Ok(job_complete);
            }
        }
        return Ok(false);
    }
}

/// A builder for [QueryClient].
///
/// ```
/// # tokio_test::block_on(async {
/// # use google_cloud_bigquery::*;
/// # use client::ClientBuilder;
/// # use client::QueryClient;
/// let builder : ClientBuilder = Storage::builder();
/// let client = builder
///     .with_endpoint("https://storage.googleapis.com")
///     .build().await?;
/// # gax::Result::<()>::Ok(()) });
/// ```
pub type ClientBuilder =
    gax::client_builder::ClientBuilder<client_builder::Factory, gaxi::options::Credentials>;

pub(crate) mod client_builder {
    use super::QueryClient;
    pub struct Factory;
    impl gax::client_builder::internal::ClientFactory for Factory {
        type Client = QueryClient;
        type Credentials = gaxi::options::Credentials;
        async fn build(self, config: gaxi::options::ClientConfig) -> gax::Result<Self::Client> {
            Self::Client::new(config).await
        }
    }
}
