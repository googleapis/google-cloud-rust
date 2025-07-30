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

use std::sync::Arc;

use crate::Result;
use crate::query::Query;
use crate::query_request::IntoPostQueryRequest;
use bigquery_v2::client::JobService;
use bigquery_v2::model::PostQueryRequest;

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
    job_service: Arc<JobService>,
    project_id: String,
}

const DEFAULT_HOST: &str = "https://bigquery.googleapis.com";

impl QueryClient {
    /// Returns a builder for [QueryClient].
    ///
    /// # Example
    /// ```
    /// # use google_cloud_bigquery::client::QueryClient;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let client = QueryClient::builder().build().await?;
    /// # Ok(()) }
    /// ```
    pub fn builder() -> ClientBuilder {
        ClientBuilder::new()
    }

    /// Start a QueryRequest and returns a Query handle to monitor progress.
    pub async fn query<T>(&self, req: T) -> Result<Query>
    where
        T: IntoPostQueryRequest,
    {
        let mut req: PostQueryRequest = req.into_post_query_request();
        if req.project_id.is_empty() {
            req.project_id = self.project_id.clone();
        }
        let res = self
            .job_service
            .query()
            .set_project_id(req.project_id)
            .set_or_clear_query_request(req.query_request)
            .send()
            .await?;
        Ok(Query::new(self.job_service.clone(), res))
    }

    pub(crate) async fn new(builder: ClientBuilder) -> gax::client_builder::Result<Self> {
        let job_service = bigquery_v2::client::JobService::builder()
            //.with_credentials(builder.credentials)
            .with_endpoint(builder.endpoint.unwrap_or(DEFAULT_HOST.to_string()))
            .build()
            .await?;
        Ok(Self {
            job_service: Arc::new(job_service),
            project_id: builder.project_id.unwrap_or_default(),
        })
    }
}

/// A builder for [QueryClient].
///
/// ```
/// # use google_cloud_bigquery::client::QueryClient;
/// # async fn sample() -> anyhow::Result<()> {
/// let builder = QueryClient::builder();
/// let client = builder
///     .with_endpoint("https://bigquery.googleapis.com")
///     .build()
///     .await?;
/// # Ok(()) }
/// ```
pub struct ClientBuilder {
    pub(crate) endpoint: Option<String>,
    pub(crate) project_id: Option<String>,
    pub(crate) credentials: Option<auth::credentials::Credentials>,
}

impl ClientBuilder {
    pub(crate) fn new() -> Self {
        Self {
            endpoint: None,
            project_id: None,
            credentials: None,
        }
    }

    /// Creates a new client.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_bigquery::client::QueryClient;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let client = QueryClient::builder().build().await?;
    /// # Ok(()) }
    /// ```
    pub async fn build(self) -> gax::client_builder::Result<QueryClient> {
        QueryClient::new(self).await
    }

    /// Sets the endpoint.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_bigquery::client::QueryClient;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let client = QueryClient::builder()
    ///     .with_endpoint("https://private.googleapis.com")
    ///     .build()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn with_endpoint<V: Into<String>>(mut self, v: V) -> Self {
        self.endpoint = Some(v.into());
        self
    }

    /// Sets the project id.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_bigquery::client::QueryClient;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let client = QueryClient::builder()
    ///     .with_project_id("my-project-id")
    ///     .build()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn with_project_id<V: Into<String>>(mut self, v: V) -> Self {
        self.project_id = Some(v.into());
        self
    }

    /// Configures the authentication credentials.
    ///
    /// Google Cloud BigQuery requires authentication. Use this
    /// method to change the credentials used by the client. More information
    /// about valid credentials types can be found in the [google-cloud-auth]
    /// crate documentation.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_bigquery::client::QueryClient;
    /// # async fn sample() -> anyhow::Result<()> {
    /// use auth::credentials::mds;
    /// let client = QueryClient::builder()
    ///     .with_credentials(
    ///         mds::Builder::default()
    ///             .with_scopes(["https://www.googleapis.com/auth/cloud-platform.read-only"])
    ///             .build()?)
    ///     .build()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    ///
    /// [google-cloud-auth]: https://docs.rs/google-cloud-auth
    pub fn with_credentials<V: Into<auth::credentials::Credentials>>(mut self, v: V) -> Self {
        self.credentials = Some(v.into());
        self
    }
}
