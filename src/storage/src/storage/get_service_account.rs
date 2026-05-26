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

use super::client::StorageInner;
use crate::Result;
use crate::storage::info::X_GOOG_API_CLIENT_HEADER;
use crate::storage::request_options::RequestOptions;
use gaxi::http::NoBody;
use gaxi::http::reqwest::{HeaderValue, Method, RequestBuilder};
use std::sync::Arc;

/// The request builder for [Storage::get_service_account][crate::client::Storage::get_service_account] calls.
#[derive(Clone, Debug)]
pub struct GetServiceAccount<S>
where
    S: crate::storage::stub::Storage + 'static,
{
    stub: Arc<S>,
    project: String,
    options: RequestOptions,
}

impl<S> GetServiceAccount<S>
where
    S: crate::storage::stub::Storage + 'static,
{
    pub(crate) fn new(stub: Arc<S>, project: String, options: RequestOptions) -> Self {
        let project = project
            .strip_prefix("projects/")
            .unwrap_or(&project)
            .to_string();
        Self {
            stub,
            project,
            options,
        }
    }

    /// The retry policy used for this request.
    pub fn with_retry_policy<V: Into<google_cloud_gax::retry_policy::RetryPolicyArg>>(
        mut self,
        v: V,
    ) -> Self {
        self.options.retry_policy = v.into().into();
        self
    }

    /// The backoff policy used for this request.
    pub fn with_backoff_policy<V: Into<google_cloud_gax::backoff_policy::BackoffPolicyArg>>(
        mut self,
        v: V,
    ) -> Self {
        self.options.backoff_policy = v.into().into();
        self
    }

    /// The retry throttler used for this request.
    pub fn with_retry_throttler<V: Into<google_cloud_gax::retry_throttler::RetryThrottlerArg>>(
        mut self,
        v: V,
    ) -> Self {
        self.options.retry_throttler = v.into().into();
        self
    }

    /// Sets the project that will be billed for this request.
    pub fn with_quota_project(mut self, project: impl Into<String>) -> Self {
        self.options.set_quota_project(project);
        self
    }

    /// Sends the request and returns the GCS service account email address.
    pub async fn send(self) -> Result<String> {
        self.stub.get_service_account(self.project, self.options).await
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ServiceAccountLookup {
    pub inner: Arc<StorageInner>,
    pub project: String,
    pub options: RequestOptions,
}

impl ServiceAccountLookup {
    async fn http_request_builder(&self) -> Result<RequestBuilder> {
        let project_id = super::client::enc(&self.project);
        let builder = self
            .inner
            .client
            .builder(
                Method::GET,
                format!("/storage/v1/projects/{project_id}/serviceAccount"),
            )
            .header(
                "x-goog-api-client",
                HeaderValue::from_static(&X_GOOG_API_CLIENT_HEADER),
            );

        Ok(builder)
    }

    pub(crate) async fn response(self) -> Result<String> {
        let builder = self.http_request_builder().await?;
        let response = self
            .inner
            .client
            .execute::<NoBody, crate::storage::v1::ProjectServiceAccount>(
                builder,
                None,
                self.options.gax().clone(),
            )
            .await?;
        Ok(response.into_body().email_address)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::Storage;
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
    use httptest::{
        Expectation,
        Server,
        matchers::*,
        responders::*,
    };

    #[tokio::test]
    async fn test_get_service_account_success() -> anyhow::Result<()> {
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::method_path("GET", "/storage/v1/projects/my-project-123/serviceAccount"),
                request::headers(contains(("x-goog-api-client", super::super::info::X_GOOG_API_CLIENT_HEADER.as_str()))),
            ])
            .respond_with(
                status_code(200).body(serde_json::json!({
                    "kind": "storage#serviceAccount",
                    "email_address": "service-123456@gs-project-accounts.iam.gserviceaccount.com",
                    "extra_field": "test-value"
                }).to_string()),
            ),
        );

        let client = Storage::builder()
            .with_endpoint(format!("http://{}", server.addr()))
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        let email = client.get_service_account("my-project-123").send().await?;
        assert_eq!(email, "service-123456@gs-project-accounts.iam.gserviceaccount.com");

        Ok(())
    }

    #[tokio::test]
    async fn test_get_service_account_with_projects_prefix() -> anyhow::Result<()> {
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::method_path("GET", "/storage/v1/projects/my-project-123/serviceAccount"),
            ])
            .respond_with(
                status_code(200).body(serde_json::json!({
                    "kind": "storage#serviceAccount",
                    "email_address": "service-123456@gs-project-accounts.iam.gserviceaccount.com"
                }).to_string()),
            ),
        );

        let client = Storage::builder()
            .with_endpoint(format!("http://{}", server.addr()))
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        let email = client.get_service_account("projects/my-project-123").send().await?;
        assert_eq!(email, "service-123456@gs-project-accounts.iam.gserviceaccount.com");

        Ok(())
    }

    #[tokio::test]
    async fn test_get_service_account_not_found() -> anyhow::Result<()> {
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::method_path("GET", "/storage/v1/projects/non-existent/serviceAccount"),
            ])
            .respond_with(status_code(404).body("Not Found")),
        );

        let client = Storage::builder()
            .with_endpoint(format!("http://{}", server.addr()))
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        let err = client.get_service_account("non-existent").send().await.expect_err("should fail with 404");
        assert_eq!(err.http_status_code(), Some(404));

        Ok(())
    }

    #[tokio::test]
    async fn test_get_service_account_with_domain_prefix() -> anyhow::Result<()> {
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::method_path("GET", "/storage/v1/projects/example.com%3Amy-project-123/serviceAccount"),
                request::headers(contains(("x-goog-api-client", super::X_GOOG_API_CLIENT_HEADER.as_str()))),
            ])
            .respond_with(
                status_code(200).body(serde_json::json!({
                    "kind": "storage#serviceAccount",
                    "email_address": "service-123456@gs-project-accounts.iam.gserviceaccount.com"
                }).to_string()),
            ),
        );

        let client = Storage::builder()
            .with_endpoint(format!("http://{}", server.addr()))
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        let email = client.get_service_account("example.com:my-project-123").send().await?;
        assert_eq!(email, "service-123456@gs-project-accounts.iam.gserviceaccount.com");

        Ok(())
    }
}

