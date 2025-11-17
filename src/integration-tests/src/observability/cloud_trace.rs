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

use auth::credentials::{CacheableResource, Credentials, EntityTag};
use http::Extensions;
use std::time::Duration;

/// A client for the Google Cloud Trace API (v1 REST).
///
/// It uses the v1 rest API for simplicity since there is no generated client that can get traces.
pub struct CloudTraceClient {
    project_id: String,
    http_client: reqwest::Client,
    credentials: Credentials,
    endpoint: String,
}

/// Builder for `CloudTraceClient`.
pub struct CloudTraceClientBuilder {
    project_id: String,
    credentials: Option<Credentials>,
    endpoint: String,
}

impl CloudTraceClientBuilder {
    pub fn new(project_id: impl Into<String>) -> Self {
        Self {
            project_id: project_id.into(),
            credentials: None,
            endpoint: "https://cloudtrace.googleapis.com".to_string(),
        }
    }

    /// Sets the credentials used for authentication.
    /// If not provided, Application Default Credentials (ADC) will be loaded.
    pub fn with_credentials(mut self, credentials: Credentials) -> Self {
        self.credentials = Some(credentials);
        self
    }

    /// Sets the endpoint for the Cloud Trace API.
    /// Defaults to `https://cloudtrace.googleapis.com`.
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = endpoint.into();
        self
    }

    pub async fn build(self) -> anyhow::Result<CloudTraceClient> {
        let credentials = match self.credentials {
            Some(c) => c,
            None => auth::credentials::Builder::default().build()?,
        };

        Ok(CloudTraceClient {
            project_id: self.project_id,
            http_client: reqwest::Client::new(),
            credentials,
            endpoint: self.endpoint,
        })
    }
}

impl CloudTraceClient {
    /// Creates a builder for `CloudTraceClient`.
    pub fn builder(project_id: impl Into<String>) -> CloudTraceClientBuilder {
        CloudTraceClientBuilder::new(project_id)
    }

    /// Polls the Cloud Trace API for a specific trace ID.
    ///
    /// This method will retry up to `max_retries` times, waiting `interval`
    /// between attempts.
    pub async fn get_trace(
        &self,
        trace_id: &str,
        max_retries: usize,
        interval: Duration,
    ) -> anyhow::Result<serde_json::Value> {
        let url = format!(
            "{}/v1/projects/{}/traces/{}",
            self.endpoint, self.project_id, trace_id
        );

        let mut cached_token: Option<String> = None;
        let mut last_etag: Option<EntityTag> = None;

        for i in 1..=max_retries {
            let token = self.get_token(&mut cached_token, &mut last_etag).await?;

            let resp = self
                .http_client
                .get(&url)
                .header("Authorization", token)
                .send()
                .await?;

            let status = resp.status();
            if status.is_success() {
                let json: serde_json::Value = resp.json().await?;
                return Ok(json);
            } else if status == reqwest::StatusCode::NOT_FOUND {
                tracing::info!(
                    "Trace {} not found yet (attempt {}/{})",
                    trace_id,
                    i,
                    max_retries
                );
            } else {
                let text = resp.text().await?;
                return Err(anyhow::anyhow!(
                    "Unexpected status {} from Cloud Trace API: {}",
                    status,
                    text
                ));
            }

            tokio::time::sleep(interval).await;
        }

        Err(anyhow::anyhow!(
            "Timed out waiting for trace {} after {} attempts",
            trace_id,
            max_retries
        ))
    }

    async fn get_token(
        &self,
        cached_token: &mut Option<String>,
        last_etag: &mut Option<EntityTag>,
    ) -> anyhow::Result<String> {
        let mut extensions = Extensions::new();
        if let Some(etag) = last_etag.clone() {
            extensions.insert(etag);
        }

        match self.credentials.headers(extensions).await? {
            CacheableResource::New { data, entity_tag } => {
                let token_str = data
                    .get("authorization")
                    .ok_or_else(|| anyhow::anyhow!("Missing authorization header"))?
                    .to_str()?
                    .to_string();
                *last_etag = Some(entity_tag);
                *cached_token = Some(token_str.clone());
                Ok(token_str)
            }
            CacheableResource::NotModified => cached_token
                .clone()
                .ok_or_else(|| anyhow::anyhow!("Cache hit but no token stored")),
        }
    }
}