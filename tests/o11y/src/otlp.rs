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

pub mod metrics;
pub mod trace;

const GCP_OTLP_ENDPOINT: &str = "https://telemetry.googleapis.com";
const OTEL_KEY_GCP_PROJECT_ID: &str = "gcp.project_id";
const OTEL_KEY_SERVICE_NAME: &str = "service.name";

#[cfg(test)]
mod tests {
    use google_cloud_auth::credentials::{CacheableResource, CredentialsProvider, EntityTag};
    use google_cloud_auth::errors::CredentialsError;

    /// A test credentials provider that returns static, known values.
    ///
    /// This provider is used to verify that the authentication interceptor correctly
    /// retrieves and injects credentials. It returns a fixed "Bearer test-token"
    /// authorization header and "test-project" project header.
    #[derive(Debug)]
    pub struct TestTokenProvider;
    impl CredentialsProvider for TestTokenProvider {
        async fn headers(
            &self,
            _: http::Extensions,
        ) -> Result<CacheableResource<http::HeaderMap>, CredentialsError> {
            let mut map = http::HeaderMap::new();
            map.insert("authorization", "Bearer test-token".parse().unwrap());
            map.insert("x-goog-user-project", "test-project".parse().unwrap());
            Ok(CacheableResource::New {
                entity_tag: EntityTag::new(),
                data: map,
            })
        }
        async fn universe_domain(&self) -> Option<String> {
            None
        }
    }
}
