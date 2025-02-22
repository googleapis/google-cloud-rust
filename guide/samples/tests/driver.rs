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

#[cfg(all(test, feature = "run-integration-tests"))]
mod driver {
    #[tokio::test(flavor = "multi_thread")]
    async fn lro_start() -> user_guide_samples::Result<()> {
        let project_id = std::env::var("GOOGLE_CLOUD_PROJECT").unwrap();
        user_guide_samples::lro::start(&project_id).await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn lro_automatic() -> user_guide_samples::Result<()> {
        let project_id = std::env::var("GOOGLE_CLOUD_PROJECT").unwrap();
        user_guide_samples::lro::automatic(&project_id).await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn lro_polling() -> user_guide_samples::Result<()> {
        let project_id = std::env::var("GOOGLE_CLOUD_PROJECT").unwrap();
        user_guide_samples::lro::polling(&project_id).await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn polling_policies_client_backoff() -> user_guide_samples::Result<()> {
        let project_id = std::env::var("GOOGLE_CLOUD_PROJECT").unwrap();
        user_guide_samples::polling_policies::client_backoff(&project_id).await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn polling_policies_rpc_backoff() -> user_guide_samples::Result<()> {
        let project_id = std::env::var("GOOGLE_CLOUD_PROJECT").unwrap();
        user_guide_samples::polling_policies::rpc_backoff(&project_id).await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn polling_policies_client_errors() -> user_guide_samples::Result<()> {
        let project_id = std::env::var("GOOGLE_CLOUD_PROJECT").unwrap();
        user_guide_samples::polling_policies::client_errors(&project_id).await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn polling_policies_rpc_errors() -> user_guide_samples::Result<()> {
        let project_id = std::env::var("GOOGLE_CLOUD_PROJECT").unwrap();
        user_guide_samples::polling_policies::rpc_errors(&project_id).await
    }
}
