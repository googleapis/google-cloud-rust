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
    use google_cloud_test_utils::errors::anydump;

    #[tokio::test(flavor = "multi_thread")]
    async fn authentication() -> anyhow::Result<()> {
        user_guide_samples::authentication::drive_adc()
            .await
            .inspect_err(anydump)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn id_token() -> anyhow::Result<()> {
        user_guide_samples::authentication::drive_id_token()
            .await
            .inspect_err(anydump)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn endpoint() -> anyhow::Result<()> {
        let project_id = std::env::var("GOOGLE_CLOUD_PROJECT").unwrap();
        let region = std::env::var("GOOGLE_CLOUD_TEST_REGION").unwrap_or("us-central1".to_string());
        user_guide_samples::endpoint::default::sample(&project_id)
            .await
            .inspect_err(anydump)?;
        user_guide_samples::endpoint::regional::sample(&project_id, &region)
            .await
            .inspect_err(anydump)?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn gemini_text_prompt() -> anyhow::Result<()> {
        let project_id = std::env::var("GOOGLE_CLOUD_PROJECT").unwrap();
        user_guide_samples::gemini::text_prompt::sample(&project_id)
            .await
            .inspect_err(anydump)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn gemini_prompt_and_image() -> anyhow::Result<()> {
        let project_id = std::env::var("GOOGLE_CLOUD_PROJECT").unwrap();
        user_guide_samples::gemini::prompt_and_image::sample(&project_id)
            .await
            .inspect_err(anydump)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn logging() -> anyhow::Result<()> {
        let project_id = std::env::var("GOOGLE_CLOUD_PROJECT").unwrap();
        user_guide_samples::logging::sample(&project_id)
            .await
            .inspect_err(anydump)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pagination_iterate_pages() -> anyhow::Result<()> {
        let project_id = std::env::var("GOOGLE_CLOUD_PROJECT").unwrap();
        user_guide_samples::pagination::paginator_iterate_pages::sample(&project_id)
            .await
            .inspect_err(anydump)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pagination_iterate_items() -> anyhow::Result<()> {
        let project_id = std::env::var("GOOGLE_CLOUD_PROJECT").unwrap();
        user_guide_samples::pagination::paginator_iterate_items::sample(&project_id)
            .await
            .inspect_err(anydump)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pagination_stream_pages() -> anyhow::Result<()> {
        let project_id = std::env::var("GOOGLE_CLOUD_PROJECT").unwrap();
        user_guide_samples::pagination::paginator_stream_pages::sample(&project_id)
            .await
            .inspect_err(anydump)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pagination_stream_items() -> anyhow::Result<()> {
        let project_id = std::env::var("GOOGLE_CLOUD_PROJECT").unwrap();
        user_guide_samples::pagination::paginator_stream_items::sample(&project_id)
            .await
            .inspect_err(anydump)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pagination_page_token() -> anyhow::Result<()> {
        let project_id = std::env::var("GOOGLE_CLOUD_PROJECT").unwrap();
        user_guide_samples::pagination::pagination_page_token::sample(&project_id)
            .await
            .inspect_err(anydump)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn retry_policies_client() -> anyhow::Result<()> {
        let project_id = std::env::var("GOOGLE_CLOUD_PROJECT").unwrap();
        user_guide_samples::retry_policies::client_retry::sample(&project_id)
            .await
            .inspect_err(anydump)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn retry_policies_client_full() -> anyhow::Result<()> {
        let project_id = std::env::var("GOOGLE_CLOUD_PROJECT").unwrap();
        user_guide_samples::retry_policies::client_retry_full::sample(&project_id)
            .await
            .inspect_err(anydump)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn retry_policies_request() -> anyhow::Result<()> {
        user_guide_samples::retry_policies::drive_request_retry()
            .await
            .inspect_err(anydump)
    }

    #[tokio::test]
    async fn error_handling_found() -> anyhow::Result<()> {
        user_guide_samples::error_handling::drive_update_secret()
            .await
            .inspect_err(anydump)
    }

    #[tokio::test]
    async fn error_handling_not_found() -> anyhow::Result<()> {
        user_guide_samples::error_handling::drive_update_secret_not_found()
            .await
            .inspect_err(anydump)
    }

    #[tokio::test]
    async fn examine_error_details() -> anyhow::Result<()> {
        user_guide_samples::examine_error_details::sample()
            .await
            .inspect_err(anydump)
    }

    #[tokio::test]
    async fn binding_fail() -> anyhow::Result<()> {
        user_guide_samples::binding_errors::binding_fail::sample()
            .await
            .inspect_err(anydump)
    }

    #[tokio::test]
    async fn binding_success() -> anyhow::Result<()> {
        user_guide_samples::binding_errors::binding_success::sample()
            .await
            .inspect_err(anydump)
    }
}
