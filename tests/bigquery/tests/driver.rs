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

#[cfg(all(test, feature = "run-integration-tests"))]
mod bigquery {
    use google_cloud_test_utils::errors::anydump;
    use google_cloud_test_utils::tracing::enable_tracing;

    #[tokio::test]
    async fn run_dataset_service() -> anyhow::Result<()> {
        let _guard = enable_tracing();
        integration_tests_bigquery::dataset_admin()
            .await
            .inspect_err(anydump)
    }

    #[tokio::test]
    async fn run_job_service() -> anyhow::Result<()> {
        let _guard = enable_tracing();
        integration_tests_bigquery::job_service()
            .await
            .inspect_err(anydump)
    }

    #[tokio::test]
    async fn run_query_client() -> anyhow::Result<()> {
        let _guard = enable_tracing();
        integration_tests_bigquery::query_client()
            .await
            .inspect_err(anydump)
    }

    #[tokio::test]
    async fn run_query_client_datatypes() -> anyhow::Result<()> {
        let _guard = enable_tracing();
        integration_tests_bigquery::query_client_datatypes()
            .await
            .inspect_err(anydump)
    }

    #[tokio::test]
    async fn run_query_client_numeric_limits() -> anyhow::Result<()> {
        let _guard = enable_tracing();
        integration_tests_bigquery::query_client_numeric_limits()
            .await
            .inspect_err(anydump)
    }

    #[tokio::test]
    async fn run_query_client_multi_page() -> anyhow::Result<()> {
        let _guard = enable_tracing();
        integration_tests_bigquery::query_client_multi_page()
            .await
            .inspect_err(anydump)
    }

    #[tokio::test]
    async fn run_query_client_job() -> anyhow::Result<()> {
        let _guard = enable_tracing();
        integration_tests_bigquery::query_client_job()
            .await
            .inspect_err(anydump)
    }

    #[tokio::test]
    async fn run_query_client_nested_types() -> anyhow::Result<()> {
        let _guard = enable_tracing();
        integration_tests_bigquery::query_client_nested_types()
            .await
            .inspect_err(anydump)
    }
}
