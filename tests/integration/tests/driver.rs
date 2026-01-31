// Copyright 2024 Google LLC
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
    use google_cloud_test_utils::tracing::enable_tracing;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_bigquery_dataset_service() -> integration_tests::Result<()> {
        let _guard = enable_tracing();
        integration_tests::bigquery::dataset_admin()
            .await
            .map_err(integration_tests::report_error)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_bigquery_job_service() -> integration_tests::Result<()> {
        let _guard = enable_tracing();
        integration_tests::bigquery::job_service()
            .await
            .map_err(integration_tests::report_error)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_firestore() -> integration_tests::Result<()> {
        let _guard = enable_tracing();
        integration_tests::firestore::basic()
            .await
            .map_err(integration_tests::report_error)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_error_details_http() -> integration_tests::Result<()> {
        let _guard = enable_tracing();
        integration_tests::error_details::error_details_http()
            .await
            .map_err(integration_tests::report_error)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_error_details_grpc() -> integration_tests::Result<()> {
        let _guard = enable_tracing();
        integration_tests::error_details::error_details_grpc()
            .await
            .map_err(integration_tests::report_error)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_check_code_for_http() -> integration_tests::Result<()> {
        let _guard = enable_tracing();
        integration_tests::error_details::check_code_for_http()
            .await
            .map_err(integration_tests::report_error)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_check_code_for_grpc() -> integration_tests::Result<()> {
        let _guard = enable_tracing();
        integration_tests::error_details::check_code_for_grpc()
            .await
            .map_err(integration_tests::report_error)
    }
}
