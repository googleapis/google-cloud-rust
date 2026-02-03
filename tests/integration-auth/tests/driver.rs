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

#[cfg(all(test, feature = "run-auth-integration-tests"))]
mod driver {
    #[cfg(all(test, feature = "run-byoid-integration-tests"))]
    use test_case::test_case;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[serial_test::serial]
    async fn run_service_account() -> anyhow::Result<()> {
        integration_tests_auth::service_account().await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[serial_test::serial]
    async fn run_service_account_with_audience() -> anyhow::Result<()> {
        integration_tests_auth::service_account_with_audience().await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[serial_test::serial]
    async fn run_impersonated() -> anyhow::Result<()> {
        integration_tests_auth::impersonated().await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[serial_test::serial]
    async fn run_api_key() -> anyhow::Result<()> {
        integration_tests_auth::api_key().await
    }

    #[cfg(all(test, feature = "run-byoid-integration-tests"))]
    #[test_case(false; "without impersonation")]
    #[test_case(true; "with impersonation")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_workload_identity_provider_url_sourced(
        with_impersonation: bool,
    ) -> anyhow::Result<()> {
        integration_tests_auth::workload_identity_provider_url_sourced(with_impersonation).await
    }

    #[cfg(all(test, feature = "run-byoid-integration-tests"))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[serial_test::serial]
    async fn run_workload_identity_provider_executable_sourced_with_impersonation()
    -> anyhow::Result<()> {
        integration_tests_auth::workload_identity_provider_executable_sourced(true).await
    }

    #[cfg(all(test, feature = "run-byoid-integration-tests"))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[serial_test::serial]
    async fn run_workload_identity_provider_executable_sourced_without_impersonation()
    -> anyhow::Result<()> {
        integration_tests_auth::workload_identity_provider_executable_sourced(false).await
    }

    #[cfg(all(test, feature = "run-byoid-integration-tests"))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[serial_test::serial]
    async fn run_workload_identity_provider_programmatic_sourced() -> anyhow::Result<()> {
        integration_tests_auth::workload_identity_provider_programmatic_sourced().await
    }

    #[cfg(all(test, feature = "run-byoid-integration-tests"))]
    #[test_case(false; "without impersonation")]
    #[test_case(true; "with impersonation")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_workload_identity_provider_file_sourced(
        with_impersonation: bool,
    ) -> anyhow::Result<()> {
        integration_tests_auth::workload_identity_provider_file_sourced(with_impersonation).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[serial_test::serial]
    async fn run_mds_id_token() -> anyhow::Result<()> {
        integration_tests_auth::mds_id_token().await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[serial_test::serial]
    async fn run_id_token_adc() -> anyhow::Result<()> {
        let with_impersonation = false;
        integration_tests_auth::id_token_adc(with_impersonation).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[serial_test::serial]
    // verify that include_email via ADC flow is passed down to the impersonated
    // builder and email claim is included in the token.
    async fn run_id_token_adc_impersonated() -> anyhow::Result<()> {
        let with_impersonation = true;
        integration_tests_auth::id_token_adc(with_impersonation).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[serial_test::serial]
    async fn run_id_token_service_account() -> anyhow::Result<()> {
        integration_tests_auth::id_token_service_account().await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[serial_test::serial]
    async fn run_id_token_impersonated() -> anyhow::Result<()> {
        integration_tests_auth::id_token_impersonated().await
    }
}
