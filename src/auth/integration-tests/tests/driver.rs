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
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[serial_test::serial]
    async fn run_service_account() -> anyhow::Result<()> {
        auth_integration_tests::service_account().await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_api_key() -> anyhow::Result<()> {
        auth_integration_tests::api_key().await
    }

    #[cfg(all(test, feature = "run-byoid-integration-tests"))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_workload_identity_provider_url_sourced() -> anyhow::Result<()> {
        auth_integration_tests::workload_identity_provider_url_sourced().await
    }
}
