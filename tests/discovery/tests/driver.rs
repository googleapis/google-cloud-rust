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
mod compute {
    use google_cloud_test_utils::tracing::enable_tracing;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_compute_zones() -> anyhow::Result<()> {
        let _guard = enable_tracing();
        integration_tests_discovery::zones().await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_compute_errors() -> anyhow::Result<()> {
        let _guard = enable_tracing();
        integration_tests_discovery::errors().await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_compute_lro_errors() -> anyhow::Result<()> {
        let _guard = enable_tracing();
        integration_tests_discovery::lro_errors().await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_compute_machine_types() -> anyhow::Result<()> {
        let _guard = enable_tracing();
        integration_tests_discovery::machine_types().await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_compute_images() -> anyhow::Result<()> {
        let _guard = enable_tracing();
        integration_tests_discovery::images().await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_compute_instances() -> anyhow::Result<()> {
        let _guard = enable_tracing();
        integration_tests_discovery::instances().await
    }

    #[ignore = "TODO(#3691) - disabled because it was flaky"]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_compute_region_instances() -> anyhow::Result<()> {
        let _guard = enable_tracing();
        integration_tests_discovery::region_instances().await
    }
}
