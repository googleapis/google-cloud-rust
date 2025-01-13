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
    use gax::error::*;
    use gax::options::ClientConfig as Config;
    use test_case::test_case;

    fn report(e: Error) -> Error {
        println!("\nERROR {e}\n");
        Error::other("test failed")
    }

    fn retry_policy() -> impl gax::retry_policy::RetryPolicy {
        use gax::retry_policy::RetryPolicyExt;
        use std::time::Duration;
        gax::retry_policy::AlwaysRetry
            .with_time_limit(Duration::from_secs(15))
            .with_attempt_limit(5)
    }

    #[test_case(None; "default")]
    #[test_case(Some(Config::new().enable_tracing()); "with tracing enabled")]
    #[test_case(Some(Config::new().set_retry_policy(retry_policy())); "with retry enabled")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_secretmanager_protobuf(config: Option<Config>) -> integration_tests::Result<()> {
        integration_tests::secret_manager::protobuf::run(config)
            .await
            .map_err(report)
    }

    #[test_case(None; "default")]
    #[test_case(Some(Config::new().enable_tracing()); "with tracing enabled")]
    #[test_case(Some(Config::new().set_retry_policy(retry_policy())); "with retry enabled")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_secretmanager_openapi(config: Option<Config>) -> integration_tests::Result<()> {
        integration_tests::secret_manager::openapi::run(config)
            .await
            .map_err(report)
    }

    #[test_case(None; "default")]
    #[test_case(Some(Config::new().enable_tracing()); "with tracing enabled")]
    #[test_case(Some(Config::new().set_retry_policy(retry_policy())); "with retry enabled")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_secretmanager_openapi_locational(
        config: Option<Config>,
    ) -> integration_tests::Result<()> {
        integration_tests::secret_manager::openapi_locational::run(config)
            .await
            .map_err(report)
    }
}
