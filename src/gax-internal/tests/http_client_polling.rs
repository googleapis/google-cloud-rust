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

#[cfg(all(test, feature = "_internal_http_client"))]
mod test {
    type TestResult = Result<(), Box<dyn std::error::Error>>;

    /// A test policy, the only interesting bit is the name, which is included
    /// in debug messages and used in the tests.
    #[derive(Debug)]
    struct TestErrorPolicy {
        pub _name: String,
    }
    impl gax::polling_error_policy::PollingErrorPolicy for TestErrorPolicy {
        fn on_error(
            &self,
            _loop_start: std::time::Instant,
            _attempt_count: u32,
            error: gax::error::Error,
        ) -> gax::loop_state::LoopState {
            gax::loop_state::LoopState::Continue(error)
        }
    }

    #[derive(Debug)]
    struct TestBackoffPolicy {
        pub _name: String,
    }
    impl gax::polling_backoff_policy::PollingBackoffPolicy for TestBackoffPolicy {
        fn wait_period(
            &self,
            _loop_start: std::time::Instant,
            _attempt_count: u32,
        ) -> std::time::Duration {
            std::time::Duration::from_millis(1)
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn default_polling_policies() -> TestResult {
        let (endpoint, _server) = echo_server::start().await?;
        let client = echo_server::builder(endpoint)
            .with_credentials(auth::credentials::testing::test_credentials())
            .build()
            .await?;

        let options = gax::options::RequestOptions::default();
        // Verify the functions are callable from outside the crate.
        let _ = client.get_polling_error_policy(&options);
        let _ = client.get_polling_backoff_policy(&options);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn client_config_polling_policies() -> TestResult {
        let (endpoint, _server) = echo_server::start().await?;
        let client = echo_server::builder(endpoint)
            .with_credentials(auth::credentials::testing::test_credentials())
            .with_polling_error_policy(TestErrorPolicy {
                _name: "client-polling-error".to_string(),
            })
            .with_polling_backoff_policy(TestBackoffPolicy {
                _name: "client-polling-backoff".to_string(),
            })
            .build()
            .await?;

        let options = gax::options::RequestOptions::default();
        let polling = client.get_polling_error_policy(&options);
        let fmt = format!("{polling:?}");
        assert!(fmt.contains("client-polling-error"), "{polling:?}");
        let backoff = client.get_polling_backoff_policy(&options);
        let fmt = format!("{backoff:?}");
        assert!(fmt.contains("client-polling-backoff"), "{backoff:?}");

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn request_options_polling_policies() -> TestResult {
        let (endpoint, _server) = echo_server::start().await?;
        let client = echo_server::builder(endpoint)
            .with_credentials(auth::credentials::testing::test_credentials())
            .with_polling_error_policy(TestErrorPolicy {
                _name: "client-polling-error".to_string(),
            })
            .with_polling_backoff_policy(TestBackoffPolicy {
                _name: "client-polling-backoff".to_string(),
            })
            .build()
            .await?;

        let mut options = gax::options::RequestOptions::default();
        options.set_polling_error_policy(TestErrorPolicy {
            _name: "request-options-polling-error".to_string(),
        });
        options.set_polling_backoff_policy(TestBackoffPolicy {
            _name: "request-options-polling-backoff".to_string(),
        });
        let polling = client.get_polling_error_policy(&options);
        let fmt = format!("{polling:?}");
        assert!(fmt.contains("request-options-polling-error"), "{polling:?}");
        let backoff = client.get_polling_backoff_policy(&options);
        let fmt = format!("{backoff:?}");
        assert!(
            fmt.contains("request-options-polling-backoff"),
            "{backoff:?}"
        );

        Ok(())
    }
}
