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

#[cfg(test)]
mod default_idempotency {
    // Test Design:
    //
    // In these tests, we send a request which we know will fail. The error gets
    // passed along to the retry policy, along with whether the RPC is
    // idempotent or not. We use this to verify that the operation's default
    // idempotency is correct.

    type Result = anyhow::Result<()>;
    use gax::error::Error;
    use gax::loop_state::LoopState;
    use gax::options::RequestOptionsBuilder;
    use gax::retry_policy::RetryPolicy;

    mockall::mock! {
        #[derive(Debug)]
        RetryPolicy {}
        impl RetryPolicy for RetryPolicy {
            fn on_error(&self, loop_start: std::time::Instant, attempt_count: u32, idempotent: bool, error: Error) -> LoopState;
            fn on_throttle(&self, loop_start: std::time::Instant, attempt_count: u32) -> Option<Error>;
            fn remaining_time(&self, loop_start: std::time::Instant, attempt_count: u32) -> Option<std::time::Duration>;
        }
    }

    // Returns a policy that verifies the expected idempotency, and terminates
    // the retry loop.
    fn make_retry_policy(expected_idempotency: bool) -> MockRetryPolicy {
        let mut seq = mockall::Sequence::new();
        let mut retry_policy = MockRetryPolicy::new();
        retry_policy
            .expect_remaining_time()
            .once()
            .in_sequence(&mut seq)
            .return_const(None);
        retry_policy
            .expect_on_error()
            .withf(move |_, _, idempotent, _| *idempotent == expected_idempotency)
            .once()
            .in_sequence(&mut seq)
            .returning(move |_, _, _, e| LoopState::Permanent(e));

        retry_policy
    }

    fn expect_idempotent() -> MockRetryPolicy {
        make_retry_policy(true)
    }

    fn expect_non_idempotent() -> MockRetryPolicy {
        make_retry_policy(false)
    }

    mod http {
        use super::*;
        use sm::client::SecretManagerService;

        #[tokio::test]
        async fn test_default_idempotent() -> Result {
            let client = SecretManagerService::builder().build().await?;

            // We are calling `GetSecret`, which is a `GET`. This request should
            // be idempotent.
            let _ = client
                .get_secret("invalid")
                .with_retry_policy(expect_idempotent())
                .send()
                .await;

            Ok(())
        }

        #[tokio::test]
        async fn test_default_non_idempotent() -> Result {
            let client = SecretManagerService::builder().build().await?;

            // We are calling `AddSecretVersion`, which is a `POST`. This
            // request should not be idempotent.
            let _ = client
                .add_secret_version("invalid")
                .with_retry_policy(expect_non_idempotent())
                .send()
                .await;

            Ok(())
        }
    }

    mod grpc {
        use super::*;
        use firestore::client::Firestore;

        #[tokio::test]
        async fn test_default_idempotent() -> Result {
            let client = Firestore::builder().build().await?;

            // We are calling `GetDocument`, which is a `GET`. This request
            // should be idempotent.
            let _ = client
                .get_document("invalid")
                .with_retry_policy(expect_idempotent())
                .send()
                .await;

            Ok(())
        }

        #[tokio::test]
        async fn test_default_non_idempotent() -> Result {
            let client = Firestore::builder().build().await?;

            // We are calling `BeginTransaction`, which is a `POST`. This
            // request should not be idempotent.
            let _ = client
                .begin_transaction("invalid")
                .with_retry_policy(expect_non_idempotent())
                .send()
                .await;

            Ok(())
        }
    }
}
