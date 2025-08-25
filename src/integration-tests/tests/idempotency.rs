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
    use gax::options::RequestOptionsBuilder;
    use gax::retry_policy::RetryPolicy;
    use gax::retry_result::RetryResult;
    use gax::retry_state::RetryState;
    use gax::throttle_result::ThrottleResult;

    mockall::mock! {
        #[derive(Debug)]
        RetryPolicy {}
        impl RetryPolicy for RetryPolicy {
            fn on_error(&self,       state: &RetryState, error: Error) -> RetryResult;
            fn on_throttle(&self,    state: &RetryState, error: Error) -> ThrottleResult;
            fn remaining_time(&self, state: &RetryState) -> Option<std::time::Duration>;
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
            .withf(move |state, _| state.idempotent == expected_idempotency)
            .once()
            .in_sequence(&mut seq)
            .returning(move |_, e| RetryResult::Permanent(e));

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
        async fn default_idempotent() -> Result {
            let client = SecretManagerService::builder().build().await?;

            // We are calling `GetSecret`, which is a `GET`. This request should
            // be idempotent.
            let _ = client
                .get_secret()
                .set_name("projects/fake-project/secrets/fake-secret")
                .with_retry_policy(expect_idempotent())
                .send()
                .await;

            Ok(())
        }

        #[tokio::test]
        async fn default_non_idempotent() -> Result {
            let client = SecretManagerService::builder().build().await?;

            // We are calling `AddSecretVersion`, which is a `POST`. This
            // request should not be idempotent.
            let _ = client
                .add_secret_version()
                .set_parent("projects/fake-project/secrets/fake-secret")
                .with_retry_policy(expect_non_idempotent())
                .send()
                .await;

            Ok(())
        }
    }

    mod grpc {
        use super::*;
        use firestore::client::Firestore;
        use storage::client::StorageControl;

        #[tokio::test]
        async fn default_idempotent() -> Result {
            let client = Firestore::builder().build().await?;

            // We are calling `GetDocument`, which is a `GET`. This request
            // should be idempotent.
            let _ = client
                .get_document()
                .set_name("invalid")
                .with_retry_policy(expect_idempotent())
                .send()
                .await;

            Ok(())
        }

        #[tokio::test]
        async fn default_non_idempotent() -> Result {
            let client = Firestore::builder().build().await?;

            // We are calling `BeginTransaction`, which is a `POST`. This
            // request should not be idempotent.
            let _ = client
                .begin_transaction()
                .set_database("invalid")
                .with_retry_policy(expect_non_idempotent())
                .send()
                .await;

            Ok(())
        }

        #[tokio::test]
        async fn request_id_default_idempotent() -> Result {
            let client = StorageControl::builder().build().await?;

            // This RPC has an auto-populated request ID field. It should be
            // idempotent.
            let _ = client
                .create_folder()
                .set_parent("invalid")
                .with_retry_policy(expect_idempotent())
                .send()
                .await;

            Ok(())
        }
    }
}
