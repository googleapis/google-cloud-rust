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

use google_cloud_bigtable_admin_v2::client::BigtableTableAdmin;
use google_cloud_bigtable_admin_v2::model::{CheckConsistencyRequest, CheckConsistencyResponse};
use google_cloud_bigtable_admin_v2::stub::BigtableTableAdmin as BigtableTableAdminStub;
use google_cloud_gax::Result as GaxResult;
use google_cloud_gax::error::Error;
use google_cloud_gax::error::rpc::{Code, Status};
use google_cloud_gax::exponential_backoff::{ExponentialBackoff, ExponentialBackoffBuilder};
use google_cloud_gax::options::{RequestOptions, RequestOptionsBuilder as _};
use google_cloud_gax::polling_error_policy::{Aip194Strict, PollingErrorPolicyExt as _};
use google_cloud_gax::response::Response;
use mockall::{Sequence, mock};
use std::time::Duration;

mock! {
    #[derive(Debug)]
    pub TableAdmin {}

    impl BigtableTableAdminStub for TableAdmin {
        async fn check_consistency(
            &self,
            req: CheckConsistencyRequest,
            options: RequestOptions,
        ) -> GaxResult<Response<CheckConsistencyResponse>>;
    }
}

fn fast_backoff() -> ExponentialBackoff {
    ExponentialBackoffBuilder::new()
        .with_initial_delay(Duration::from_millis(1))
        .with_maximum_delay(Duration::from_millis(1))
        .clamp()
}

fn check_request(req: &CheckConsistencyRequest, options: &RequestOptions) {
    assert_eq!(req.name, "projects/p/instances/i/tables/t");
    assert_eq!(req.consistency_token, "token-123");
    assert!(
        options.retry_policy().is_some(),
        "retry_policy should be set to NeverRetry"
    );
}

#[tokio::test]
async fn immediate_consistency() -> anyhow::Result<()> {
    let mut mock = MockTableAdmin::new();
    mock.expect_check_consistency()
        .times(1)
        .returning(|req, options| {
            check_request(&req, &options);
            Ok(Response::from(
                CheckConsistencyResponse::new().set_consistent(true),
            ))
        });

    let client = BigtableTableAdmin::from_stub(mock);
    client
        .check_consistency()
        .set_name("projects/p/instances/i/tables/t")
        .set_consistency_token("token-123")
        .with_polling_backoff_policy(fast_backoff())
        .until_consistent()
        .await?;
    Ok(())
}

#[tokio::test]
async fn eventual_consistency_after_in_progress() -> anyhow::Result<()> {
    let mut mock = MockTableAdmin::new();
    let mut seq = Sequence::new();

    mock.expect_check_consistency()
        .times(2)
        .in_sequence(&mut seq)
        .returning(|req, options| {
            check_request(&req, &options);
            Ok(Response::from(
                CheckConsistencyResponse::new().set_consistent(false),
            ))
        });
    mock.expect_check_consistency()
        .times(1)
        .in_sequence(&mut seq)
        .returning(|req, options| {
            check_request(&req, &options);
            Ok(Response::from(
                CheckConsistencyResponse::new().set_consistent(true),
            ))
        });

    let client = BigtableTableAdmin::from_stub(mock);
    client
        .check_consistency()
        .set_name("projects/p/instances/i/tables/t")
        .set_consistency_token("token-123")
        .with_polling_backoff_policy(fast_backoff())
        .until_consistent()
        .await?;
    Ok(())
}

#[tokio::test]
async fn transient_error_then_consistent() -> anyhow::Result<()> {
    let mut mock = MockTableAdmin::new();
    let mut seq = Sequence::new();

    mock.expect_check_consistency()
        .times(1)
        .in_sequence(&mut seq)
        .returning(|req, options| {
            check_request(&req, &options);
            Ok(Response::from(
                CheckConsistencyResponse::new().set_consistent(false),
            ))
        });
    mock.expect_check_consistency()
        .times(1)
        .in_sequence(&mut seq)
        .returning(|req, options| {
            check_request(&req, &options);
            Err(Error::service(
                Status::default().set_code(Code::Unavailable),
            ))
        });
    mock.expect_check_consistency()
        .times(1)
        .in_sequence(&mut seq)
        .returning(|req, options| {
            check_request(&req, &options);
            Ok(Response::from(
                CheckConsistencyResponse::new().set_consistent(true),
            ))
        });

    let client = BigtableTableAdmin::from_stub(mock);
    client
        .check_consistency()
        .set_name("projects/p/instances/i/tables/t")
        .set_consistency_token("token-123")
        .with_polling_backoff_policy(fast_backoff())
        .until_consistent()
        .await?;
    Ok(())
}

#[tokio::test]
async fn permanent_error_stops_immediately() {
    let mut mock = MockTableAdmin::new();
    let mut seq = Sequence::new();

    mock.expect_check_consistency()
        .times(1)
        .in_sequence(&mut seq)
        .returning(|req, options| {
            check_request(&req, &options);
            Ok(Response::from(
                CheckConsistencyResponse::new().set_consistent(false),
            ))
        });
    mock.expect_check_consistency()
        .times(1)
        .in_sequence(&mut seq)
        .returning(|req, options| {
            check_request(&req, &options);
            Err(Error::service(
                Status::default().set_code(Code::PermissionDenied),
            ))
        });

    let client = BigtableTableAdmin::from_stub(mock);
    let err = client
        .check_consistency()
        .set_name("projects/p/instances/i/tables/t")
        .set_consistency_token("token-123")
        .with_polling_backoff_policy(fast_backoff())
        .until_consistent()
        .await
        .expect_err("expected permanent error");
    assert_eq!(
        err.status().map(|s| s.code),
        Some(Code::PermissionDenied),
        "{err:?}"
    );
}

#[tokio::test]
async fn polling_policy_exhausted_while_in_progress() {
    let mut mock = MockTableAdmin::new();
    mock.expect_check_consistency()
        .times(3)
        .returning(|req, options| {
            check_request(&req, &options);
            Ok(Response::from(
                CheckConsistencyResponse::new().set_consistent(false),
            ))
        });

    let client = BigtableTableAdmin::from_stub(mock);
    let err = client
        .check_consistency()
        .set_name("projects/p/instances/i/tables/t")
        .set_consistency_token("token-123")
        .with_polling_error_policy(Aip194Strict.with_attempt_limit(3))
        .with_polling_backoff_policy(fast_backoff())
        .until_consistent()
        .await
        .expect_err("expected exhausted error");
    assert!(err.is_exhausted(), "{err:?}");
    assert!(
        err.to_string().contains("projects/p/instances/i/tables/t"),
        "{err:?}"
    );
}

#[tokio::test]
async fn polling_policy_exhausted_on_transient_error() {
    let mut mock = MockTableAdmin::new();
    mock.expect_check_consistency()
        .times(3)
        .returning(|req, options| {
            check_request(&req, &options);
            Err(Error::service(
                Status::default().set_code(Code::Unavailable),
            ))
        });

    let client = BigtableTableAdmin::from_stub(mock);
    let err = client
        .check_consistency()
        .set_name("projects/p/instances/i/tables/t")
        .set_consistency_token("token-123")
        .with_polling_error_policy(Aip194Strict.with_attempt_limit(3))
        .with_polling_backoff_policy(fast_backoff())
        .until_consistent()
        .await
        .expect_err("expected exhausted error");
    assert_eq!(
        err.status().map(|s| s.code),
        Some(Code::Unavailable),
        "{err:?}"
    );
}
