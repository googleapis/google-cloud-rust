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

//! Common test helpers for `google-cloud-spanner`.

use crate::retry_delay::{ProtoRetryInfo, RETRY_INFO_TYPE_URL};
use gaxi::grpc::tonic::{Code as TonicCode, Status as TonicStatus};
use gaxi::options::ClientConfig;
use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
use prost::Message;
use prost_types::{Any, Duration as ProtoDuration};
use std::time::Duration;

pub(crate) fn test_client_config() -> ClientConfig {
    let mut config = ClientConfig::default();
    config.cred = Some(Anonymous::new().build());
    config
}

pub(crate) fn create_unavailable_status(retry_delay: Duration) -> TonicStatus {
    let retry_info = ProtoRetryInfo {
        retry_delay: Some(ProtoDuration {
            seconds: retry_delay.as_secs() as i64,
            nanos: retry_delay.subsec_nanos() as i32,
        }),
    };

    let mut retry_buffer = vec![];
    retry_info
        .encode(&mut retry_buffer)
        .expect("encoding retry_info should succeed");

    let status = spanner_grpc_mock::google::rpc::Status {
        code: TonicCode::Unavailable as i32,
        message: "test stream unavailable with retry info".to_string(),
        details: vec![Any {
            type_url: RETRY_INFO_TYPE_URL.to_string(),
            value: retry_buffer,
        }],
    };

    let mut status_buffer = vec![];
    status
        .encode(&mut status_buffer)
        .expect("encoding mock status should succeed");

    TonicStatus::with_details(
        TonicCode::Unavailable,
        "test stream unavailable with retry info",
        status_buffer.into(),
    )
}
