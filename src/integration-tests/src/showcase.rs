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

use crate::Error;
use crate::Result;
use gax::options::RequestOptionsBuilder;
use showcase::model::compliance_data::LifeKingdom;
use std::process::Stdio;
use std::time::Duration;
use tokio::process::Command;

const SHOWCASE_NAME: &str = "github.com/googleapis/gapic-showcase/cmd/gapic-showcase@latest";

pub async fn run() -> Result<()> {
    use showcase::model::*;

    #[cfg(feature = "log-integration-tests")]
    let _guard = {
        use tracing_subscriber::fmt::format::FmtSpan;
        let subscriber = tracing_subscriber::fmt()
            .with_level(true)
            .with_thread_ids(true)
            .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
            .finish();

        tracing::subscriber::set_default(subscriber)
    };

    install().await?;
    let child = Command::new("go")
        .args(["run", SHOWCASE_NAME, "run"])
        .stdin(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .map_err(Error::other)?;
    tracing::info!("started showcase server: {child:?}");
    if wait_until_ready().await.is_err() {
        tracing::error!("showcase server is not ready {child:?}");
    }

    let client = showcase::client::Compliance::builder()
        .with_endpoint("http://localhost:7469")
        .with_credentials(auth::credentials::testing::test_credentials())
        .with_retry_policy(gax::retry_policy::NeverRetry)
        .with_tracing()
        .build()
        .await?;

    let request = RepeatRequest::new()
        .set_f_int32(1)
        .set_f_int64(2)
        .set_f_double(3)
        .set_p_int32(4)
        .set_p_int64(5)
        .set_p_double(6.5);
    let response = client
        .repeat_data_body()
        .with_request(request.clone())
        .send()
        .await?;
    tracing::info!("response: {response:?}");
    assert_eq!(response.binding_uri, "/v1beta1/repeat:body");
    assert_eq!(response.request, Some(request));

    let request = RepeatRequest::new()
        .set_f_int32(1)
        .set_f_int64(2)
        .set_f_double(3)
        .set_p_int32(4)
        .set_p_int64(5)
        .set_p_double(6.5)
        .set_info(
            ComplianceData::new()
                .set_f_string("the quick brown fox jumps over the lazy dog")
                .set_f_int32(1)
                .set_f_sint32(2)
                .set_f_sfixed32(3)
                .set_f_uint32(4_u32)
                .set_f_fixed32(5_u32)
                .set_f_int64(6)
                .set_f_sint64(7)
                .set_f_sfixed64(8)
                .set_f_sfixed64(8)
                .set_f_uint64(9_u64)
                .set_f_fixed64(10_u64)
                .set_f_double(11.25_f64)
                .set_f_float(12.5_f32)
                .set_f_bool(true)
                .set_f_bytes(bytes::Bytes::from_static(
                    b"How vexingly quick daft zebras jump!",
                ))
                .set_f_kingdom(LifeKingdom::Fungi)
                .set_f_child(
                    ComplianceDataChild::new()
                        .set_f_continent(Continent::Africa)
                        .set_p_continent(Continent::Australia),
                )
                .set_p_string(
                    "Answer to the Ultimate Question of Life, the Universe, and Everything"
                        .to_string(),
                )
                .set_p_int32(42)
                .set_p_double(42.42)
                .set_p_bool(false)
                .set_p_kingdom(LifeKingdom::Eubacteria),
        );
    let response = client
        .repeat_data_body()
        .with_request(request.clone())
        .send()
        .await?;
    tracing::info!("response: {response:?}");
    assert_eq!(response.binding_uri, "/v1beta1/repeat:body");
    assert_eq!(response.request, Some(request));

    Ok(())
}

async fn install() -> Result<()> {
    let install = Command::new("go")
        .args(["install", SHOWCASE_NAME])
        .stdin(Stdio::null())
        .output()
        .await
        .map_err(Error::other)?;
    if !install.status.success() {
        return Err(Error::other(format!(
            "error installing showcase: {install:?}"
        )));
    }
    tracing::info!("installed showcase binary: {install:?}");
    Ok(())
}

async fn wait_until_ready() -> Result<()> {
    let client = showcase::client::Testing::builder()
        .with_endpoint("http://localhost:7469")
        .with_credentials(auth::credentials::testing::test_credentials())
        .with_retry_policy(gax::retry_policy::NeverRetry)
        .with_tracing()
        .build()
        .await?;

    for _ in 0..10 {
        let attempt = client
            .list_sessions()
            .with_attempt_timeout(Duration::from_secs(1))
            .send()
            .await;
        if attempt.is_ok() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    Err(Error::other("not ready"))
}
