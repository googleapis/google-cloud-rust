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
use gax::retry_policy::RetryPolicyExt;
use std::process::Stdio;
use std::time::Duration;
use tokio::process::Command;

mod compliance;

const SHOWCASE_NAME: &str = "github.com/googleapis/gapic-showcase/cmd/gapic-showcase@v0.36.2";

pub async fn run() -> Result<()> {
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

    compliance::run().await?;

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
        .with_tracing()
        .build()
        .await?;

    let _list = client
        .list_sessions()
        .with_retry_policy(gax::retry_policy::AlwaysRetry.with_attempt_limit(10))
        .with_attempt_timeout(Duration::from_secs(1))
        .send()
        .await?;
    Ok(())
}
