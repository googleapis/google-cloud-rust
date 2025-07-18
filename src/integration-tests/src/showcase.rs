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

use crate::Result;
use anyhow::Error;
use gax::options::RequestOptionsBuilder;
use gax::retry_policy::RetryPolicyExt;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;
use tokio::process::Command;

mod compliance;
mod echo;
mod identity;

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

    let path = install().await?;
    let showcase: PathBuf = [path.as_str(), "bin", "gapic-showcase"].iter().collect();
    tracing::info!("starting {showcase:?}");
    let child = Command::new(showcase)
        .args(["run"])
        .stdin(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .map_err(Error::from)?;
    tracing::info!("started showcase server: {child:?}");
    if wait_until_ready().await.is_err() {
        tracing::error!("showcase server is not ready {child:?}");
    }

    tracing::info!("running tests for Echo service");
    echo::run().await?;

    tracing::info!("running tests for Identity service");
    identity::run().await?;

    tracing::info!("running tests for Compliance service");
    compliance::run().await?;

    Ok(())
}

async fn install() -> Result<String> {
    let install = Command::new("go")
        .args(["install", SHOWCASE_NAME])
        .stdin(Stdio::null())
        .output()
        .await
        .map_err(Error::from)?;
    if !install.status.success() {
        return Err(Error::msg(format!(
            "error installing showcase: {install:?}"
        )));
    }
    tracing::info!("installed showcase binary: {install:?}");
    let gopath = Command::new("go")
        .args(["env", "GOPATH"])
        .stdin(Stdio::null())
        .output()
        .await
        .map_err(Error::from)?;
    if !gopath.status.success() {
        return Err(Error::msg(format!(
            "error installing showcase: {install:?}"
        )));
    }
    let mut dir = gopath.stdout.clone();
    assert!(!dir.is_empty(), "{gopath:?}");
    dir.truncate(dir.len() - 1);
    String::from_utf8(dir).map_err(Error::from)
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
