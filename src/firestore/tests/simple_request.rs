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

use gax::options::RequestOptionsBuilder;
use google_cloud_firestore::client::Firestore;
use google_cloud_firestore::model;
use google_cloud_firestore::Error;
use google_cloud_firestore::Result;

/// Returns the project id used for the integration tests.
pub fn project_id() -> Result<String> {
    let project_id = std::env::var("GOOGLE_CLOUD_PROJECT").map_err(Error::other)?;
    Ok(project_id)
}

pub async fn hello_world() -> Result<()> {
    let project_id = project_id()?;
    let client = Firestore::new().await?;
    let response = client
        .create_document(
            format!("projects/{project_id}/databases/(default)/documents"),
            "greeting",
        )
        .set_document(model::Document::new().set_fields([(
            "greeting",
            model::Value::new().set_string_value("Hello World!"),
        )]))
        .send()
        .await?;
    println!("SUCCESS on create_document: {response:?}");
    Ok(())
}

pub async fn goodbye_retry() -> Result<()> {
    use gax::retry_policy::RetryPolicyExt;

    let project_id = project_id()?;
    let client = Firestore::new().await?;
    let response = client
        .create_document(
            format!("projects/{project_id}/databases/(default)/documents"),
            "valediction",
        )
        .with_retry_policy(gax::retry_policy::AlwaysRetry.with_attempt_limit(3))
        .set_document(model::Document::new().set_fields([(
            "text",
            model::Value::new().set_string_value("See you soon!"),
        )]))
        .send()
        .await?;
    println!("SUCCESS on create_document: {response:?}");
    Ok(())
}

#[cfg(all(test, feature = "run-integration-tests"))]
mod driver {
    use super::Error;
    use super::Result;

    fn report(e: Error) -> Error {
        println!("\nERROR {e}\n");
        Error::other("test failed")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_hello_world() -> Result<()> {
        super::hello_world().await.map_err(report)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_goodbye_retry() -> Result<()> {
        super::goodbye_retry().await.map_err(report)
    }
}
