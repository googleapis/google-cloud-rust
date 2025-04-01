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

use crate::Result;

pub async fn run(builder: ta::builder::telco_automation::ClientBuilder) -> Result<()> {
    // Enable a basic subscriber. Useful to troubleshoot problems and visually
    // verify tracing is doing something.
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

    let project_id = crate::project_id()?;
    let region_id = crate::region_id();
    let client = builder.build().await?;

    let response = client
        .list_orchestration_clusters(format!("projects/{project_id}/locations/{region_id}"))
        .send()
        .await;
    let err = response
        .expect_err("expect an error, the service should be disabled in integration test projects");
    let svcerror = err.as_inner::<gax::error::ServiceError>().expect(
        "expect a service error, Google Cloud returns service errors for disabled services",
    );
    assert!(
        !svcerror.status().details.is_empty(),
        "expected at least some error details {svcerror:?}"
    );

    Ok(())
}
