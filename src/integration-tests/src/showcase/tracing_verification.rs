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
use google_cloud_test_utils::test_layer::TestLayer;
use showcase::client::Identity;

pub async fn run() -> Result<()> {
    let guard = TestLayer::initialize();

    let client = Identity::builder()
        .with_endpoint("http://localhost:7469")
        .with_credentials(auth::credentials::anonymous::Builder::new().build())
        .with_tracing()
        .build()
        .await?;

    // We don't need the call to succeed, just to be sent so we can capture the span.
    let _ = client.get_user().set_name("users/test-user").send().await;

    let captured = TestLayer::capture(&guard);
    // Find the HTTP attempt span. It should have OTEL_KIND = Client.
    let http_span = captured.iter().find(|s| {
        s.attributes
            .get("otel.kind")
            .map(|v| v.as_string() == Some("Client".to_string()))
            .unwrap_or(false)
    });

    if let Some(span) = http_span {
        let resource_name = span.attributes.get("gcp.resource.name");
        assert_eq!(
            resource_name,
            Some(&"//localhost:7469/users/test-user".into()),
            "Attributes: {:?}",
            span.attributes
        );
    } else {
        panic!("HTTP Client span not found. Captured: {:?}", captured);
    }

    Ok(())
}
