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

//! This module contains submodules with authentication examples and some
//! helper functions to simplify the integration test.

pub mod adc;
pub mod api_key;
pub mod impersonation;
pub mod request_id_token;
pub mod verify_id_token;

use google_cloud_auth::credentials::idtoken::Builder as IdTokenBuilder;
use google_cloud_gax::error::rpc::{Code, StatusDetails};
use httptest::{Expectation, Server, matchers::*, responders::*};

pub async fn drive_adc() -> anyhow::Result<()> {
    let Err(err) = adc::sample().await else {
        return Ok(());
    };
    // When the credentials lack a quota project the service returns this
    // error.
    let details = err
        .downcast_ref::<google_cloud_gax::error::Error>()
        .and_then(|s| s.status())
        .filter(|s| s.code == Code::PermissionDenied)
        .and_then(|s| {
            // Must have a StatusDetails::ErrorInfo(_) in the details.
            s.details.iter().find(
                // ErrorInfo.reason can be treated as an enum. Testing its
                // value programmatically is fine.
                |d| matches!(d, StatusDetails::ErrorInfo(i) if i.reason == "SERVICE_DISABLED"),
            )
        });
    let Some(StatusDetails::ErrorInfo(_)) = details else {
        return Err(err);
    };
    eprintln!("ignoring error: {err:?}");
    Ok(())
}

pub async fn drive_id_token() -> anyhow::Result<()> {
    const AUDIENCE: &str = "https://my-service.a.run.app";

    let credentials = match IdTokenBuilder::new(AUDIENCE).build() {
        Ok(c) => c,
        Err(e) if e.is_not_supported() => {
            eprintln!("ADC credentials type not supported for idtoken credentials: {e:?}");
            return Ok(());
        }
        Err(e) => return Err(e.into()),
    };

    let id_token = request_id_token::sample(AUDIENCE).await?;
    verify_id_token::sample(&id_token, AUDIENCE).await?;

    // Create a server so the `api_call_with_id_token()` example has
    // a valid URL to call upon.
    let server = Server::run();
    server.expect(
        Expectation::matching(all_of![
            request::method_path("GET", "/"),
            request::headers(contains(("authorization", format!("Bearer {}", id_token)))),
        ])
        .respond_with(status_code(200)),
    );

    let target_url = server.url("/").to_string();
    request_id_token::api_call_with_id_token(&target_url, &credentials).await?;
    Ok(())
}
