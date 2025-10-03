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

use google_cloud_auth::credentials::{Builder, impersonated};

async fn build_adc_credentials() -> anyhow::Result<()> {
    // ANCHOR: ADC
    let credentials = Builder::default().build()?;
    // ANCHOR_END: ADC

    dbg!(credentials);

    Ok(())
}

async fn build_impersonated_credentials() -> anyhow::Result<()> {
    let source_credentials = Builder::default().build()?;
    let impersonate_service_account_email = "GOOGLE_SERVICE_ACCOUNT_EMAIL";

    // ANCHOR: service-account-impersonation
    let credentials = impersonated::Builder::from_source_credentials(source_credentials)
        .with_target_principal(impersonate_service_account_email)
        .build()?;
    // ANCHOR_END: service-account-impersonation

    dbg!(credentials);

    Ok(())
}
