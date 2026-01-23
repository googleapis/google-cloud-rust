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

use crate::credentials::CacheableResource;
use crate::errors::CredentialsError;
use crate::headers_util::build_cacheable_headers;
use crate::mds::client::Client as MDSClient;
use crate::token::CachedTokenProvider;
use http::Extensions;
use reqwest::Client;
use std::clone::Clone;
use std::fmt::Debug;
use tokio::sync::watch;
use tokio::time::{Duration, sleep};

pub(crate) const TRUST_BOUNDARY_HEADER: &str = "x-goog-allowed-locations";
const TRUST_BOUNDARIES_ENV_VAR: &str = "GOOGLE_AUTH_ENABLE_TRUST_BOUNDARIES";
const NO_OP_ENCODED_LOCATIONS: &str = "0x0";

// Refresh interval: 1 hour
const REFRESH_INTERVAL: Duration = Duration::from_secs(3600);
// Retry interval on error: 1 minute
const ERROR_RETRY_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Debug)]
pub(crate) struct TrustBoundary {
    rx_header: watch::Receiver<Option<String>>,
}

impl TrustBoundary {
    pub(crate) fn new<T>(token_provider: T, url: String) -> Self
    where
        T: CachedTokenProvider + 'static,
    {
        let enabled = Self::is_trust_boundaries_enabled();
        let (tx_header, rx_header) = watch::channel(None);

        if enabled {
            tokio::spawn(refresh_task(token_provider, url, tx_header));
        }

        Self { rx_header }
    }

    pub(crate) fn new_for_mds<T>(token_provider: T, mds_client: MDSClient) -> Self
    where
        T: CachedTokenProvider + 'static,
    {
        let enabled = Self::is_trust_boundaries_enabled();
        let (tx_header, rx_header) = watch::channel(None);

        if enabled {
            tokio::spawn(refresh_task_mds(token_provider, mds_client, tx_header));
        }

        Self { rx_header }
    }

    fn is_trust_boundaries_enabled() -> bool {
        std::env::var(TRUST_BOUNDARIES_ENV_VAR)
            .map(|v| v.to_lowercase() == "true")
            .unwrap_or(false)
    }

    pub(crate) fn header_value(&self) -> Option<String> {
        let val = self.rx_header.borrow().clone();
        if let Some(ref v) = val {
            if v == NO_OP_ENCODED_LOCATIONS {
                return None;
            }
        }
        val
    }
}

#[derive(serde::Deserialize)]
struct AllowedLocationsResponse {
    #[allow(dead_code)]
    locations: Vec<String>,
    #[serde(rename = "encodedLocations")]
    encoded_locations: String,
}

async fn fetch_trust_boundary<T>(
    token_provider: &T,
    url: &str,
) -> Result<Option<String>, CredentialsError>
where
    T: CachedTokenProvider,
{
    let token = token_provider.token(Extensions::new()).await?;
    let headers = build_cacheable_headers(&token, &None, &None)?;
    let headers = match headers {
        CacheableResource::New { data, .. } => data,
        CacheableResource::NotModified => {
            unreachable!("requested trust boundary without a caching etag")
        }
    };

    let client = Client::new();

    // TODO: retries ?
    let resp = client
        .get(url)
        .headers(headers)
        .send()
        .await
        .map_err(|e| CredentialsError::from_msg(true, e.to_string()))?;

    // TODO: add error handling - default fallback ?
    if !resp.status().is_success() {
        return Err(CredentialsError::from_msg(
            true,
            format!("Failed to fetch trust boundary: {}", resp.status()),
        ));
    }

    let response: AllowedLocationsResponse = resp
        .json()
        .await
        .map_err(|e| CredentialsError::from_msg(true, e.to_string()))?;

    if !response.encoded_locations.is_empty() {
        return Ok(Some(response.encoded_locations));
    }

    Ok(None)
}

async fn refresh_task_mds<T>(
    token_provider: T,
    mds_client: MDSClient,
    tx_header: watch::Sender<Option<String>>,
) where
    T: CachedTokenProvider,
{
    let mut url: Option<String> = None;

    loop {
        if url.is_none() {
            let res = mds_client.email().await;
            match res {
                Ok(email) => {
                    url = Some(service_account_lookup_url(&email));
                }
                Err(_e) => {
                    sleep(ERROR_RETRY_INTERVAL).await;
                    continue;
                }
            }
        }

        if let Some(ref url) = url {
            fetch_and_update(&token_provider, url, &tx_header).await;
        }
    }
}

async fn refresh_task<T>(token_provider: T, url: String, tx_header: watch::Sender<Option<String>>)
where
    T: CachedTokenProvider,
{
    loop {
        fetch_and_update(&token_provider, &url, &tx_header).await;
    }
}

async fn fetch_and_update<T>(
    token_provider: &T,
    url: &str,
    tx_header: &watch::Sender<Option<String>>,
) where
    T: CachedTokenProvider,
{
    match fetch_trust_boundary(token_provider, url).await {
        Ok(val) => {
            let _ = tx_header.send(val);
            sleep(REFRESH_INTERVAL).await;
        }
        Err(_e) => {
            // TODO: better error handling - default fallback ?
            sleep(ERROR_RETRY_INTERVAL).await;
        }
    }
}

pub(crate) fn service_account_lookup_url(email: &str) -> String {
    format!(
        "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/{}/allowedLocations",
        email
    )
}

pub(crate) fn external_account_lookup_url(audience: &str) -> Option<String> {
    let path = audience
        .trim_start_matches("//iam.googleapis.com/")
        .trim_start_matches("https://iam.googleapis.com/")
        .trim_start_matches('/');

    let parts: Vec<&str> = path.split('/').collect();

    // Workload: projects/{project}/locations/global/workloadIdentityPools/{pool}/providers/{provider} (6 parts)
    if parts.len() >= 6
        && parts[0] == "projects"
        && parts[2] == "locations"
        && parts[4] == "workloadIdentityPools"
    {
        let project = parts[1];
        let pool = parts[5];
        return Some(format!(
            "https://iamcredentials.googleapis.com/v1/projects/{}/locations/global/workloadIdentityPools/{}/allowedLocations",
            project, pool
        ));
    }

    // Workforce: locations/global/workforcePools/{pool}/providers/{provider} (4 parts)
    if parts.len() >= 4 && parts[0] == "locations" && parts[2] == "workforcePools" {
        let pool = parts[3];
        return Some(format!(
            "https://iamcredentials.googleapis.com/v1/locations/global/workforcePools/{}/allowedLocations",
            pool
        ));
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_service_account_url() {
        assert_eq!(
            service_account_lookup_url("sa@project.iam.gserviceaccount.com"),
            "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/sa@project.iam.gserviceaccount.com/allowedLocations"
        );
    }

    #[test]
    fn test_external_account_url_workload() {
        let aud = "//iam.googleapis.com/projects/12345/locations/global/workloadIdentityPools/my-pool/providers/my-provider";
        assert_eq!(
            external_account_lookup_url(aud).unwrap(),
            "https://iamcredentials.googleapis.com/v1/projects/12345/locations/global/workloadIdentityPools/my-pool/allowedLocations"
        );
    }

    #[test]
    fn test_external_account_url_workforce() {
        let aud =
            "//iam.googleapis.com/locations/global/workforcePools/my-pool/providers/my-provider";
        assert_eq!(
            external_account_lookup_url(aud).unwrap(),
            "https://iamcredentials.googleapis.com/v1/locations/global/workforcePools/my-pool/allowedLocations"
        );
    }

    #[test]
    fn test_external_account_url_invalid() {
        assert!(external_account_lookup_url("invalid").is_none());
        assert!(
            external_account_lookup_url("//iam.googleapis.com/projects/123/locations/global/wrong")
                .is_none()
        );
    }
}
