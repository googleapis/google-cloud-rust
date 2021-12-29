// Copyright 2021 Google LLC
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

use http::HeaderMap;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::env;
use tokio::time::{self, Duration};

use super::{Error, Result};

const DEFAULT_ACCOUNT: &str = "default";
const GCE_METADATA_HOST_ENV: &str = "GCE_METADATA_HOST";
const DEFAULT_GCE_METADATA_HOST: &str = "169.254.169.254";
const GCE_METADATA_HOST_DNS: &str = "metadata.google.internal";

// TODO(codyoss): cache a client
// TODO(codyoss): funcs could take &str or impl Into<String>?

// TODO: Create a wrapper for reuse that caches useful values.
fn new_metadata_client() -> Client {
    let mut headers = HeaderMap::with_capacity(2);
    headers.insert("Metadata-Flavor", "Google".parse().unwrap());
    headers.insert("User-Agent", "gcloud-rust/0.1".parse().unwrap());
    Client::builder().default_headers(headers).build().unwrap()
}

/// Makes a request to the supplied metadata endpoint.
#[allow(dead_code)]
pub async fn get(suffix: impl Into<String>) -> Result<String> {
    get_with_query::<()>(suffix.into(), None).await
}

async fn get_with_query<T: Serialize + ?Sized>(
    suffix: String,
    query: Option<&T>,
) -> Result<String> {
    let host = env::var(GCE_METADATA_HOST_ENV)
        .unwrap_or_else(|_| -> String { String::from(DEFAULT_GCE_METADATA_HOST) });
    let suffix = suffix.trim_start_matches('/');

    let client = new_metadata_client();
    let content = backoff::future::retry(backoff::ExponentialBackoff::default(), || async {
        let url = format!("http://{}/computeMetadata/v1/{}", host, suffix);
        let req = client.get(url);
        let req = if let Some(query) = query {
            req.query(query)
        } else {
            req
        };
        let res = req.send().await.map_err(Error::Http)?;
        if !res.status().is_success() {
            return Err(backoff::Error::transient(Error::Other(format!(
                "bad request with status: {}",
                res.status().as_str()
            ))));
        }
        let content = res.text().await.map_err(Error::Http)?;
        Ok(content)
    })
    .await?;
    Ok(content)
}

/// Checks the environment to determine if code is executing in a Google Cloud
/// environment.
pub async fn is_running_on_gce() -> bool {
    // If a user explicitly provides env var to talk to the metadata service we
    // trust them.
    if std::env::var(GCE_METADATA_HOST_ENV).is_ok() {
        return true;
    }
    let client = new_metadata_client();
    let res1 = client.get("http://169.254.169.254").send();
    let res2 = tokio::net::TcpListener::bind((GCE_METADATA_HOST_DNS, 0));

    // Race pinging the metadata service by IP and DNS. Depending on the
    // environment different requests return faster. In the future we should
    // check for more env vars.
    let check = tokio::select! {
        _ = time::sleep(Duration::from_secs(5)) => false,
        _ = res1 => true,
        _ = res2 => true,
    };
    if check {
        return true;
    }

    false
}

/// The result of requesting a token from the metadata service.
#[derive(Deserialize)]
pub struct Token {
    pub access_token: String,
    pub expires_in: i64,
}

/// Fetches a [AccessToken] from the metadata service with the provided scopes. If an
/// account is not provided the value will be set to `default`.
pub async fn fetch_access_token(account: Option<&str>, scopes: Vec<String>) -> Result<Token> {
    if scopes.is_empty() {
        return Err(Error::Other("scopes must be provided".into()));
    }
    if !is_running_on_gce().await {
        return Err(Error::Other(
            "can't get token from metadata service, not running on GCE".into(),
        ));
    }
    let account = account.unwrap_or(DEFAULT_ACCOUNT);
    let suffix = format!("instance/service-accounts/{}/token", account);
    let query = &[("scopes", scopes.join(","))];
    let json = get_with_query(suffix, Some(query)).await?;
    let token_response: Token = serde_json::from_str(json.as_str())?;
    if token_response.expires_in == 0 || token_response.access_token.is_empty() {
        return Err(Error::Other(
            "incomplete token received from metadata".into(),
        ));
    }
    Ok(token_response)
}
