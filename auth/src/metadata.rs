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

// TODO(codyoss): cache a client
// TODO(codyoss): funcs could take &str or impl Into<String>?

// TODO: Create a wrapper for reuse that cachces useful values.
fn new_metadata_client() -> Client {
    let mut headers = HeaderMap::with_capacity(2);
    headers.insert("Metadata-Flavor", "Google".parse().unwrap());
    headers.insert("User-Agent", "gcloud-rust/0.1".parse().unwrap());
    Client::builder().default_headers(headers).build().unwrap()
}

/// Makes a request to the supplied metadata endpoint.
///
/// ```rust
/// let project_id = get("project/project-id")?;
/// println!("{}", project_id);
/// ```
#[allow(dead_code)]
pub async fn get(suffix: String) -> Result<String> {
    get_with_query::<()>(suffix, None).await
}

async fn get_with_query<T: Serialize + ?Sized>(
    suffix: String,
    query: Option<&T>,
) -> Result<String> {
    let host = env::var("GCE_METADATA_HOST")
        .unwrap_or_else(|_| -> String { String::from("169.254.169.254") });
    let suffix = suffix.trim_start_matches('/');
    let url = format!("http://{}/computeMetadata/v1/{}", host, suffix);

    // TODO(codyoss): retry
    let req = new_metadata_client().get(url);
    let req = if let Some(query) = query {
        req.query(query)
    } else {
        req
    };
    let res = req.send().await?;
    if !res.status().is_success() {
        return Err(Error::Other(format!(
            "bad request with status: {}",
            res.status().as_str()
        )));
    }
    let content = res.text().await?;
    Ok(content)
}

/// Checks the environment to determine if code is executing in a Google Cloud
/// environment.
pub async fn is_running_on_gce() -> bool {
    // If a user explicitly provides envvar to talk to the metadata service we
    // trust them.
    if std::env::var("GCE_METADATA_HOST").is_ok() {
        return true;
    }
    let client = new_metadata_client();
    let res1 = client.get("http://169.254.169.254").send();
    let res2 = tokio::net::TcpListener::bind(("metadata.google.internal", 0));

    // Race pinging the metadata service by IP and DNS. Depending on the
    // environment different requests return faster. In the future we should
    // check for more envvars.
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
    let account = account.unwrap_or("default");
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
