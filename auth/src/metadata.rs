use http::HeaderMap;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::env;
use tokio::time::{self, Duration};

use super::{Error, Result};

// TODO(codyoss): cache a client
// TODO(codyoss): move client impl elsewhere
// TODO(codyoss): Instead of vecs things could be slices... don't need ownership
// TODO(codyoss): funcs could take &str or impl Into<String>?

/// A wrapper around a HTTP client used to talk to a Google Cloud metadata service.
struct MetadataClient {
    c: Client,
    // TODO(codyoss): could include a cache here
}

impl MetadataClient {
    /// Create a new Metadata client with the proper headers required to talk to
    /// the Google Cloud metadata service.
    fn new() -> Self {
        let mut headers = HeaderMap::with_capacity(2);
        headers.insert("Metadata-Flavor", "Google".parse().unwrap());
        headers.insert("User-Agent", "gcloud-rust/0.1".parse().unwrap());
        let client = Client::builder().default_headers(headers).build().unwrap();
        Self { c: client }
    }
}

/// Makes a request to the supplied metadata endpoint.
///
/// ```rust
/// let project_id = get("project/project-id")?;
/// println!("{}", project_id);
/// ```
// TODO(codyoss): I wonder if this should be a pathbuf
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
    let req = MetadataClient::new().c.get(url);
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
pub async fn on_gce() -> bool {
    if std::env::var("GCE_METADATA_HOST").is_ok() {
        return true;
    }
    let client = MetadataClient::new();
    let res1 = client.c.get("http://169.254.169.254").send();
    let res2 = tokio::net::TcpListener::bind(("metadata.google.internal", 0));

    // TODO(codyoss): checks could validate something instead of just a ping
    let check = tokio::select! {
        _ = time::sleep(Duration::from_secs(5)) => false,
        _ = res1 => true,
        _ = res2 => true,
    };
    if check {
        return true;
    }

    // TODO(codyoss): Could try harder for GCE /w GCE instance info.

    false
}

/// The result of requesting a token from the metadata service.
#[derive(Deserialize)]
pub struct Token {
    pub access_token: String,
    pub expires_in: i64,
}

/// Fetches a [Token] from the metadata service with the provided scopes. If an
/// account is not provided the value will be set to `default`.
pub async fn access_token(account: Option<&str>, scopes: Vec<String>) -> Result<Token> {
    if scopes.is_empty() {
        return Err(Error::Other("scopes must be provided".into()));
    }
    if !on_gce().await {
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
