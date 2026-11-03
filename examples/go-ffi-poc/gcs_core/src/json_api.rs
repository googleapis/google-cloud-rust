// Copyright 2026 Google LLC
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

//! JSON API calls the Rust SDK does not offer: its control plane is gRPC-only, so listing over
//! HTTP/JSON is implemented here, with the core's credentials. Each request runs through the
//! SDK's own retry loop, retry/backoff policies and error types, so retries, caller predicates,
//! attempt limits, time limits and error mapping behave exactly as for every other core call.
//! The response is the raw JSON API resource, which each language decodes with the JSON types
//! it already ships.
//!
//! `retry_loop`, `Error::http` and `Error::service_with_http_metadata` are doc-hidden SDK APIs
//! (the SDK's own HTTP transport is built on them); the exact-version pins in Cargo.toml keep
//! them stable for a given core build.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use google_cloud_auth::credentials::CacheableResource;
use google_cloud_gax::backoff_policy::BackoffPolicy;
use google_cloud_gax::error::Error as GaxError;
use google_cloud_gax::error::rpc::Status as RpcStatus;
use google_cloud_gax::retry_loop_internal::retry_loop;
use google_cloud_gax::retry_policy::{RetryPolicy, RetryPolicyExt};
use google_cloud_gax::retry_throttler::{AdaptiveThrottler, SharedRetryThrottler};
use google_cloud_storage::retry_policy::RetryableErrors;
use serde::Deserialize;

use crate::options::CallOptions;
use crate::{GcsClient, Status};

/// `storage.objects.list` query (JSON API parameter names), plus the bucket.
#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
struct ListObjects {
    bucket: String,
    prefix: String,
    delimiter: String,
    page_token: String,
    max_results: i64,
    versions: bool,
    start_offset: String,
    end_offset: String,
    include_trailing_delimiter: bool,
    match_glob: String,
    include_folders_as_prefixes: bool,
    soft_deleted: bool,
    projection: String,
    fields: String,
    filter: String,
}

/// One page of `storage.objects.list`. Returns the JSON API `Objects` resource.
pub(crate) async fn list_objects(client: &GcsClient, req: serde_json::Value, opts: &CallOptions) -> Result<String, Status> {
    let r: ListObjects = serde_json::from_value(req).map_err(|e| Status::invalid(format!("storage.objects.list: {e}")))?;
    let bucket = r.bucket.trim_start_matches("projects/_/buckets/");
    if bucket.is_empty() {
        return Err(Status::invalid("bucket is required"));
    }
    // Compact responses, as every generated JSON API client requests.
    let mut q: Vec<(&'static str, String)> = vec![("prettyPrint", "false".to_string())];
    for (k, v) in [
        ("prefix", &r.prefix),
        ("delimiter", &r.delimiter),
        ("pageToken", &r.page_token),
        ("startOffset", &r.start_offset),
        ("endOffset", &r.end_offset),
        ("matchGlob", &r.match_glob),
        ("projection", &r.projection),
        ("fields", &r.fields),
        ("filter", &r.filter),
    ] {
        if !v.is_empty() {
            q.push((k, v.clone()));
        }
    }
    if let Some(up) = opts.user_project.as_deref().filter(|s| !s.is_empty()) {
        q.push(("userProject", up.to_string()));
    }
    for (k, on) in [
        ("versions", r.versions),
        ("includeTrailingDelimiter", r.include_trailing_delimiter),
        ("includeFoldersAsPrefixes", r.include_folders_as_prefixes),
        ("softDeleted", r.soft_deleted),
    ] {
        if on {
            q.push((k, "true".to_string()));
        }
    }
    if r.max_results > 0 {
        q.push(("maxResults", r.max_results.to_string()));
    }
    let url = format!("{}/storage/v1/b/{bucket}/o", client.json_endpoint);
    // Listing is a read: idempotent unless the caller says otherwise.
    get(client, url, q, opts, opts.idempotent(true))?.await
}

/// A GET under the caller's retry settings, or the storage client's defaults (retry transient
/// errors for up to 300s, exponential backoff 1s..60s).
fn get(
    client: &GcsClient,
    url: String,
    query: Vec<(&'static str, String)>,
    opts: &CallOptions,
    idempotent: bool,
) -> Result<impl Future<Output = Result<String, Status>> + Send + 'static, Status> {
    let policy: Arc<dyn RetryPolicy> = match opts.retry_policy(client)? {
        Some(p) => p.into(),
        None => Arc::new(RetryableErrors.with_time_limit(Duration::from_secs(300))),
    };
    let backoff: Arc<dyn BackoffPolicy> = match opts.backoff_policy()? {
        Some(b) => b.into(),
        None => Arc::new(google_cloud_storage::backoff_policy::default()),
    };
    let throttler: SharedRetryThrottler = Arc::new(Mutex::new(AdaptiveThrottler::default()));
    let call = Arc::new(Get { client: client.clone(), url, query, opts: opts.clone() });
    // Each attempt is an owned, explicitly `Send` future: rustc cannot prove `Send` for
    // borrowing attempt futures inside the (higher-ranked) retry loop of a spawned operation.
    let inner = move |remaining: Option<Duration>| -> Attempt { Box::pin(get_once(call.clone(), remaining)) };
    let sleep = async |d| tokio::time::sleep(d).await;
    Ok(async move { retry_loop(inner, sleep, idempotent, throttler, policy, backoff).await.map_err(|e| Status::from_sdk(&e)) })
}

type Attempt = Pin<Box<dyn Future<Output = Result<String, GaxError>> + Send>>;

struct Get {
    client: GcsClient,
    url: String,
    query: Vec<(&'static str, String)>,
    opts: CallOptions,
}

/// One attempt, with the same headers and error types as the SDK's HTTP transport.
async fn get_once(call: Arc<Get>, remaining: Option<Duration>) -> Result<String, GaxError> {
    let Get { client, url, query, opts } = &*call;
    let auth = match client.credentials.headers(http::Extensions::new()).await {
        Ok(CacheableResource::New { data, .. }) => data,
        // Only returned when the request carries an entity tag, which this one does not.
        Ok(CacheableResource::NotModified) => http::HeaderMap::new(),
        Err(e) => return Err(GaxError::authentication(e)),
    };
    let mut b = client.http.get(url.as_str()).query(query).headers(auth);
    let timeout = match (opts.attempt_timeout_ms, remaining) {
        (0, r) => r,
        (ms, None) => Some(Duration::from_millis(ms)),
        (ms, Some(r)) => Some(r.min(Duration::from_millis(ms))),
    };
    if let Some(t) = timeout {
        b = b.timeout(t);
    }
    if let Some(ua) = &client.user_agent {
        b = b.header(http::header::USER_AGENT, ua.as_str());
    }
    if let Some(qp) = &client.quota_project {
        b = b.header("x-goog-user-project", qp.as_str());
    }
    for (k, v) in opts.header_pairs().map_err(|s| GaxError::ser(s.message))? {
        b = b.header(k, v);
    }
    let resp = b.send().await.map_err(|e| {
        if e.is_connect() {
            GaxError::connect(e)
        } else if e.is_timeout() {
            GaxError::timeout(e)
        } else {
            GaxError::io(e)
        }
    })?;
    let code = resp.status().as_u16();
    if resp.status().is_success() {
        return resp.text().await.map_err(GaxError::io);
    }
    // JSON API error body: {"error": {"code": 404, "message": "...", "errors": [...]}}.
    let headers = resp.headers().clone();
    let body = resp.bytes().await.map_err(GaxError::io)?;
    Err(match RpcStatus::try_from(&body) {
        Ok(status) => GaxError::service_with_http_metadata(status, Some(code), Some(headers)),
        Err(_) => GaxError::http(code, headers, body),
    })
}
