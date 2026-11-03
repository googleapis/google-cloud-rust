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

//! Credentials for the engine.
//!
//! The POC only supported Application Default Credentials, which meant every Go credential
//! option (WithCredentialsFile/JSON, WithTokenSource, impersonation, ...) was silently ignored
//! by the Rust half. Two additions close most of that gap:
//!
//! * explicit credential JSON (service account / authorized user), and
//! * a *token callback*: the engine asks Go for an access token, so whatever credential the Go
//!   client was configured with is used unchanged. Calls happen on Tokio's blocking pool (a Go
//!   token refresh may do network I/O) and results are cached until shortly before expiry.

use std::ffi::{CStr, c_char};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use google_cloud_auth::credentials::{CacheableResource, Credentials, CredentialsProvider, EntityTag};
use google_cloud_gax::error::CredentialsError;
use http::{Extensions, HeaderMap, HeaderValue};

use crate::{GcsReleaseCb, GcsTokenCb, Status, c_free, lock};

pub(crate) fn from_json(js: &str) -> Result<Credentials, Status> {
    let v: serde_json::Value =
        serde_json::from_str(js).map_err(|e| Status::invalid(format!("credentials json: {e}")))?;
    let kind = v.get("type").and_then(|t| t.as_str()).unwrap_or_default().to_string();
    let built = match kind.as_str() {
        "service_account" => google_cloud_auth::credentials::service_account::Builder::new(v).build(),
        "authorized_user" => google_cloud_auth::credentials::user_account::Builder::new(v).build(),
        other => {
            return Err(Status::invalid(format!(
                "credentials type {other:?} is not supported by the Rust engine; use the token callback"
            )));
        }
    };
    built.map_err(|e| Status::invalid(format!("credentials: {e}")))
}

/// Refresh this long before the reported expiry (Go's oauth2 uses 10s; be more conservative).
const EXPIRY_SKEW: Duration = Duration::from_secs(60);
/// When Go reports no expiry, re-ask Go after this long (Go caches internally anyway).
const UNKNOWN_EXPIRY_TTL: Duration = Duration::from_secs(30);

#[derive(Clone, Copy)]
struct TokenCb {
    cb: GcsTokenCb,
    user: usize,
}

struct Cached {
    headers: HeaderMap,
    refresh_at: Instant,
    tag: EntityTag,
}

struct GoTokenCredentials {
    cb: TokenCb,
    cache: std::sync::Mutex<Option<Cached>>,
    release: Option<GcsReleaseCb>,
}

/// The host's token source is released only after the last clone of these credentials (held
/// by clients and in-flight operations) is gone, so a closed client never leaks it and an
/// in-flight operation never sees it freed.
impl Drop for GoTokenCredentials {
    fn drop(&mut self) {
        if let Some(release) = self.release {
            unsafe { release(self.cb.user) };
        }
    }
}

impl std::fmt::Debug for GoTokenCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GoTokenCredentials").finish_non_exhaustive()
    }
}

pub(crate) fn go_token_credentials(cb: GcsTokenCb, user: usize, release: Option<GcsReleaseCb>) -> Credentials {
    Credentials::from(GoTokenCredentials { cb: TokenCb { cb, user }, cache: std::sync::Mutex::new(None), release })
}

fn call_go(cb: TokenCb) -> Result<(String, i64), CredentialsError> {
    let mut tok: *mut c_char = std::ptr::null_mut();
    let mut err: *mut c_char = std::ptr::null_mut();
    let mut exp: i64 = 0;
    let rc = unsafe { (cb.cb)(cb.user, &mut tok, &mut exp, &mut err) };
    let take = |p: *mut c_char| -> Option<String> {
        if p.is_null() {
            return None;
        }
        let s = unsafe { CStr::from_ptr(p) }.to_string_lossy().into_owned();
        unsafe { c_free(p) };
        Some(s)
    };
    let tok = take(tok);
    let err = take(err);
    if rc != 0 {
        // Treat Go-side failures as transient: the Go credential decides when to give up.
        return Err(CredentialsError::from_msg(true, err.unwrap_or_else(|| "go token source failed".into())));
    }
    match tok {
        Some(t) if !t.is_empty() => Ok((t, exp)),
        _ => Err(CredentialsError::from_msg(false, "go token source returned an empty token")),
    }
}

impl CredentialsProvider for GoTokenCredentials {
    async fn headers(&self, extensions: Extensions) -> Result<CacheableResource<HeaderMap>, CredentialsError> {
        {
            let cache = lock(&self.cache);
            if let Some(c) = cache.as_ref() {
                if Instant::now() < c.refresh_at {
                    if extensions.get::<EntityTag>() == Some(&c.tag) {
                        return Ok(CacheableResource::NotModified);
                    }
                    return Ok(CacheableResource::New { entity_tag: c.tag.clone(), data: c.headers.clone() });
                }
            }
        }
        let cb = self.cb;
        let (token, expiry_unix) = tokio::task::spawn_blocking(move || call_go(cb))
            .await
            .map_err(|e| CredentialsError::from_msg(true, format!("token task failed: {e}")))??;
        let mut value = HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|e| CredentialsError::from_msg(false, format!("invalid token: {e}")))?;
        value.set_sensitive(true);
        let mut headers = HeaderMap::new();
        headers.insert(http::header::AUTHORIZATION, value);
        let ttl = if expiry_unix > 0 {
            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as i64;
            Duration::from_secs((expiry_unix - now).max(0) as u64).saturating_sub(EXPIRY_SKEW)
        } else {
            UNKNOWN_EXPIRY_TTL
        };
        let tag = EntityTag::new();
        *lock(&self.cache) = Some(Cached { headers: headers.clone(), refresh_at: Instant::now() + ttl, tag: tag.clone() });
        Ok(CacheableResource::New { entity_tag: tag, data: headers })
    }

    async fn universe_domain(&self) -> Option<String> {
        None
    }
}
