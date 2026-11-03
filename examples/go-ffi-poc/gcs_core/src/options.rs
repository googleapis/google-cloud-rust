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

//! Per-call options: the *caller's* policy knobs (retry policy and limits, backoff, user
//! project, per-call headers, read/write tuning), encoded as JSON so the ABI does not change
//! when a knob is added. Everything that *interprets* them — retry loops, backoff, per-RPC
//! idempotency, user-project routing — lives in this crate, once, for every language.
//!
//! ```json
//! {
//!   "retry": {"policy": "idempotent|always|never", "maxAttempts": 3, "maxRetryDurationMs": 60000,
//!             "backoff": {"initialMs": 100, "maxMs": 30000, "multiplier": 2.0}, "predicate": 0},
//!   "idempotent": true,
//!   "userProject": "billing-project",
//!   "attemptTimeoutMs": 0,
//!   "headers": {"x-goog-custom": "v"},
//!   "grpc": false, "decompress": true, "verifyCrc32c": true, "prefetchBytes": 8388608,
//!   "metadataOnly": false,
//!   "chunkSize": 16777216
//! }
//! ```

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use google_cloud_gax::backoff_policy::BackoffPolicyArg;
use google_cloud_gax::error::Error as GaxError;
use google_cloud_gax::exponential_backoff::ExponentialBackoffBuilder;
use google_cloud_gax::options::RequestOptions;
use google_cloud_gax::retry_policy::{NeverRetry, RetryPolicy, RetryPolicyArg, RetryPolicyExt};
use google_cloud_gax::retry_result::RetryResult;
use google_cloud_gax::retry_state::RetryState;
use google_cloud_storage::read_resume_policy::{ReadResumePolicy, Recommended, ResumeQuery, ResumeResult};
use google_cloud_storage::retry_policy::RetryableErrors;
use serde::Deserialize;

use crate::{GcsClient, GcsReleaseCb, GcsStatus, Status};

/// Which operations may be retried (mirrors the retry policies every SDK exposes).
#[derive(Deserialize, Default, Clone, Copy, PartialEq, Eq, Debug)]
#[serde(rename_all = "lowercase")]
pub(crate) enum RetryMode {
    /// Retry transient errors of idempotent operations. *Which* operations are idempotent is
    /// decided by the core (see `policy.rs`), from the request itself.
    #[default]
    Idempotent,
    /// Retry transient errors of every operation.
    Always,
    /// Never retry.
    Never,
}

#[derive(Deserialize, Default, Clone, Debug)]
#[serde(rename_all = "camelCase", default)]
pub(crate) struct Backoff {
    pub initial_ms: u64,
    /// 0 = default maximum (30s).
    pub max_ms: u64,
    /// <= 1 = default (2.0).
    pub multiplier: f64,
}

#[derive(Deserialize, Default, Clone, Debug)]
#[serde(rename_all = "camelCase", default)]
pub(crate) struct RetryOpts {
    pub policy: RetryMode,
    /// Maximum number of attempts, including the first. Absent = no attempt limit; 0 or 1 = a
    /// single attempt (no retries).
    pub max_attempts: Option<u32>,
    /// 0 = core default.
    pub max_retry_duration_ms: u64,
    pub backoff: Option<Backoff>,
    /// Opaque caller id of a custom "should this error be retried?" predicate. The core
    /// evaluates it through the client's retry callback (`GcsClientConfig.retry_cb`) and
    /// releases it through `release_cb` after its last use; 0 = none.
    pub predicate: u64,
}

#[derive(Deserialize, Default, Clone, Debug)]
#[serde(rename_all = "camelCase", default)]
pub(crate) struct CallOptions {
    pub retry: RetryOpts,
    /// The caller's own idempotency classification. It can only *narrow* the core's per-RPC
    /// rule (e.g. Go's ACL helpers are non-idempotent although their update request carries a
    /// metageneration precondition); it never makes an unsafe request retryable.
    pub idempotent: Option<bool>,
    /// Requester-pays / billing project for this call (overrides the client's quota project).
    pub user_project: Option<String>,
    pub attempt_timeout_ms: u64,
    /// Extra request headers / gRPC metadata for this call.
    pub headers: BTreeMap<String, String>,
    // ---- reads ----
    /// Read over gRPC (`BidiReadObject`, CloudPath) instead of the JSON API.
    pub grpc: bool,
    /// Return decompressed content for objects stored with `Content-Encoding: gzip` (JSON only:
    /// gRPC always returns stored bytes).
    pub decompress: bool,
    /// Validate the full-object CRC32C (default true).
    pub verify_crc32c: Option<bool>,
    /// Bytes buffered ahead of the reader (0 = core default).
    pub prefetch_bytes: usize,
    /// Zero-length read: return the object metadata and an immediate EOF.
    pub metadata_only: bool,
    // ---- writes ----
    /// Absent = default (16 MiB). 0 = no chunking: minimal buffering and no retries.
    /// N = uploads larger than N bytes are resumable with N-byte chunks.
    pub chunk_size: Option<usize>,
    /// Owns the caller's predicate from `bind` on (see `PredicateGuard`).
    #[serde(skip)]
    guard: Option<Arc<PredicateGuard>>,
}

/// Called by the core to evaluate a caller-supplied retry predicate (`RetryOpts.predicate`).
/// `status` strings are owned by the callee (free with `gcs_string_free`). Returns non-zero to
/// retry. Runs on a core thread; it must be fast and must not block.
pub type GcsRetryCb = unsafe extern "C" fn(predicate: usize, status: GcsStatus, attempt: u32) -> i32;

/// The core's reference to a caller predicate. Every retry policy built from these options
/// holds a clone; when the last one is dropped (the operation finished, failed or was
/// aborted) the predicate is released exactly once, so the host never frees it while the core
/// may still call it.
pub(crate) struct PredicateGuard {
    id: usize,
    cb: GcsRetryCb,
    release: Option<GcsReleaseCb>,
}

impl Drop for PredicateGuard {
    fn drop(&mut self) {
        if let Some(release) = self.release {
            unsafe { release(self.id) };
        }
    }
}

impl std::fmt::Debug for PredicateGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PredicateGuard").field("id", &self.id).finish_non_exhaustive()
    }
}

impl PredicateGuard {
    fn should_retry(&self, error: &GaxError, attempt: u32) -> bool {
        let st = Status::from_sdk(error).into_ffi();
        unsafe { (self.cb)(self.id, st, attempt) != 0 }
    }
}

impl CallOptions {
    pub(crate) fn parse(p: *const u8, len: usize) -> Result<Self, Status> {
        if p.is_null() || len == 0 {
            return Ok(Self::default());
        }
        let b = unsafe { std::slice::from_raw_parts(p, len) };
        serde_json::from_slice(b).map_err(|e| Status::invalid(format!("options: {e}")))
    }

    /// Takes ownership of the caller's predicate (if any). From here on the core releases it
    /// through `release_cb`, also if the operation fails synchronously.
    pub(crate) fn bind(&mut self, client: &GcsClient) {
        if self.retry.predicate != 0 {
            if let Some(cb) = client.retry_cb {
                self.guard =
                    Some(Arc::new(PredicateGuard { id: self.retry.predicate as usize, cb, release: client.release_cb }));
            }
        }
    }

    /// Effective idempotency: the core's per-RPC rule, narrowed by the caller's own
    /// classification and widened only by an explicit "always retry" policy.
    pub(crate) fn idempotent(&self, by_rule: bool) -> bool {
        self.retry.policy == RetryMode::Always || (by_rule && self.idempotent.unwrap_or(true))
    }

    fn attempt_limit(&self) -> Option<u32> {
        self.retry.max_attempts.map(|n| n.max(1))
    }

    /// The retry policy for this call, or `None` to keep the core default.
    pub(crate) fn retry_policy(&self, _client: &GcsClient) -> Result<Option<RetryPolicyArg>, Status> {
        let r = &self.retry;
        if r.policy == RetryMode::Never {
            return Ok(Some(NeverRetry.into()));
        }
        let (attempts, deadline) = (self.attempt_limit(), r.max_retry_duration_ms);
        if r.predicate != 0 {
            let guard = self.guard.clone().ok_or_else(|| Status::invalid("retry predicate given but the client has no retry_cb"))?;
            return Ok(Some(limited(CallerPredicate { guard }, attempts, deadline)));
        }
        if attempts.is_some() || deadline > 0 {
            return Ok(Some(limited(RetryableErrors, attempts, deadline)));
        }
        Ok(None)
    }

    /// Policy for resuming an interrupted download. The SDK resumes reads with its own
    /// (unlimited) policy unless told otherwise; the caller's retry settings must apply too.
    pub(crate) fn resume_policy(&self) -> Option<CallerResume> {
        let r = &self.retry;
        let configured = r.policy == RetryMode::Never || r.max_attempts.is_some() || self.guard.is_some();
        configured.then(|| CallerResume {
            never: r.policy == RetryMode::Never,
            max_attempts: self.attempt_limit(),
            guard: self.guard.clone(),
        })
    }

    pub(crate) fn backoff_policy(&self) -> Result<Option<BackoffPolicyArg>, Status> {
        let Some(b) = &self.retry.backoff else { return Ok(None) };
        let initial = if b.initial_ms == 0 { 1_000 } else { b.initial_ms };
        let max = if b.max_ms == 0 { 30_000 } else { b.max_ms }.max(initial);
        let mult = if b.multiplier > 1.0 { b.multiplier } else { 2.0 };
        let p = ExponentialBackoffBuilder::new()
            .with_initial_delay(Duration::from_millis(initial))
            .with_maximum_delay(Duration::from_millis(max))
            .with_scaling(mult)
            .build()
            .map_err(|e| Status::invalid(format!("backoff: {e}")))?;
        Ok(Some(p.into()))
    }

    pub(crate) fn quota_project(&self, client: &GcsClient) -> Option<String> {
        self.user_project.clone().filter(|s| !s.is_empty()).or_else(|| client.quota_project.clone())
    }

    pub(crate) fn header_pairs(&self) -> Result<Vec<(http::HeaderName, http::HeaderValue)>, Status> {
        self.headers
            .iter()
            .map(|(k, v)| {
                let k = http::HeaderName::from_bytes(k.as_bytes()).map_err(|e| Status::invalid(format!("header {k:?}: {e}")))?;
                let v = http::HeaderValue::from_str(v).map_err(|e| Status::invalid(format!("header value: {e}")))?;
                Ok((k, v))
            })
            .collect()
    }

    /// Request options for the generated (gRPC) control-plane request builders.
    pub(crate) fn request_options(&self, client: &GcsClient, idempotent_by_rule: bool) -> Result<RequestOptions, Status> {
        let mut o = RequestOptions::default();
        o.set_idempotency(self.idempotent(idempotent_by_rule));
        if let Some(p) = self.retry_policy(client)? {
            o.set_retry_policy(p);
        }
        if let Some(b) = self.backoff_policy()? {
            o.set_backoff_policy(b);
        }
        if let Some(q) = self.quota_project(client) {
            o.set_quota_project(q);
        }
        if let Some(ua) = &client.user_agent {
            o.set_user_agent(ua.clone());
        }
        if self.attempt_timeout_ms > 0 {
            o.set_attempt_timeout(Duration::from_millis(self.attempt_timeout_ms));
        }
        Ok(o)
    }
}

fn limited<P: RetryPolicy + 'static>(p: P, attempts: Option<u32>, deadline_ms: u64) -> RetryPolicyArg {
    let d = Duration::from_millis(deadline_ms);
    match (attempts, deadline_ms > 0) {
        (Some(n), true) => p.with_attempt_limit(n).with_time_limit(d).into(),
        (Some(n), false) => p.with_attempt_limit(n).into(),
        (None, true) => p.with_time_limit(d).into(),
        (None, false) => p.into(),
    }
}

/// A caller-supplied retry predicate (e.g. Go's `storage.WithErrorFunc`). Idempotency and the
/// caller's policy still gate retries; the predicate only classifies errors.
#[derive(Clone, Debug)]
struct CallerPredicate {
    guard: Arc<PredicateGuard>,
}

impl RetryPolicy for CallerPredicate {
    fn on_error(&self, state: &RetryState, error: GaxError) -> RetryResult {
        if !state.idempotent {
            return RetryResult::Permanent(error);
        }
        if self.guard.should_retry(&error, state.attempt_count) {
            RetryResult::Continue(error)
        } else {
            RetryResult::Permanent(error)
        }
    }
}

/// Download resumption under the caller's retry settings: `never` disables it, the attempt
/// limit bounds it, and a predicate classifies the errors; otherwise the SDK's recommended
/// classification applies.
#[derive(Debug)]
pub(crate) struct CallerResume {
    never: bool,
    max_attempts: Option<u32>,
    guard: Option<Arc<PredicateGuard>>,
}

impl ReadResumePolicy for CallerResume {
    fn on_error(&self, query: &ResumeQuery, error: GaxError) -> ResumeResult {
        if self.never {
            return ResumeResult::Permanent(error);
        }
        if self.max_attempts.is_some_and(|max| query.attempt_count >= max) {
            return ResumeResult::Exhausted(error);
        }
        match &self.guard {
            Some(g) if g.should_retry(&error, query.attempt_count) => ResumeResult::Continue(error),
            Some(_) => ResumeResult::Permanent(error),
            None => Recommended.on_error(query, error),
        }
    }
}

/// Applies retry/backoff/user-project/user-agent options to the data-path request builders
/// (`read_object`, `write_object`), which expose them as inherent methods.
macro_rules! apply_data_opts {
    ($req:expr, $opts:expr, $client:expr) => {{
        let o: &$crate::options::CallOptions = $opts;
        let client: &$crate::GcsClient = $client;
        let mut req = $req;
        if let Some(p) = o.retry_policy(client)? {
            req = req.with_retry_policy(p);
        }
        if let Some(b) = o.backoff_policy()? {
            req = req.with_backoff_policy(b);
        }
        if let Some(q) = o.quota_project(client) {
            req = req.with_quota_project(q);
        }
        if let Some(ua) = &client.user_agent {
            req = req.with_user_agent(ua.clone());
        }
        req
    }};
}
pub(crate) use apply_data_opts;
