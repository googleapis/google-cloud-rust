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

//! gcs_core v3 — one C ABI over the `google-cloud-storage` crate that serves *every* Cloud
//! Storage operation for the Go SDK. Nothing in the ABI is Go-specific: it is designed so other
//! language SDKs can bind to the same core.
//!
//! The ABI is deliberately generic so that it does not change when the storage API grows:
//!
//! * `gcs_call(method, request, options)` serves every unary RPC of `google.storage.v2.Storage`
//!   (bucket and object metadata, list, compose, rewrite, move, restore, IAM). Requests and
//!   responses are proto3-JSON encodings of the v2 messages, which every language can produce
//!   from the protos it already ships.
//! * `gcs_read_*` / `gcs_writer_*` stream object data (requests: proto3-JSON
//!   `ReadObjectRequest` / `StartResumableWriteRequest`).
//! * The options JSON carries only the *caller's* policy knobs (see `options.rs`).
//!
//! All storage *logic* runs here, once, for every language: retries and backoff, per-RPC
//! idempotency rules, resumable vs single-shot uploads, checksums, read resumption,
//! decompression, credential handling and error classification.
//!
//! Safety contract for embedding in a long-running host process (each rule fixes a defect
//! found in the original POC):
//!
//! * **No unwinding into C.** Every exported function runs under `catch_unwind`, including the
//!   synchronous prologue of async entry points. Background tasks are wrapped too, so a panic
//!   always becomes an error completion instead of a lost callback (= a caller hung forever).
//! * **No use-after-free.** Readers and writers are `Arc`-backed. The FFI owns one reference;
//!   each in-flight operation owns its own clone, so `close/free` racing with an operation can
//!   never free memory that a task is still using.
//! * **Exactly-once completions.** Every async operation reports exactly one callback
//!   `GcsDoneCb(user, status, n)`. `user` is an opaque `uintptr_t` (a `cgo.Handle` in Go); it is
//!   never a pointer, so the host never fabricates a pointer from an integer.
//! * **Borrowed host memory is released at the callback.** Read destinations / write sources
//!   handed to an async operation are only touched until that operation's callback fires.
//! * **Uploads finalize only on an explicit close.** Abort, cancellation, dropping the handle
//!   or the upload task dying never finalize a partially written object.
//! * **Errors keep their structure.** gRPC code *and* HTTP status (the core fills in whichever
//!   the transport did not provide), service message and headers cross the boundary, so every
//!   language can rebuild its idiomatic error types regardless of the transport used.

use std::collections::HashMap;
use std::ffi::{CStr, CString, c_char, c_void};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard, OnceLock};

use google_cloud_storage::client::{Storage, StorageControl};
use tokio::runtime::Runtime;
use tokio::task::AbortHandle;

mod call;
mod creds;
mod json_api;
mod options;
mod policy;
mod reader;
mod writer;

pub use call::*;
pub use options::GcsRetryCb;
pub use reader::*;
pub use writer::*;

// ---------------------------------------------------------------------------------------------
// Status
// ---------------------------------------------------------------------------------------------

pub const GCS_OK: i32 = 0;
pub const GCS_ERR: i32 = 1;
pub const GCS_PANIC: i32 = 2;
pub const GCS_INVALID: i32 = 3;
pub const GCS_CANCELLED: i32 = 4;

/// Status crossing the boundary. Strings are owned by Rust; free with `gcs_string_free`.
#[repr(C)]
pub struct GcsStatus {
    pub code: i32,
    /// HTTP status of the failure (derived from `rpc_code` for gRPC calls), or 0.
    pub http_code: i32,
    /// `google.rpc.Code` of the failure (derived from `http_code` for HTTP calls), or -1.
    pub rpc_code: i32,
    /// Human readable message (the service message when available). NULL on OK.
    pub message: *mut c_char,
    /// Response headers as "Name: value\n" lines, or NULL.
    pub headers: *mut c_char,
}

#[derive(Clone, Debug)]
pub(crate) struct Status {
    pub code: i32,
    pub http_code: i32,
    pub rpc_code: i32,
    pub message: String,
    pub headers: String,
}

/// Canonical google.rpc.Code -> HTTP mapping, with Cloud Storage's 412 for preconditions.
pub(crate) fn http_for_rpc(code: i32) -> i32 {
    match code {
        1 => 499,
        2 | 13 | 15 => 500,
        3 => 400,
        4 => 504,
        5 => 404,
        6 | 10 => 409,
        7 => 403,
        8 => 429,
        9 => 412,
        11 => 416,
        12 => 501,
        14 => 503,
        16 => 401,
        _ => 0,
    }
}

pub(crate) fn rpc_for_http(code: i32) -> i32 {
    match code {
        304 | 412 => 9,
        400 => 3,
        401 => 16,
        403 => 7,
        404 => 5,
        409 => 6,
        416 => 11,
        429 => 8,
        499 => 1,
        500 => 13,
        501 => 12,
        502 | 503 => 14,
        504 => 4,
        c if (500..600).contains(&c) => 14,
        _ => -1,
    }
}

impl Status {
    pub(crate) fn ok() -> Self {
        Self::new(GCS_OK, String::new())
    }
    pub(crate) fn new(code: i32, message: impl Into<String>) -> Self {
        Status { code, http_code: 0, rpc_code: -1, message: message.into(), headers: String::new() }
    }
    pub(crate) fn err(message: impl Into<String>) -> Self {
        Self::new(GCS_ERR, message)
    }
    pub(crate) fn invalid(message: impl Into<String>) -> Self {
        Self::new(GCS_INVALID, message)
    }
    pub(crate) fn cancelled(message: impl Into<String>) -> Self {
        Self::new(GCS_CANCELLED, message)
    }
    pub(crate) fn unimplemented(message: impl Into<String>) -> Self {
        let mut s = Self::new(GCS_INVALID, message);
        s.rpc_code = 12;
        s.http_code = 501;
        s
    }
    pub(crate) fn from_panic(p: Box<dyn std::any::Any + Send>) -> Self {
        let msg = p
            .downcast_ref::<&str>()
            .map(|s| s.to_string())
            .or_else(|| p.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "unknown panic".into());
        Self::new(GCS_PANIC, format!("gcs_core: rust panic: {msg}"))
    }
    /// Keeps the structure every language needs to rebuild its idiomatic error type.
    pub(crate) fn from_sdk(e: &google_cloud_storage::Error) -> Self {
        // Retry exhaustion (and some wrappers) carry the codes on a nested error: report the
        // codes of the error that actually failed.
        let mut inner = e;
        let mut cur: Option<&(dyn std::error::Error + 'static)> = std::error::Error::source(e);
        while inner.status().is_none() && inner.http_status_code().is_none() {
            let Some(c) = cur else { break };
            if let Some(x) = c.downcast_ref::<google_cloud_storage::Error>() {
                inner = x;
            }
            cur = c.source();
        }
        let service_msg = inner.status().map(|s| s.message.clone()).filter(|m| !m.is_empty());
        let mut s = Self::err(service_msg.unwrap_or_else(|| e.to_string()));
        s.http_code = inner.http_status_code().map(i32::from).unwrap_or(0);
        s.rpc_code = inner.status().map(|st| st.code as i32).unwrap_or(-1);
        if s.rpc_code < 0 && s.http_code == 0 {
            // Failures without a service response still get a code, so callers (and retry
            // predicates) can classify them like the other transports do.
            if e.to_string().contains("checksum mismatch") {
                s.rpc_code = 15; // DATA_LOSS
            } else if e.is_timeout() {
                s.rpc_code = 4; // DEADLINE_EXCEEDED
            } else if e.is_io() || e.is_transport() {
                s.rpc_code = 14; // UNAVAILABLE
            }
        }
        if s.http_code == 0 && s.rpc_code >= 0 {
            s.http_code = http_for_rpc(s.rpc_code);
        }
        // JSON API errors often carry no gRPC code (UNKNOWN): the HTTP status is more specific.
        if (s.rpc_code < 0 || s.rpc_code == 2) && s.http_code != 0 {
            let derived = rpc_for_http(s.http_code);
            if derived >= 0 {
                s.rpc_code = derived;
            }
        }
        if let Some(h) = inner.http_headers() {
            for (k, v) in h {
                if let Ok(v) = v.to_str() {
                    s.headers.push_str(k.as_str());
                    s.headers.push_str(": ");
                    s.headers.push_str(v);
                    s.headers.push('\n');
                }
            }
        }
        s
    }
    pub(crate) fn into_ffi(self) -> GcsStatus {
        let message = if self.code == GCS_OK { std::ptr::null_mut() } else { to_c(self.message) };
        let headers = if self.headers.is_empty() { std::ptr::null_mut() } else { to_c(self.headers) };
        GcsStatus { code: self.code, http_code: self.http_code, rpc_code: self.rpc_code, message, headers }
    }
}

pub(crate) fn to_c(s: String) -> *mut c_char {
    CString::new(s.replace('\0', " ")).map(CString::into_raw).unwrap_or(std::ptr::null_mut())
}

pub(crate) unsafe fn set_status(out: *mut GcsStatus, s: Status) {
    if !out.is_null() {
        unsafe { out.write(s.into_ffi()) };
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcs_string_free(s: *mut c_char) {
    if !s.is_null() {
        drop(unsafe { CString::from_raw(s) });
    }
}

// ---------------------------------------------------------------------------------------------
// Panic guards
// ---------------------------------------------------------------------------------------------

/// For entry points that return a `GcsStatus` by value.
pub(crate) fn guard_status<F: FnOnce() -> Result<(), Status>>(f: F) -> GcsStatus {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(())) => Status::ok().into_ffi(),
        Ok(Err(s)) => s.into_ffi(),
        Err(p) => Status::from_panic(p).into_ffi(),
    }
}

/// For entry points that return an `i32` code and report failures through `out_status`.
/// Convention: `0` = completed synchronously, `1` = pending (callback will fire exactly once),
/// `-1` = failed synchronously (`*out_status` set, no callback).
pub(crate) fn guard_rc<F: FnOnce() -> Result<i32, Status>>(out_status: *mut GcsStatus, f: F) -> i32 {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(rc)) => rc,
        Ok(Err(s)) => {
            unsafe { set_status(out_status, s) };
            -1
        }
        Err(p) => {
            unsafe { set_status(out_status, Status::from_panic(p)) };
            -1
        }
    }
}

/// For `void` entry points (free/close/abort): swallow panics, never unwind into C.
pub(crate) fn guard_void<F: FnOnce()>(f: F) {
    let _ = catch_unwind(AssertUnwindSafe(f));
}

/// Mutex lock that ignores poisoning (a panicking task must not wedge every later call).
pub(crate) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

// ---------------------------------------------------------------------------------------------
// Completions
// ---------------------------------------------------------------------------------------------

/// Completion callback. `user` is an opaque value supplied by the caller (a cgo.Handle).
/// `status.message`/`status.headers` are owned by the callee (free with `gcs_string_free`).
pub type GcsDoneCb = unsafe extern "C" fn(user: usize, status: GcsStatus, n: u64);

/// Where an async operation reports its single result.
#[derive(Clone, Copy)]
pub(crate) struct Completion {
    cb: GcsDoneCb,
    user: usize,
}

impl Completion {
    pub(crate) fn new(cb: GcsDoneCb, user: usize) -> Self {
        Completion { cb, user }
    }
    /// Consumes the completion: the type system makes "exactly once" hard to get wrong.
    pub(crate) fn complete(self, st: Status, n: u64) {
        unsafe { (self.cb)(self.user, st.into_ffi(), n) }
    }
}

/// A region of borrowed host memory lent to an async operation until its callback fires.
#[derive(Clone, Copy)]
pub(crate) struct GoBuf {
    pub ptr: *mut u8,
    pub len: usize,
}
// SAFETY: the host pins the memory (Go: runtime.Pinner) and does not touch it until the
// completion for the operation that owns it has fired.
unsafe impl Send for GoBuf {}

impl GoBuf {
    /// SAFETY: only valid until the owning operation's completion fires (see above).
    pub(crate) unsafe fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

// ---------------------------------------------------------------------------------------------
// Runtime
// ---------------------------------------------------------------------------------------------

static RT: OnceLock<Runtime> = OnceLock::new();
static RT_WORKERS: AtomicUsize = AtomicUsize::new(0);

/// Installs the process-wide rustls crypto provider selected at build time.
fn install_crypto_provider() {
    #[cfg(feature = "crypto-ring")]
    {
        // Ignore the error: a provider may already be installed (e.g. by another Rust library).
        let _ = rustls::crypto::ring::default_provider().install_default();
    }
}

/// Rust binaries ignore SIGPIPE from `main`; a static library linked into another runtime does
/// not get that. TLS and HTTP/1 writes use `writev`, which (unlike `send(MSG_NOSIGNAL)`) raises
/// SIGPIPE when the peer reset the connection, and a SIGPIPE on a thread the host runtime does
/// not own is fatal in Go hosts (the Go runtime applies the default action: the process exits).
/// Blocking it on every core thread turns it into an EPIPE error, handled like any I/O error,
/// without changing the host's process-wide signal disposition.
fn block_sigpipe() {
    #[cfg(unix)]
    // SAFETY: plain libc calls on a local, zero-initialized signal set; affects this thread only.
    unsafe {
        let mut set: libc::sigset_t = std::mem::zeroed();
        libc::sigemptyset(&mut set);
        libc::sigaddset(&mut set, libc::SIGPIPE);
        libc::pthread_sigmask(libc::SIG_BLOCK, &set, std::ptr::null_mut());
    }
}

pub(crate) fn rt() -> &'static Runtime {
    RT.get_or_init(|| {
        install_crypto_provider();
        let mut b = tokio::runtime::Builder::new_multi_thread();
        // All network I/O of the core runs on these threads (workers and blocking pool).
        b.enable_all().thread_name("gcs-core").on_thread_start(block_sigpipe);
        let n = RT_WORKERS.load(Ordering::SeqCst);
        if n > 0 {
            b.worker_threads(n);
        }
        b.build().expect("failed to start the tokio runtime")
    })
}

/// Sets the number of Tokio worker threads. Only effective before the first client is created;
/// returns 1 when applied and 0 when the runtime already exists. 0 = Tokio default (#CPUs).
#[unsafe(no_mangle)]
pub extern "C" fn gcs_runtime_configure(worker_threads: u32) -> i32 {
    let mut applied = 0;
    guard_void(|| {
        RT_WORKERS.store(worker_threads as usize, Ordering::SeqCst);
        applied = if RT.get().is_none() { 1 } else { 0 };
    });
    applied
}

/// ABI version, bumped on any incompatible change. Bindings check it at load time.
#[unsafe(no_mangle)]
pub extern "C" fn gcs_abi_version() -> u32 {
    3
}

// ---------------------------------------------------------------------------------------------
// Cancellable one-shot operations (calls, reader opens). Long-lived handles have own cancel.
// ---------------------------------------------------------------------------------------------

static NEXT_OP: AtomicU64 = AtomicU64::new(1);
static OPS: OnceLock<Mutex<HashMap<u64, AbortHandle>>> = OnceLock::new();

fn ops() -> &'static Mutex<HashMap<u64, AbortHandle>> {
    OPS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Runs `fut` on the runtime and completes `done` exactly once, also when the task is aborted
/// through `gcs_op_cancel` or panics. Returns the op id (never 0).
pub(crate) fn spawn_op<F>(fut: F, done: Completion) -> u64
where
    F: std::future::Future<Output = Result<u64, Status>> + Send + 'static,
{
    let id = NEXT_OP.fetch_add(1, Ordering::Relaxed);
    let jh = rt().spawn(futures::FutureExt::catch_unwind(AssertUnwindSafe(fut)));
    lock(ops()).insert(id, jh.abort_handle());
    rt().spawn(async move {
        let r = jh.await;
        lock(ops()).remove(&id);
        match r {
            Ok(Ok(Ok(n))) => done.complete(Status::ok(), n),
            Ok(Ok(Err(s))) => done.complete(s, 0),
            Ok(Err(p)) => done.complete(Status::from_panic(p), 0),
            Err(je) if je.is_cancelled() => done.complete(Status::cancelled("operation cancelled"), 0),
            Err(je) => done.complete(Status::new(GCS_PANIC, format!("gcs_core: task failed: {je}")), 0),
        }
    });
    id
}

/// Cancels an operation started by `spawn_op`. The completion still fires (code CANCELLED, or
/// the real result if the operation already finished).
#[unsafe(no_mangle)]
pub extern "C" fn gcs_op_cancel(op_id: u64) {
    guard_void(|| {
        if let Some(h) = lock(ops()).get(&op_id) {
            h.abort();
        }
    });
}

// ---------------------------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------------------------

pub const GCS_AUTH_ADC: i32 = 0;
pub const GCS_AUTH_ANONYMOUS: i32 = 1;
pub const GCS_AUTH_JSON: i32 = 2;
pub const GCS_AUTH_TOKEN_CALLBACK: i32 = 3;

/// Token callback used for `GCS_AUTH_TOKEN_CALLBACK`: lets the core use *any* host credential
/// (Go: option.WithCredentials*, WithTokenSource, impersonation, WIF, ...).
/// Returns 0 on success with `*out_token` (malloc'ed C string, freed by Rust with `free`) and
/// `*out_expiry_unix` (0 = unknown); non-zero on failure with `*out_err` (malloc'ed or NULL).
pub type GcsTokenCb =
    unsafe extern "C" fn(user: usize, out_token: *mut *mut c_char, out_expiry_unix: *mut i64, out_err: *mut *mut c_char) -> i32;

/// Called exactly once when the core drops its last reference to a host value it was handed
/// (the token source in `token_user`, a retry predicate id). The host frees the value then.
pub type GcsReleaseCb = unsafe extern "C" fn(user: usize);

#[repr(C)]
pub struct GcsClientConfig {
    /// JSON API / data path endpoint, e.g. "https://storage.googleapis.com" or
    /// "http://localhost:9000"; NULL = default.
    pub endpoint: *const c_char,
    /// gRPC endpoint for the control plane, e.g. "http://localhost:8888"; NULL = default.
    pub grpc_endpoint: *const c_char,
    pub universe_domain: *const c_char,
    pub credentials_json: *const c_char,
    pub quota_project: *const c_char,
    /// Prepended to the core's user agent (e.g. "gcloud-golang/1.70 rust-core").
    pub user_agent: *const c_char,
    /// Extra headers for every data-path request, as "Name: value\n" lines.
    pub custom_headers: *const c_char,
    pub auth_mode: i32,
    pub token_cb: Option<GcsTokenCb>,
    pub token_user: usize,
    /// Evaluates caller-supplied retry predicates (`options.retry.predicate`); may be NULL.
    pub retry_cb: Option<GcsRetryCb>,
    /// Releases host values (token source, predicates) after the core's last use; may be NULL.
    pub release_cb: Option<GcsReleaseCb>,
}

/// A client: the data-path (`Storage`) and control-plane (`StorageControl`) clients of the Rust
/// SDK, sharing one credential. Cheap to clone (everything inside is reference counted).
#[derive(Clone)]
pub struct GcsClient {
    pub(crate) storage: Storage,
    pub(crate) control: StorageControl,
    pub(crate) user_agent: Option<String>,
    pub(crate) quota_project: Option<String>,
    pub(crate) retry_cb: Option<GcsRetryCb>,
    pub(crate) release_cb: Option<GcsReleaseCb>,
    /// For JSON API calls the SDK does not offer (json_api.rs): same credential, endpoint and
    /// default headers as the data path.
    pub(crate) credentials: google_cloud_auth::credentials::Credentials,
    pub(crate) http: reqwest::Client,
    pub(crate) json_endpoint: String,
}

pub(crate) unsafe fn opt_str(p: *const c_char) -> Result<Option<String>, Status> {
    if p.is_null() {
        return Ok(None);
    }
    let s = unsafe { CStr::from_ptr(p) }.to_str().map_err(|e| Status::invalid(format!("invalid utf-8: {e}")))?;
    Ok(if s.is_empty() { None } else { Some(s.to_string()) })
}

pub(crate) unsafe fn req_str(p: *const c_char, what: &str) -> Result<String, Status> {
    unsafe { opt_str(p) }?.ok_or_else(|| Status::invalid(format!("{what} is required")))
}

pub(crate) fn bucket_name(b: &str) -> String {
    if b.starts_with("projects/") { b.to_string() } else { format!("projects/_/buckets/{b}") }
}

fn parse_headers(s: &str) -> Result<Vec<(http::HeaderName, http::HeaderValue)>, Status> {
    let mut out = Vec::new();
    for line in s.lines().filter(|l| !l.trim().is_empty()) {
        let (k, v) = line.split_once(':').ok_or_else(|| Status::invalid(format!("bad header line {line:?}")))?;
        let k = http::HeaderName::from_bytes(k.trim().as_bytes()).map_err(|e| Status::invalid(e.to_string()))?;
        let v = http::HeaderValue::from_str(v.trim()).map_err(|e| Status::invalid(e.to_string()))?;
        out.push((k, v));
    }
    Ok(out)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcs_client_new(cfg: *const GcsClientConfig, out: *mut *mut GcsClient) -> GcsStatus {
    guard_status(|| {
        if cfg.is_null() || out.is_null() {
            return Err(Status::invalid("null argument"));
        }
        let cfg = unsafe { &*cfg };
        let endpoint = unsafe { opt_str(cfg.endpoint) }?;
        let grpc_endpoint = unsafe { opt_str(cfg.grpc_endpoint) }?;
        let universe = unsafe { opt_str(cfg.universe_domain) }?;
        let quota_project = unsafe { opt_str(cfg.quota_project) }?;
        let user_agent = unsafe { opt_str(cfg.user_agent) }?;
        let headers = match unsafe { opt_str(cfg.custom_headers) }? {
            Some(h) => parse_headers(&h)?,
            None => Vec::new(),
        };
        // One credential shared by the data-path and control-plane clients (one token cache).
        let credentials = match cfg.auth_mode {
            GCS_AUTH_ADC => google_cloud_auth::credentials::Builder::default()
                .build()
                .map_err(|e| Status::err(format!("application default credentials: {e}")))?,
            GCS_AUTH_ANONYMOUS => google_cloud_auth::credentials::anonymous::Builder::new().build(),
            GCS_AUTH_JSON => {
                let js = unsafe { req_str(cfg.credentials_json, "credentials_json") }?;
                creds::from_json(&js)?
            }
            GCS_AUTH_TOKEN_CALLBACK => {
                let cb = cfg.token_cb.ok_or_else(|| Status::invalid("token_cb is required"))?;
                creds::go_token_credentials(cb, cfg.token_user, cfg.release_cb)
            }
            m => return Err(Status::invalid(format!("unknown auth_mode {m}"))),
        };
        // JSON API calls made by the core itself (json_api.rs) use the same credential,
        // endpoint and client-level headers as the SDK's data path.
        let json_credentials = credentials.clone();
        let json_endpoint = match (&endpoint, &universe) {
            (Some(e), _) => e.trim_end_matches('/').to_string(),
            (None, Some(u)) => format!("https://storage.{u}"),
            (None, None) => "https://storage.googleapis.com".to_string(),
        };
        let build = async move {
            // Built on the runtime, i.e. after `rt()` installed the rustls crypto provider (the
            // dependency graph enables reqwest's `rustls-no-provider`).
            let http = reqwest::Client::builder()
                .default_headers(headers.iter().cloned().collect())
                .build()
                .map_err(|e| Status::err(format!("building http client: {e}")))?;
            let mut b = Storage::builder().with_credentials(credentials.clone());
            if let Some(e) = endpoint {
                b = b.with_endpoint(e);
            }
            if let Some(u) = universe.clone() {
                b = b.with_universe_domain(u);
            }
            for (k, v) in headers {
                b = b.with_custom_header(k, v);
            }
            let storage = b.build().await.map_err(|e| Status::err(format!("building storage client: {e}")))?;

            let mut cb = StorageControl::builder().with_credentials(credentials);
            if let Some(e) = grpc_endpoint {
                cb = cb.with_endpoint(e);
            }
            if let Some(u) = universe {
                cb = cb.with_universe_domain(u);
            }
            let control = cb.build().await.map_err(|e| Status::err(format!("building control client: {e}")))?;
            Ok::<_, Status>((storage, control, http))
        };
        // Build on a core thread, so any I/O (e.g. a metadata-server lookup) runs with SIGPIPE
        // blocked (see block_sigpipe), never on the host's thread. block_on panics on a runtime
        // thread (e.g. a client created inside a retry callback): wait on a helper thread then.
        let task = rt().spawn(build);
        let wait = move || -> Result<_, Status> {
            rt().block_on(task).map_err(|e| Status::err(format!("building the client: {e}")))?
        };
        let (storage, control, http) = if tokio::runtime::Handle::try_current().is_ok() {
            std::thread::scope(|sc| sc.spawn(wait).join()).map_err(Status::from_panic)??
        } else {
            wait()?
        };
        let client = GcsClient {
            storage,
            control,
            user_agent,
            quota_project,
            retry_cb: cfg.retry_cb,
            release_cb: cfg.release_cb,
            credentials: json_credentials,
            http,
            json_endpoint,
        };
        unsafe { *out = Box::into_raw(Box::new(client)) };
        Ok(())
    })
}

/// Frees the client. In-flight calls, readers and writers keep working: they hold their own
/// clones of the underlying (reference-counted) transports.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcs_client_free(c: *mut GcsClient) {
    guard_void(|| {
        if !c.is_null() {
            drop(unsafe { Box::from_raw(c) });
        }
    });
}

/// Frees memory allocated by the host with C malloc (e.g. Go's C.CString) and handed to Rust.
pub(crate) unsafe fn c_free(p: *mut c_char) {
    unsafe extern "C" {
        fn free(p: *mut c_void);
    }
    if !p.is_null() {
        unsafe { free(p.cast()) };
    }
}

/// Cheap call used to measure raw host->Rust crossing cost.
#[unsafe(no_mangle)]
pub extern "C" fn gcs_noop(x: u64) -> u64 {
    x.wrapping_add(1)
}

/// Completes immediately from a runtime thread: measures the cost of one asynchronous
/// completion (task spawn + Rust->host callback on a foreign thread + wake-up).
#[unsafe(no_mangle)]
pub extern "C" fn gcs_async_noop(cb: GcsDoneCb, user: usize) -> i32 {
    let mut rc = -1;
    guard_void(|| {
        let done = Completion::new(cb, user);
        rt().spawn(async move { done.complete(Status::ok(), 1) });
        rc = 1;
    });
    rc
}
