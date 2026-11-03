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

//! Streaming object reads.
//!
//! Request: proto3-JSON `google.storage.v2.ReadObjectRequest` (bucket, object, generation,
//! readOffset/readLimit, preconditions, commonObjectRequestParams for CSEK). Options: retry,
//! `grpc` (BidiReadObject over CloudPath instead of the JSON API), `decompress` (JSON only),
//! `verifyCrc32c`, `prefetchBytes`, `metadataOnly`, `userProject`.
//!
//! A background task prefetches response chunks into a bounded queue; `gcs_reader_read` first
//! drains that queue *synchronously* (one plain FFI call, one memcpy), and only registers an
//! async waiter when the queue is empty. Small reads therefore cost one cheap FFI call instead
//! of a task spawn plus a cross-thread callback, while time-to-first-byte is unchanged.
//! Retries, read resumption, CRC32C validation and decompression happen inside the SDK.

use std::collections::VecDeque;
use std::ffi::{CString, c_char};
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex};

use bytes::{Buf, Bytes};
use google_cloud_storage::builder::storage::{OpenObject, ReadObject};
use google_cloud_storage::model;
use google_cloud_storage::model_ext::{KeyAes256, ReadRange};
use google_cloud_storage::read_object::ReadObjectResponse;
use tokio::sync::Notify;
use tokio::task::AbortHandle;

use crate::call::{bytes as ffi_bytes, to_json};
use crate::options::{CallOptions, apply_data_opts};
use crate::{
    Completion, GcsClient, GcsDoneCb, GcsStatus, GoBuf, Status, bucket_name, guard_rc, guard_void, lock, rt, spawn_op,
};

const DEFAULT_PREFETCH: usize = 8 << 20;
const WAKE_BYTES: usize = 256 << 10;

pub struct GcsReader {
    shared: Arc<RShared>,
    task: Option<AbortHandle>,
    metadata_json: CString,
}

struct RShared {
    st: Mutex<RState>,
    space: Notify,
    limit: usize,
    /// A pending read is woken only once this many bytes are queued (or at EOF/error), except
    /// for the first delivery (time-to-first-byte). Network-bound small reads then cost one
    /// async completion per `wake_bytes` instead of one per network chunk.
    wake_bytes: usize,
}

struct RState {
    queue: VecDeque<Bytes>,
    queued: usize,
    /// Some(Ok) = EOF, Some(Err) = sticky failure (including cancellation).
    end: Option<Result<(), Status>>,
    waiter: Option<(GoBuf, Completion)>,
    closed: bool,
    delivered: bool,
}

fn drain_into(st: &mut RState, buf: GoBuf) -> usize {
    // SAFETY: `buf` is pinned host memory owned by the in-progress read (see GoBuf).
    let dst = unsafe { std::slice::from_raw_parts_mut(buf.ptr, buf.len) };
    let mut n = 0;
    while n < dst.len() {
        let Some(front) = st.queue.front_mut() else { break };
        let take = front.len().min(dst.len() - n);
        dst[n..n + take].copy_from_slice(&front[..take]);
        front.advance(take);
        n += take;
        if front.is_empty() {
            st.queue.pop_front();
        }
    }
    st.queued -= n;
    n
}

/// Must be called with the lock held. Returns the completion to fire after unlocking.
fn take_ready_waiter(st: &mut RState, wake_bytes: usize) -> Option<(Completion, Status, u64)> {
    if st.waiter.is_none() {
        return None;
    }
    if st.end.is_none() && st.delivered && st.queued < wake_bytes {
        let want = st.waiter.as_ref().map(|(b, _)| b.len).unwrap_or(0);
        if st.queued < want {
            return None; // keep batching: neither the threshold nor the caller's buffer is full
        }
    }
    let (buf, done) = st.waiter.take()?;
    let n = drain_into(st, buf);
    if n > 0 {
        st.delivered = true;
        return Some((done, Status::ok(), n as u64));
    }
    match &st.end {
        Some(Ok(())) => Some((done, Status::ok(), 0)),
        Some(Err(s)) => Some((done, s.clone(), 0)),
        None => {
            st.waiter = Some((buf, done));
            None
        }
    }
}

async fn prefetch(shared: Arc<RShared>, mut resp: ReadObjectResponse) {
    let r = futures::FutureExt::catch_unwind(AssertUnwindSafe(prefetch_loop(&shared, &mut resp))).await;
    if let Err(p) = r {
        let done = {
            let mut st = lock(&shared.st);
            if st.end.is_none() {
                st.end = Some(Err(Status::from_panic(p)));
            }
            take_ready_waiter(&mut st, shared.wake_bytes)
        };
        if let Some((c, s, n)) = done {
            c.complete(s, n);
        }
    }
}

async fn prefetch_loop(shared: &RShared, resp: &mut ReadObjectResponse) {
    loop {
        // Backpressure: bounded read-ahead, but never stall a waiting reader.
        loop {
            {
                let st = lock(&shared.st);
                if st.closed || st.end.is_some() {
                    return;
                }
                if st.queued < shared.limit || st.waiter.is_some() {
                    break;
                }
            }
            shared.space.notified().await;
        }
        let item = resp.next().await;
        let done = {
            let mut st = lock(&shared.st);
            if st.closed {
                return;
            }
            match item {
                Some(Ok(chunk)) => {
                    if !chunk.is_empty() {
                        st.queued += chunk.len();
                        st.queue.push_back(chunk);
                    }
                }
                Some(Err(e)) => {
                    if st.end.is_none() {
                        st.end = Some(Err(Status::from_sdk(&e)));
                    }
                }
                None => {
                    if st.end.is_none() {
                        st.end = Some(Ok(()));
                    }
                }
            }
            take_ready_waiter(&mut st, shared.wake_bytes)
        };
        if let Some((c, s, n)) = done {
            c.complete(s, n);
        }
    }
}

fn new_shared(limit: usize, end: Option<Result<(), Status>>) -> Arc<RShared> {
    Arc::new(RShared {
        st: Mutex::new(RState { queue: VecDeque::new(), queued: 0, end, waiter: None, closed: false, delivered: false }),
        space: Notify::new(),
        limit,
        wake_bytes: WAKE_BYTES.min(limit),
    })
}

fn csek(p: &Option<model::CommonObjectRequestParams>) -> Result<Option<KeyAes256>, Status> {
    match p {
        Some(p) if !p.encryption_key_bytes.is_empty() => Ok(Some(
            KeyAes256::new(&p.encryption_key_bytes).map_err(|e| Status::invalid(format!("encryption key: {e}")))?,
        )),
        _ => Ok(None),
    }
}

/// Metadata envelope returned by `gcs_reader_metadata`:
/// `{"metadata": <v2 Object>, "readOffset": n, "remaining": n|-1, "decompressed": bool}`.
fn envelope(obj: &model::Object, read_offset: i64, remaining: i64, decompressed: bool) -> Result<CString, Status> {
    let js = format!(
        r#"{{"metadata":{},"readOffset":{read_offset},"remaining":{remaining},"decompressed":{decompressed}}}"#,
        to_json(obj)?
    );
    CString::new(js).map_err(|e| Status::err(e.to_string()))
}

/// The SDK range for a v2 `readOffset`/`readLimit` pair (a negative offset is a tail read).
fn read_range(offset: i64, limit: i64) -> ReadRange {
    if limit > 0 {
        ReadRange::segment(offset as u64, limit as u64)
    } else if offset < 0 {
        ReadRange::tail(offset.unsigned_abs())
    } else if offset > 0 {
        ReadRange::offset(offset as u64)
    } else {
        ReadRange::all()
    }
}

/// First byte and length served for a request, given the object size.
fn span(offset: i64, limit: i64, size: i64) -> (i64, i64) {
    if limit == 0 && offset < 0 {
        let n = offset.unsigned_abs().min(size.max(0) as u64) as i64;
        (size - n, n)
    } else if limit == 0 {
        (offset, (size - offset).max(0))
    } else {
        (offset, limit.min((size - offset).max(0)))
    }
}

/// How a read is opened.
enum Opener {
    /// Zero-length read: object metadata only (one GetObject, no media request).
    Metadata(model::GetObjectRequest),
    /// JSON API media download (`Storage::read_object`).
    Json(ReadObject),
    /// gRPC CloudPath (`BidiReadObject` through `Storage::open_object`).
    Grpc(OpenObject),
}

/// Builds the future that opens a read.
fn open_future(
    client: &GcsClient,
    req: &[u8],
    mut opts: CallOptions,
) -> Result<impl std::future::Future<Output = Result<u64, Status>> + Send + 'static, Status> {
    opts.bind(client);
    let r: model::ReadObjectRequest =
        serde_json::from_slice(req).map_err(|e| Status::invalid(format!("ReadObjectRequest: {e}")))?;
    if r.object.is_empty() || r.bucket.is_empty() {
        return Err(Status::invalid("bucket and object are required"));
    }
    let (offset, limit) = (r.read_offset, r.read_limit);
    if limit < 0 {
        return Err(Status::invalid(format!("readLimit {limit} must not be negative")));
    }
    if offset < 0 && limit > 0 {
        return Err(Status::invalid("a negative readOffset (tail read) cannot be combined with readLimit"));
    }
    let key = csek(&r.common_object_request_params)?;
    let bucket = bucket_name(&r.bucket);
    let client = client.clone();
    let limit_bytes = if opts.prefetch_bytes > 0 { opts.prefetch_bytes } else { DEFAULT_PREFETCH };

    // Generation, preconditions, CSEK, checksums, retry and resumption apply to every transport.
    macro_rules! common {
        ($b:expr) => {{
            let mut b = $b;
            if r.generation != 0 {
                b = b.set_generation(r.generation);
            }
            if let Some(v) = r.if_generation_match {
                b = b.set_if_generation_match(v);
            }
            if let Some(v) = r.if_generation_not_match {
                b = b.set_if_generation_not_match(v);
            }
            if let Some(v) = r.if_metageneration_match {
                b = b.set_if_metageneration_match(v);
            }
            if let Some(v) = r.if_metageneration_not_match {
                b = b.set_if_metageneration_not_match(v);
            }
            if let Some(k) = key {
                b = b.set_key(k);
            }
            if opts.verify_crc32c == Some(false) {
                b = b.compute_crc32c(false);
            }
            let b = apply_data_opts!(b, &opts, &client);
            // Resuming an interrupted download is governed by the caller's retry settings too.
            match opts.resume_policy() {
                Some(p) => b.with_read_resume_policy(p),
                None => b,
            }
        }};
    }

    let opener = if opts.metadata_only {
        // Zero-length reads: metadata only (one GetObject, no media request).
        let mut g = model::GetObjectRequest::default();
        g.bucket = bucket;
        g.object = r.object.clone();
        g.generation = r.generation;
        g.if_generation_match = r.if_generation_match;
        g.if_generation_not_match = r.if_generation_not_match;
        g.if_metageneration_match = r.if_metageneration_match;
        g.if_metageneration_not_match = r.if_metageneration_not_match;
        g.common_object_request_params = r.common_object_request_params.clone();
        Opener::Metadata(g)
    } else if opts.grpc {
        // gRPC (CloudPath): BidiReadObject; the range is sent with the open (`send_and_read`).
        Opener::Grpc(common!(client.storage.open_object(bucket, r.object.clone())))
    } else {
        // The v2 API returns stored bytes; decompressive transcoding is a caller choice (Go's
        // Reader decompresses by default unless ReadCompressed(true)).
        let rb = client
            .storage
            .read_object(bucket, r.object.clone())
            .set_read_range(read_range(offset, limit))
            .with_automatic_decompression(opts.decompress);
        Opener::Json(common!(rb))
    };
    let decompress = opts.decompress;
    let (bucket_s, object_s) = (r.bucket.clone(), r.object.clone());

    Ok(async move {
        let (resp, metadata_json) = match opener {
            Opener::Metadata(g) => {
                let ro = opts.request_options(&client, true)?;
                let obj =
                    client.control.get_object().with_request(g).with_options(ro).send().await.map_err(|e| Status::from_sdk(&e))?;
                let reader = GcsReader {
                    shared: new_shared(limit_bytes, Some(Ok(()))),
                    task: None,
                    metadata_json: envelope(&obj, 0, 0, false)?,
                };
                return Ok(Box::into_raw(Box::new(reader)) as u64);
            }
            Opener::Grpc(ob) => {
                let (descriptor, resp) = ob.send_and_read(read_range(offset, limit)).await.map_err(|e| Status::from_sdk(&e))?;
                // The descriptor carries the full object resource; gRPC never transcodes.
                // Dropping it is fine: the range reader keeps the stream's worker alive.
                let obj = descriptor.object();
                let (start, remain) = span(offset, limit, obj.size);
                (resp, envelope(&obj, start, remain, false)?)
            }
            Opener::Json(rb) => {
                let resp = rb.send().await.map_err(|e| Status::from_sdk(&e))?;
                let hl = resp.object();
                let (start, mut remain) = span(offset, limit, hl.size);
                // With decompressive transcoding GCS ignores the Range header and serves the
                // whole decompressed object (verified against the native client), so a
                // requested range does not change this.
                let decompressed = decompress && hl.content_encoding == "gzip";
                if decompressed {
                    remain = -1;
                }
                let mut obj = model::Object::default();
                obj.bucket = bucket_name(&bucket_s);
                obj.name = object_s;
                obj.generation = hl.generation;
                obj.metageneration = hl.metageneration;
                obj.size = hl.size;
                obj.content_encoding = hl.content_encoding.clone();
                obj.content_type = hl.content_type.clone();
                obj.content_language = hl.content_language.clone();
                obj.content_disposition = hl.content_disposition.clone();
                obj.storage_class = hl.storage_class.clone();
                obj.etag = hl.etag.clone();
                obj.metadata = hl.metadata.clone().into_iter().collect();
                obj.checksums = hl.checksums.clone();
                let metadata_json = envelope(&obj, start, remain, decompressed)?;
                (resp, metadata_json)
            }
        };
        let shared = new_shared(limit_bytes, None);
        let task = rt().spawn(prefetch(shared.clone(), resp));
        let reader = GcsReader { shared, task: Some(task.abort_handle()), metadata_json };
        Ok(Box::into_raw(Box::new(reader)) as u64)
    })
}

/// Starts a read. Pending (returns 1): the callback fires once with `n` = the `GcsReader*`
/// (convert in C) or an error. `*out_op` receives an id for `gcs_op_cancel`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcs_read_open(
    c: *const GcsClient,
    req: *const u8,
    req_len: usize,
    opts: *const u8,
    opts_len: usize,
    cb: GcsDoneCb,
    user: usize,
    out_op: *mut u64,
    out_status: *mut GcsStatus,
) -> i32 {
    guard_rc(out_status, || {
        if c.is_null() {
            return Err(Status::invalid("null client"));
        }
        let fut = open_future(unsafe { &*c }, unsafe { ffi_bytes(req, req_len) }, CallOptions::parse(opts, opts_len)?)?;
        let id = spawn_op(fut, Completion::new(cb, user));
        if !out_op.is_null() {
            unsafe { *out_op = id };
        }
        Ok(1)
    })
}

/// Metadata captured at open (see `envelope`). Borrowed; valid until `gcs_reader_close`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcs_reader_metadata(r: *const GcsReader) -> *const c_char {
    if r.is_null() {
        return std::ptr::null();
    }
    unsafe { &*r }.metadata_json.as_ptr()
}

/// Drains queued data into `buf`, or registers `done` as the pending waiter.
fn read_impl(r: &GcsReader, buf: GoBuf, done: Completion, out_n: &mut usize) -> Result<i32, Status> {
    let mut st = lock(&r.shared.st);
    if st.closed {
        return Err(Status::invalid("storage: reader is closed"));
    }
    if st.waiter.is_some() {
        return Err(Status::invalid("concurrent Read calls on one reader"));
    }
    let n = drain_into(&mut st, buf);
    if n > 0 {
        st.delivered = true;
        drop(st);
        r.shared.space.notify_one();
        *out_n = n;
        return Ok(0);
    }
    match &st.end {
        Some(Ok(())) => {
            *out_n = 0;
            return Ok(0);
        }
        Some(Err(s)) => return Err(s.clone()),
        None => {}
    }
    st.waiter = Some((buf, done));
    drop(st);
    r.shared.space.notify_one();
    Ok(1)
}

/// io.Reader semantics. Returns 0 with `*out_n` bytes copied (`*out_n == 0` => EOF) when data
/// (or EOF) is available now; 1 when pending (the callback fires once with `n` bytes, 0 = EOF,
/// and `buf` must stay pinned until then); -1 on failure (`*out_status`). One read at a time.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcs_reader_read(
    r: *const GcsReader,
    buf: *mut u8,
    cap: usize,
    cb: GcsDoneCb,
    user: usize,
    out_n: *mut usize,
    out_status: *mut GcsStatus,
) -> i32 {
    guard_rc(out_status, || {
        if r.is_null() || buf.is_null() || cap == 0 || out_n.is_null() {
            return Err(Status::invalid("null argument"));
        }
        read_impl(unsafe { &*r }, GoBuf { ptr: buf, len: cap }, Completion::new(cb, user), unsafe { &mut *out_n })
    })
}

/// Fails the reader (sticky): a pending read completes with CANCELLED, later reads fail.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcs_reader_cancel(r: *const GcsReader) {
    guard_void(|| {
        if r.is_null() {
            return;
        }
        let r = unsafe { &*r };
        let waiter = {
            let mut st = lock(&r.shared.st);
            st.end = Some(Err(Status::cancelled("context canceled")));
            st.queue.clear();
            st.queued = 0;
            st.waiter.take()
        };
        if let Some(t) = &r.task {
            t.abort();
        }
        r.shared.space.notify_one();
        if let Some((_, done)) = waiter {
            done.complete(Status::cancelled("context canceled"), 0);
        }
    });
}

/// Closes the reader. Safe to call while a read is pending on another thread: the pending
/// read completes with CANCELLED and no memory is freed while still in use.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcs_reader_close(r: *mut GcsReader) {
    guard_void(|| {
        if r.is_null() {
            return;
        }
        let r = unsafe { Box::from_raw(r) };
        let waiter = {
            let mut st = lock(&r.shared.st);
            st.closed = true;
            st.queue.clear();
            st.queued = 0;
            st.waiter.take()
        };
        if let Some(t) = &r.task {
            t.abort();
        }
        r.shared.space.notify_one();
        if let Some((_, done)) = waiter {
            done.complete(Status::cancelled("storage: reader closed during Read"), 0);
        }
    });
}
