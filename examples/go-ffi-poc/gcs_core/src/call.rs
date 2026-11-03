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

//! `gcs_call`: one entry point for every unary RPC of `google.storage.v2.Storage`.
//!
//! Requests and responses are proto3-JSON encodings of the v2 messages (`GetObjectRequest` ->
//! `Object`, `ListObjectsRequest` -> `ListObjectsResponse`, ...), which every language can
//! produce from the protos it already ships. Adding an RPC is one line in `dispatch`; adding a
//! field to an existing message needs no change here (the SDK's model types carry it).
//!
//! What the core does for every call: idempotency (`policy.rs`), retry and backoff (caller's
//! policy, core's error classification), user-project routing, per-call headers, structured
//! errors with both gRPC and HTTP codes.

use std::ffi::{CString, c_char};

use google_cloud_gax::options::RequestOptionsBuilder;
use google_cloud_iam_v1::model as iam;
use google_cloud_storage::model;
use google_cloud_wkt::FieldMask;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::options::CallOptions;
use crate::{Completion, GcsClient, GcsDoneCb, GcsStatus, Status, guard_rc, policy, req_str, spawn_op};

/// Methods served by `gcs_call`.
pub const METHODS: &[&str] = &[
    "CreateBucket",
    "DeleteBucket",
    "GetBucket",
    "ListBuckets",
    "LockBucketRetentionPolicy",
    "UpdateBucket",
    "ComposeObject",
    "DeleteObject",
    "GetObject",
    "ListObjects",
    "MoveObject",
    "RestoreObject",
    "RewriteObject",
    "UpdateObject",
    "GetIamPolicy",
    "SetIamPolicy",
    "TestIamPermissions",
    "storage.objects.list",
];

fn typed<T: DeserializeOwned>(v: Value) -> Result<T, Status> {
    serde_json::from_value(v).map_err(|e| Status::invalid(format!("request: {e}")))
}

/// proto3-JSON cannot carry every FieldMask: the encoding camelCases each path, which rejects
/// the wildcard `*` (used by read masks) and mangles map keys (`metadata.<key>`,
/// `labels.<key>` in update masks). The ABI therefore also accepts `readMask` / `updateMask`
/// as a JSON *array of raw paths*, which is passed through verbatim.
#[derive(Default)]
struct RawMasks {
    read: Option<Vec<String>>,
    update: Option<Vec<String>>,
}

fn take_raw_masks(v: &mut Value) -> RawMasks {
    let mut take = |name: &str| -> Option<Vec<String>> {
        let obj = v.as_object_mut()?;
        if !matches!(obj.get(name), Some(Value::Array(_))) {
            return None;
        }
        match obj.remove(name) {
            Some(Value::Array(a)) => Some(a.into_iter().filter_map(|p| p.as_str().map(str::to_string)).collect()),
            _ => None,
        }
    };
    RawMasks { read: take("readMask"), update: take("updateMask") }
}

fn with_raw(raw: &Option<Vec<String>>, m: Option<FieldMask>) -> Option<FieldMask> {
    match raw {
        Some(p) => Some(FieldMask::default().set_paths(p.clone())),
        None => m,
    }
}

pub(crate) fn to_json<T: serde::Serialize>(v: &T) -> Result<String, Status> {
    let s = serde_json::to_string(v).map_err(|e| Status::err(format!("encoding response: {e}")))?;
    Ok(if s == "null" { "{}".to_string() } else { s })
}

pub(crate) async fn dispatch(client: GcsClient, method: String, req: Vec<u8>, opts: CallOptions) -> Result<String, Status> {
    let mut v: Value = serde_json::from_slice(&req).map_err(|e| Status::invalid(format!("request is not JSON: {e}")))?;
    let raw = take_raw_masks(&mut v);
    let c = &client;
    let ctl = &client.control;

    // Build -> apply core policy (idempotency, retry, backoff, user project, headers) -> send.
    macro_rules! send {
        ($builder:expr, $req:expr, $idempotent:path) => {{
            let r = $req;
            let idempotent = $idempotent(&r);
            let mut b = $builder.with_request(r).with_options(opts.request_options(c, idempotent)?);
            for (k, val) in opts.header_pairs()? {
                b = b.with_custom_header(k, val);
            }
            let resp = b.send().await.map_err(|e| Status::from_sdk(&e))?;
            to_json(&resp)
        }};
    }
    // Typed request with raw read/update masks re-applied.
    macro_rules! req {
        ($T:ty) => {{ typed::<$T>(v)? }};
        ($T:ty, read_mask) => {{
            let mut r: $T = typed(v)?;
            r.read_mask = with_raw(&raw.read, r.read_mask.take());
            r
        }};
        ($T:ty, update_mask) => {{
            let mut r: $T = typed(v)?;
            r.update_mask = with_raw(&raw.update, r.update_mask.take());
            r
        }};
    }

    match method.as_str() {
        // Buckets.
        "CreateBucket" => send!(ctl.create_bucket(), req!(model::CreateBucketRequest), policy::always),
        "DeleteBucket" => send!(ctl.delete_bucket(), req!(model::DeleteBucketRequest), policy::always),
        "GetBucket" => send!(ctl.get_bucket(), req!(model::GetBucketRequest, read_mask), policy::always),
        "ListBuckets" => send!(ctl.list_buckets(), req!(model::ListBucketsRequest, read_mask), policy::always),
        "LockBucketRetentionPolicy" => {
            send!(ctl.lock_bucket_retention_policy(), req!(model::LockBucketRetentionPolicyRequest), policy::always)
        }
        "UpdateBucket" => send!(ctl.update_bucket(), req!(model::UpdateBucketRequest, update_mask), policy::update_bucket),
        // Objects.
        "ComposeObject" => send!(ctl.compose_object(), req!(model::ComposeObjectRequest), policy::compose_object),
        "DeleteObject" => send!(ctl.delete_object(), req!(model::DeleteObjectRequest), policy::delete_object),
        "GetObject" => send!(ctl.get_object(), req!(model::GetObjectRequest, read_mask), policy::always),
        "ListObjects" => send!(ctl.list_objects(), req!(model::ListObjectsRequest, read_mask), policy::always),
        "MoveObject" => send!(ctl.move_object(), req!(model::MoveObjectRequest), policy::always),
        "RestoreObject" => send!(ctl.restore_object(), req!(model::RestoreObjectRequest), policy::always),
        "RewriteObject" => send!(ctl.rewrite_object(), req!(model::RewriteObjectRequest), policy::rewrite_object),
        "UpdateObject" => send!(ctl.update_object(), req!(model::UpdateObjectRequest, update_mask), policy::update_object),
        // IAM.
        "GetIamPolicy" => send!(ctl.get_iam_policy(), req!(iam::GetIamPolicyRequest), policy::always),
        "SetIamPolicy" => send!(ctl.set_iam_policy(), req!(iam::SetIamPolicyRequest, update_mask), policy::set_iam_policy),
        "TestIamPermissions" => send!(ctl.test_iam_permissions(), req!(iam::TestIamPermissionsRequest), policy::always),
        // JSON API methods the SDK does not offer (request/response: JSON API resources).
        "storage.objects.list" => crate::json_api::list_objects(c, v, &opts).await,
        other => Err(Status::unimplemented(format!("gcs_core: method {other:?} is not supported"))),
    }
}

pub(crate) unsafe fn bytes<'a>(p: *const u8, len: usize) -> &'a [u8] {
    if p.is_null() || len == 0 { &[] } else { unsafe { std::slice::from_raw_parts(p, len) } }
}

/// Starts a unary RPC. `method` is the v2 RPC name (see `METHODS`); `req` is the proto3-JSON
/// request; `opts` the options JSON (may be NULL). Pending (returns 1): the callback fires
/// exactly once with `n` = a Rust-owned C string holding the proto3-JSON response (free with
/// `gcs_string_free`), or an error. `*out_op` receives an id for `gcs_op_cancel`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcs_call(
    c: *const GcsClient,
    method: *const c_char,
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
        let client = unsafe { &*c }.clone();
        let mut opts = CallOptions::parse(opts, opts_len)?;
        opts.bind(&client);
        let method = unsafe { req_str(method, "method") }?;
        let req = unsafe { bytes(req, req_len) }.to_vec();
        let id = spawn_op(
            async move {
                let js = dispatch(client, method, req, opts).await?;
                let cs = CString::new(js).map_err(|e| Status::err(e.to_string()))?;
                Ok(cs.into_raw() as u64)
            },
            Completion::new(cb, user),
        );
        if !out_op.is_null() {
            unsafe { *out_op = id };
        }
        Ok(1)
    })
}
