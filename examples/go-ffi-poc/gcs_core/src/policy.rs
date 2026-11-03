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

//! Per-RPC idempotency rules (https://cloud.google.com/storage/docs/retry-strategy), decided
//! from the request itself.
//!
//! The Rust SDK's generated control-plane client defaults *every* RPC to non-idempotent, so
//! without these rules nothing but connection errors would be retried. Today each language SDK
//! re-implements this table in its own veneer; here it is implemented once for all of them.

use google_cloud_iam_v1::model as iam;
use google_cloud_storage::model;

/// Reads, lists, creates/deletes of buckets, IAM reads, retention lock (which itself requires
/// a metageneration precondition), restore and move.
pub(crate) fn always<T>(_: &T) -> bool {
    true
}

/// PATCH semantics: safe to repeat only when pinned to a metageneration.
pub(crate) fn update_bucket(r: &model::UpdateBucketRequest) -> bool {
    r.if_metageneration_match.is_some()
}

pub(crate) fn update_object(r: &model::UpdateObjectRequest) -> bool {
    r.if_metageneration_match.is_some()
}

/// Deleting a specific generation, or the live object under a generation precondition.
pub(crate) fn delete_object(r: &model::DeleteObjectRequest) -> bool {
    r.generation != 0 || r.if_generation_match.is_some()
}

/// Compose/rewrite create a new generation: idempotent only with a destination precondition.
pub(crate) fn compose_object(r: &model::ComposeObjectRequest) -> bool {
    r.if_generation_match.is_some()
}

pub(crate) fn rewrite_object(r: &model::RewriteObjectRequest) -> bool {
    r.if_generation_match.is_some()
}

/// Read-modify-write of a policy: idempotent only with the etag of the policy that was read.
pub(crate) fn set_iam_policy(r: &iam::SetIamPolicyRequest) -> bool {
    r.policy.as_ref().is_some_and(|p| !p.etag.is_empty())
}

/// Single-shot uploads are idempotent with a generation precondition (resumable uploads are
/// always treated as idempotent by the SDK: they can only be finalized once).
pub(crate) fn write_object(spec: &model::WriteObjectSpec) -> bool {
    spec.if_generation_match.is_some()
}
