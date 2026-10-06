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

//! Idempotency rules and deduplication tokens for Cloud Storage requests.
//!
//! `librarian.yaml` sets `idempotency_hook: resolve_idempotency` for
//! `google/storage/v2`, so every generated `google.storage.v2.Storage` RPC calls
//! `req.resolve_idempotency(options)`. A new RPC needs an impl below; the
//! generated code does not compile without one.
//!
//! The rules follow the [GCS retry strategy]. Negative preconditions
//! (`*_not_match`) never make a request idempotent, because they can match again
//! on a retry.
//!
//! Handwritten operations (uploads) call [mutation] once, before their retry
//! loop, so every attempt sends the same token.
//!
//! [GCS retry strategy]: https://cloud.google.com/storage/docs/retry-strategy#idempotency-operations

use google_cloud_gax::options::RequestOptions;
use google_cloud_gax::options::internal::{RequestOptionsExt, set_default_idempotency};
use http::{HeaderMap, HeaderValue};

/// The header Cloud Storage uses to deduplicate retried mutations.
pub(crate) const IDEMPOTENCY_TOKEN_HEADER: &str = "x-goog-gcs-idempotency-token";

/// Reads and lists are always idempotent and never carry a deduplication token.
fn read(options: RequestOptions) -> RequestOptions {
    set_default_idempotency(options, true)
}

/// Mutations are retried only if idempotent, and then carry a deduplication token.
///
/// An explicit `with_idempotency()` from the application takes precedence over
/// `idempotent`. A token already in the headers (from a previous call or from the
/// application) is kept.
pub(crate) fn mutation(options: RequestOptions, idempotent: bool) -> RequestOptions {
    let options = set_default_idempotency(options, idempotent);
    if options.idempotent() != Some(true) {
        return options;
    }
    add_token(options)
}

/// Stamps a deduplication token onto `options` if one is not already present.
///
/// Resumable uploads are always retried (regardless of preconditions or
/// `with_idempotency(false)`), so session creation calls this directly to
/// ensure every attempt carries a deduplication token.
pub(crate) fn add_token(options: RequestOptions) -> RequestOptions {
    if options
        .get_extension::<HeaderMap>()
        .is_some_and(|h| h.contains_key(IDEMPOTENCY_TOKEN_HEADER))
    {
        return options;
    }
    let mut headers = options
        .get_extension::<HeaderMap>()
        .cloned()
        .unwrap_or_default();
    let token = HeaderValue::try_from(uuid::Uuid::new_v4().to_string())
        .expect("a hyphenated UUID is always a valid header value");
    headers.insert(IDEMPOTENCY_TOKEN_HEADER, token);
    options.insert_extension(headers)
}

impl crate::model::GetObjectRequest {
    pub(crate) fn resolve_idempotency(&self, options: RequestOptions) -> RequestOptions {
        read(options)
    }
}

impl crate::model::ListObjectsRequest {
    pub(crate) fn resolve_idempotency(&self, options: RequestOptions) -> RequestOptions {
        read(options)
    }
}

impl crate::model::GetBucketRequest {
    pub(crate) fn resolve_idempotency(&self, options: RequestOptions) -> RequestOptions {
        read(options)
    }
}

impl crate::model::ListBucketsRequest {
    pub(crate) fn resolve_idempotency(&self, options: RequestOptions) -> RequestOptions {
        read(options)
    }
}

impl crate::model::CreateBucketRequest {
    pub(crate) fn resolve_idempotency(&self, options: RequestOptions) -> RequestOptions {
        mutation(options, true)
    }
}

impl crate::model::DeleteBucketRequest {
    pub(crate) fn resolve_idempotency(&self, options: RequestOptions) -> RequestOptions {
        mutation(options, true)
    }
}

impl crate::model::LockBucketRetentionPolicyRequest {
    pub(crate) fn resolve_idempotency(&self, options: RequestOptions) -> RequestOptions {
        mutation(options, true)
    }
}

impl crate::model::UpdateBucketRequest {
    pub(crate) fn resolve_idempotency(&self, options: RequestOptions) -> RequestOptions {
        mutation(options, self.if_metageneration_match.is_some())
    }
}

impl crate::model::ComposeObjectRequest {
    pub(crate) fn resolve_idempotency(&self, options: RequestOptions) -> RequestOptions {
        mutation(options, self.if_generation_match.is_some())
    }
}

impl crate::model::DeleteObjectRequest {
    pub(crate) fn resolve_idempotency(&self, options: RequestOptions) -> RequestOptions {
        mutation(
            options,
            self.generation > 0 || self.if_generation_match.is_some(),
        )
    }
}

impl crate::model::RestoreObjectRequest {
    pub(crate) fn resolve_idempotency(&self, options: RequestOptions) -> RequestOptions {
        // `generation` is a required field selecting the soft-deleted version to
        // restore. Restoring creates a new live generation, so only
        // `if_generation_match` protects against a double restore.
        mutation(options, self.if_generation_match.is_some())
    }
}

impl crate::model::UpdateObjectRequest {
    pub(crate) fn resolve_idempotency(&self, options: RequestOptions) -> RequestOptions {
        mutation(options, self.if_metageneration_match.is_some())
    }
}

impl crate::model::RewriteObjectRequest {
    pub(crate) fn resolve_idempotency(&self, options: RequestOptions) -> RequestOptions {
        mutation(options, self.if_generation_match.is_some())
    }
}

impl crate::model::MoveObjectRequest {
    pub(crate) fn resolve_idempotency(&self, options: RequestOptions) -> RequestOptions {
        mutation(options, self.if_generation_match.is_some())
    }
}

impl crate::model::WriteObjectSpec {
    /// Returns `true` if the upload is protected by an `if_generation_match` precondition.
    pub(crate) fn is_idempotent(&self) -> bool {
        self.if_generation_match.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{
        ComposeObjectRequest, CreateBucketRequest, DeleteBucketRequest, DeleteObjectRequest,
        GetBucketRequest, GetObjectRequest, ListBucketsRequest, ListObjectsRequest,
        LockBucketRetentionPolicyRequest, MoveObjectRequest, RestoreObjectRequest,
        RewriteObjectRequest, UpdateBucketRequest, UpdateObjectRequest, WriteObjectSpec,
    };
    use test_case::test_case;

    fn token(options: &RequestOptions) -> Option<&HeaderValue> {
        options
            .get_extension::<HeaderMap>()
            .and_then(|h| h.get(IDEMPOTENCY_TOKEN_HEADER))
    }

    fn assert_mutation(got: RequestOptions, want: bool) {
        assert_eq!(got.idempotent(), Some(want));
        assert_eq!(token(&got).is_some(), want, "token iff idempotent");
    }

    #[test]
    fn reads_are_idempotent_without_token() {
        for got in [
            GetObjectRequest::new().resolve_idempotency(RequestOptions::default()),
            ListObjectsRequest::new().resolve_idempotency(RequestOptions::default()),
            GetBucketRequest::new().resolve_idempotency(RequestOptions::default()),
            ListBucketsRequest::new().resolve_idempotency(RequestOptions::default()),
        ] {
            assert_eq!(got.idempotent(), Some(true));
            assert!(token(&got).is_none(), "{got:?}");
        }
    }

    #[test]
    fn bucket_create_delete_lock_are_always_idempotent() {
        for got in [
            CreateBucketRequest::new().resolve_idempotency(RequestOptions::default()),
            DeleteBucketRequest::new().resolve_idempotency(RequestOptions::default()),
            LockBucketRetentionPolicyRequest::new().resolve_idempotency(RequestOptions::default()),
        ] {
            assert_mutation(got, true);
        }
    }

    #[test_case(UpdateBucketRequest::new(), false; "unconditioned")]
    #[test_case(UpdateBucketRequest::new().set_if_metageneration_match(1), true; "if_metageneration_match")]
    #[test_case(UpdateBucketRequest::new().set_if_metageneration_not_match(1), false; "if_metageneration_not_match")]
    fn update_bucket(req: UpdateBucketRequest, want: bool) {
        assert_mutation(req.resolve_idempotency(RequestOptions::default()), want);
    }

    #[test_case(UpdateObjectRequest::new(), false; "unconditioned")]
    #[test_case(UpdateObjectRequest::new().set_if_metageneration_match(1), true; "if_metageneration_match")]
    #[test_case(UpdateObjectRequest::new().set_if_metageneration_not_match(1), false; "if_metageneration_not_match")]
    #[test_case(UpdateObjectRequest::new().set_if_generation_match(1), false; "if_generation_match alone")]
    fn update_object(req: UpdateObjectRequest, want: bool) {
        assert_mutation(req.resolve_idempotency(RequestOptions::default()), want);
    }

    #[test_case(ComposeObjectRequest::new(), false; "unconditioned")]
    #[test_case(ComposeObjectRequest::new().set_if_generation_match(1), true; "if_generation_match")]
    #[test_case(ComposeObjectRequest::new().set_if_metageneration_match(1), false; "if_metageneration_match alone")]
    fn compose_object(req: ComposeObjectRequest, want: bool) {
        assert_mutation(req.resolve_idempotency(RequestOptions::default()), want);
    }

    #[test_case(DeleteObjectRequest::new(), false; "unconditioned")]
    #[test_case(DeleteObjectRequest::new().set_generation(1), true; "generation")]
    #[test_case(DeleteObjectRequest::new().set_if_generation_match(1), true; "if_generation_match")]
    #[test_case(DeleteObjectRequest::new().set_if_generation_not_match(1), false; "if_generation_not_match")]
    #[test_case(DeleteObjectRequest::new().set_if_metageneration_match(1), false; "if_metageneration_match alone")]
    fn delete_object(req: DeleteObjectRequest, want: bool) {
        assert_mutation(req.resolve_idempotency(RequestOptions::default()), want);
    }

    #[test_case(RestoreObjectRequest::new().set_generation(1), false; "generation alone")]
    #[test_case(RestoreObjectRequest::new().set_generation(1).set_if_generation_match(1), true; "if_generation_match")]
    #[test_case(RestoreObjectRequest::new().set_generation(1).set_if_generation_not_match(1), false; "if_generation_not_match")]
    fn restore_object(req: RestoreObjectRequest, want: bool) {
        assert_mutation(req.resolve_idempotency(RequestOptions::default()), want);
    }

    #[test_case(RewriteObjectRequest::new(), false; "unconditioned")]
    #[test_case(RewriteObjectRequest::new().set_if_generation_match(1), true; "if_generation_match")]
    #[test_case(RewriteObjectRequest::new().set_if_generation_not_match(1), false; "if_generation_not_match")]
    #[test_case(RewriteObjectRequest::new().set_if_source_generation_match(1), false; "if_source_generation_match alone")]
    fn rewrite_object(req: RewriteObjectRequest, want: bool) {
        assert_mutation(req.resolve_idempotency(RequestOptions::default()), want);
    }

    #[test_case(MoveObjectRequest::new(), false; "unconditioned")]
    #[test_case(MoveObjectRequest::new().set_if_generation_match(1), true; "if_generation_match")]
    #[test_case(MoveObjectRequest::new().set_if_generation_not_match(1), false; "if_generation_not_match")]
    #[test_case(MoveObjectRequest::new().set_if_source_generation_match(1), false; "if_source_generation_match alone")]
    fn move_object(req: MoveObjectRequest, want: bool) {
        assert_mutation(req.resolve_idempotency(RequestOptions::default()), want);
    }

    #[test_case(WriteObjectSpec::new(), false; "unconditioned")]
    #[test_case(WriteObjectSpec::new().set_if_generation_match(0), true; "if_generation_match")]
    #[test_case(WriteObjectSpec::new().set_if_generation_not_match(0), false; "if_generation_not_match")]
    #[test_case(WriteObjectSpec::new().set_if_metageneration_match(1), false; "if_metageneration_match alone")]
    fn write_object_spec(spec: WriteObjectSpec, want: bool) {
        assert_eq!(spec.is_idempotent(), want);
    }

    #[test_case(true, false; "with_idempotency(true) on unconditioned")]
    #[test_case(false, true; "with_idempotency(false) on conditioned")]
    fn explicit_override_wins(explicit: bool, rule: bool) {
        let mut options = RequestOptions::default();
        options.set_idempotency(explicit);
        assert_mutation(mutation(options, rule), explicit);
    }

    #[test]
    fn existing_token_is_kept() {
        let mut headers = HeaderMap::new();
        headers.insert(
            IDEMPOTENCY_TOKEN_HEADER,
            HeaderValue::from_static("caller-token"),
        );
        let options = RequestOptions::default().insert_extension(headers);
        let got = mutation(options, true);
        assert_eq!(
            token(&got).map(|v| v.as_bytes()),
            Some("caller-token".as_bytes())
        );
    }

    #[test]
    fn other_headers_are_kept() {
        let mut headers = HeaderMap::new();
        headers.insert("x-goog-custom", HeaderValue::from_static("keep-me"));
        let options = RequestOptions::default().insert_extension(headers);
        let got = mutation(options, true);
        let headers = got.get_extension::<HeaderMap>().expect("headers are set");
        assert_eq!(
            headers.get("x-goog-custom").map(|v| v.as_bytes()),
            Some("keep-me".as_bytes())
        );
        assert!(token(&got).is_some());
    }

    /// Guards against regenerating `gapic/transport.rs` without the hook, which
    /// would silently make every `google.storage.v2.Storage` RPC non-idempotent.
    #[test]
    fn generated_transport_uses_hook() {
        let source = include_str!("generated/gapic/transport.rs");
        assert!(source.contains("req.resolve_idempotency(options)"));
        assert!(
            !source.contains("set_default_idempotency("),
            "regenerate with `idempotency_hook: resolve_idempotency` in librarian.yaml"
        );
    }
}
