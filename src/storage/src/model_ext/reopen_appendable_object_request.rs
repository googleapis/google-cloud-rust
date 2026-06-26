// Copyright 2025 Google LLC
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

#[cfg(google_cloud_unstable_storage_bidi)]
#[cfg_attr(docsrs, doc(cfg(feature = "unstable-stream")))]
/// Represents the parameters of a request to reopen an existing object for appends.
///
/// Consumers of the `google-cloud-storage` crate rarely have a need to use this type directly, the most common exception is when mocking of the `Storage` client.
#[derive(Clone, Debug, Default, PartialEq)]
#[non_exhaustive]
pub struct ReopenAppendableObjectRequest {
    /// The bucket containing the target object.
    pub bucket: String,
    /// The target object name.
    pub object: String,
    /// The target object generation to append to.
    pub generation: i64,
    /// If set, return an error if the current metageneration does not match the value.
    pub if_metageneration_match: Option<i64>,
    /// If set, return an error if the current metageneration matches the value.
    pub if_metageneration_not_match: Option<i64>,
    /// A routing token from a previous operation.
    pub routing_token: Option<String>,
    /// A write handle from a previous operation.
    pub write_handle: Option<bytes::Bytes>,
    /// Additional request parameters.
    pub params: Option<crate::model::CommonObjectRequestParams>,
}

#[cfg(google_cloud_unstable_storage_bidi)]
impl From<ReopenAppendableObjectRequest> for crate::google::storage::v2::AppendObjectSpec {
    fn from(value: ReopenAppendableObjectRequest) -> Self {
        Self {
            bucket: value.bucket,
            object: value.object,
            generation: value.generation,
            if_metageneration_match: value.if_metageneration_match,
            if_metageneration_not_match: value.if_metageneration_not_match,
            routing_token: value.routing_token,
            write_handle: value
                .write_handle
                .map(|h| crate::google::storage::v2::BidiWriteHandle { handle: h }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reopen_appendable_object_request_from() {
        let req = ReopenAppendableObjectRequest {
            bucket: "my-bucket".into(),
            object: "my-object".into(),
            generation: 42,
            if_metageneration_match: Some(1),
            if_metageneration_not_match: Some(2),
            routing_token: Some("token".into()),
            write_handle: Some(bytes::Bytes::from("handle")),
            params: None,
        };

        let spec = crate::google::storage::v2::AppendObjectSpec::from(req);
        assert_eq!(spec.bucket, "my-bucket");
        assert_eq!(spec.object, "my-object");
        assert_eq!(spec.generation, 42);
        assert_eq!(spec.if_metageneration_match, Some(1));
        assert_eq!(spec.if_metageneration_not_match, Some(2));
        assert_eq!(spec.routing_token, Some("token".into()));
        assert_eq!(
            spec.write_handle,
            Some(crate::google::storage::v2::BidiWriteHandle {
                handle: bytes::Bytes::from("handle")
            })
        );
    }
}
