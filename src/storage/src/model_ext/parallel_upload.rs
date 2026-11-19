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

use crate::google::storage::v2;
use crate::model::{CommonObjectRequestParams, Object};
use gaxi::prost::ToProto;

/// Represents the parameters of a request to create a parallel upload session.
#[derive(Clone, Debug, Default, PartialEq)]
#[non_exhaustive]
pub struct CreateObjectWriteSessionRequest {
    /// The destination object metadata.
    pub resource: Option<Object>,
    /// A predefined set of access controls to apply to the object.
    pub predefined_acl: String,
    /// Additional request parameters.
    pub params: Option<CommonObjectRequestParams>,
    /// The preferred segment size in bytes.
    pub preferred_segment_size_bytes: i64,
}

impl CreateObjectWriteSessionRequest {
    /// Creates a new instance.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the destination object metadata.
    pub fn set_resource<V: Into<Object>>(mut self, v: V) -> Self {
        self.resource = Some(v.into());
        self
    }

    /// Sets the predefined ACL to apply to the object.
    pub fn set_predefined_acl<V: Into<String>>(mut self, v: V) -> Self {
        self.predefined_acl = v.into();
        self
    }

    /// Sets additional request parameters.
    pub fn set_params<V: Into<CommonObjectRequestParams>>(mut self, v: V) -> Self {
        self.params = Some(v.into());
        self
    }

    /// Sets the preferred segment size in bytes.
    pub fn set_preferred_segment_size_bytes(mut self, v: i64) -> Self {
        self.preferred_segment_size_bytes = v;
        self
    }
}

impl TryFrom<CreateObjectWriteSessionRequest>
    for crate::google::storage::v2::CreateObjectWriteSessionRequest
{
    type Error = crate::Error;

    fn try_from(value: CreateObjectWriteSessionRequest) -> crate::Result<Self> {
        let resource = value
            .resource
            .map(ToProto::<v2::Object>::to_proto)
            .transpose()
            .map_err(crate::Error::deser)?;
        let object_write_session_spec = crate::google::storage::v2::ObjectWriteSessionSpec {
            resource,
            predefined_acl: value.predefined_acl,
        };
        let common_object_request_params = value
            .params
            .map(ToProto::<v2::CommonObjectRequestParams>::to_proto)
            .transpose()
            .map_err(crate::Error::deser)?;

        Ok(Self {
            object_write_session_spec: Some(object_write_session_spec),
            common_object_request_params,
            preferred_segment_size_bytes: value.preferred_segment_size_bytes,
        })
    }
}

/// Metadata for a completed parallel upload segment.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct Segment {
    /// The resource name of the segment.
    pub name: String,
    /// The size of the segment in bytes.
    pub size_bytes: i64,
    /// The CRC32C checksum of the segment data.
    pub segment_crc32c: u32,
}

impl Segment {
    /// Creates a new instance.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the resource name of the segment.
    pub fn set_name<V: Into<String>>(mut self, v: V) -> Self {
        self.name = v.into();
        self
    }

    /// Sets the size of the segment in bytes.
    pub fn set_size_bytes(mut self, v: i64) -> Self {
        self.size_bytes = v;
        self
    }

    /// Sets the CRC32C checksum of the segment data.
    pub fn set_segment_crc32c(mut self, v: u32) -> Self {
        self.segment_crc32c = v;
        self
    }
}

impl From<crate::google::storage::v2::Segment> for Segment {
    fn from(value: crate::google::storage::v2::Segment) -> Self {
        Self {
            name: value.name,
            size_bytes: value.size_bytes,
            segment_crc32c: value.segment_crc32c,
        }
    }
}

/// Response from aborting a parallel upload session.
#[derive(Clone, Debug, Default, PartialEq)]
#[non_exhaustive]
pub struct AbortSessionResponse {}

impl AbortSessionResponse {
    /// Creates a new instance.
    pub fn new() -> Self {
        Self::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::google::storage::v2::{
        CommonObjectRequestParams as ProtoParams,
        CreateObjectWriteSessionRequest as ProtoCreateRequest, Object as ProtoObject,
        ObjectWriteSessionSpec as ProtoSpec, Segment as ProtoSegment,
    };

    #[test]
    fn test_create_object_write_session_request_try_from() -> anyhow::Result<()> {
        let req = CreateObjectWriteSessionRequest::new()
            .set_resource(Object::new().set_bucket("my-bucket").set_name("my-object"))
            .set_predefined_acl("private")
            .set_params(CommonObjectRequestParams::new().set_encryption_algorithm("AES256"))
            .set_preferred_segment_size_bytes(1024);

        let got = ProtoCreateRequest::try_from(req)?;
        let want = ProtoCreateRequest {
            object_write_session_spec: Some(ProtoSpec {
                resource: Some(ProtoObject {
                    bucket: "my-bucket".into(),
                    name: "my-object".into(),
                    ..ProtoObject::default()
                }),
                predefined_acl: "private".into(),
            }),
            common_object_request_params: Some(ProtoParams {
                encryption_algorithm: "AES256".into(),
                ..ProtoParams::default()
            }),
            preferred_segment_size_bytes: 1024,
        };
        assert_eq!(got, want);
        Ok(())
    }

    #[test]
    fn test_segment_from() {
        let proto = ProtoSegment {
            name: "test-segment".into(),
            size_bytes: 1024,
            segment_crc32c: 42,
        };
        let got = Segment::from(proto);
        let want = Segment::new()
            .set_name("test-segment")
            .set_size_bytes(1024)
            .set_segment_crc32c(42);
        assert_eq!(got, want);
    }

    #[test]
    fn test_abort_session_response_traits() {
        static_assertions::assert_impl_all!(
            AbortSessionResponse: Clone,
            std::fmt::Debug,
            Default,
            PartialEq,
            Send,
            Sync
        );
        let resp = AbortSessionResponse::new();
        assert_eq!(resp.clone(), AbortSessionResponse::default());
        assert!(format!("{resp:?}").contains("AbortSessionResponse"));
    }
}
