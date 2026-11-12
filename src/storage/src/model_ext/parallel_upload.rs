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

impl TryFrom<CreateObjectWriteSessionRequest>
    for crate::google::storage::v2::CreateObjectWriteSessionRequest
{
    type Error = crate::Error;

    fn try_from(value: CreateObjectWriteSessionRequest) -> crate::Result<Self> {
        let resource = value
            .resource
            .map(ToProto::to_proto)
            .transpose()
            .map_err(crate::Error::deser)?;
        let object_write_session_spec = crate::google::storage::v2::ObjectWriteSessionSpec {
            resource,
            predefined_acl: value.predefined_acl,
        };
        let common_object_request_params = value
            .params
            .map(ToProto::to_proto)
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

impl From<crate::google::storage::v2::Segment> for Segment {
    fn from(value: crate::google::storage::v2::Segment) -> Self {
        Self {
            name: value.name,
            size_bytes: value.size_bytes,
            segment_crc32c: value.segment_crc32c,
        }
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
        let req = CreateObjectWriteSessionRequest {
            resource: Some(Object::new().set_bucket("my-bucket").set_name("my-object")),
            predefined_acl: "private".into(),
            params: Some(CommonObjectRequestParams::new().set_encryption_algorithm("AES256")),
            preferred_segment_size_bytes: 1024,
        };

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
        assert_eq!(got.name, "test-segment");
        assert_eq!(got.size_bytes, 1024);
        assert_eq!(got.segment_crc32c, 42);
    }
}
