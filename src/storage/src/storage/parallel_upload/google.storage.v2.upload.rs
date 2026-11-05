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

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateObjectWriteSessionRequest {
    #[prost(message, optional, tag = "1")]
    pub write_object_spec: ::core::option::Option<WriteObjectSpec>,
    #[prost(message, optional, tag = "2")]
    pub common_object_request_params: ::core::option::Option<CommonObjectRequestParams>,
    #[prost(int64, tag = "3")]
    pub preferred_segment_size_bytes: i64,
}
impl ::prost::Name for CreateObjectWriteSessionRequest {
    const NAME: &'static str = "CreateObjectWriteSessionRequest";
    const PACKAGE: &'static str = "google.storage.v2";
    fn full_name() -> ::prost::alloc::string::String {
        "google.storage.v2.CreateObjectWriteSessionRequest".into()
    }
    fn type_url() -> ::prost::alloc::string::String {
        "type.googleapis.com/google.storage.v2.CreateObjectWriteSessionRequest".into()
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ObjectWriteSession {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "2")]
    pub object: ::core::option::Option<Object>,
    #[prost(int64, tag = "3")]
    pub segment_size_bytes: i64,
    #[prost(message, optional, tag = "4")]
    pub start_time: ::core::option::Option<::prost_types::Timestamp>,
}
impl ::prost::Name for ObjectWriteSession {
    const NAME: &'static str = "ObjectWriteSession";
    const PACKAGE: &'static str = "google.storage.v2";
    fn full_name() -> ::prost::alloc::string::String {
        "google.storage.v2.ObjectWriteSession".into()
    }
    fn type_url() -> ::prost::alloc::string::String {
        "type.googleapis.com/google.storage.v2.ObjectWriteSession".into()
    }
}
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct BidiWriteSegmentRequest {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(int64, tag = "2")]
    pub write_offset: i64,
    #[prost(message, optional, tag = "3")]
    pub checksummed_data: ::core::option::Option<ChecksummedData>,
    #[prost(bool, tag = "7")]
    pub finish_write: bool,
    #[prost(message, optional, tag = "8")]
    pub common_object_request_params: ::core::option::Option<CommonObjectRequestParams>,
}
impl ::prost::Name for BidiWriteSegmentRequest {
    const NAME: &'static str = "BidiWriteSegmentRequest";
    const PACKAGE: &'static str = "google.storage.v2";
    fn full_name() -> ::prost::alloc::string::String {
        "google.storage.v2.BidiWriteSegmentRequest".into()
    }
    fn type_url() -> ::prost::alloc::string::String {
        "type.googleapis.com/google.storage.v2.BidiWriteSegmentRequest".into()
    }
}
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct BidiWriteSegmentResponse {
    #[prost(message, optional, tag = "2")]
    pub segment: ::core::option::Option<Segment>,
}
impl ::prost::Name for BidiWriteSegmentResponse {
    const NAME: &'static str = "BidiWriteSegmentResponse";
    const PACKAGE: &'static str = "google.storage.v2";
    fn full_name() -> ::prost::alloc::string::String {
        "google.storage.v2.BidiWriteSegmentResponse".into()
    }
    fn type_url() -> ::prost::alloc::string::String {
        "type.googleapis.com/google.storage.v2.BidiWriteSegmentResponse".into()
    }
}
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct Segment {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(int64, tag = "2")]
    pub size_bytes: i64,
    #[prost(fixed32, tag = "4")]
    pub segment_crc32c: u32,
}
impl ::prost::Name for Segment {
    const NAME: &'static str = "Segment";
    const PACKAGE: &'static str = "google.storage.v2";
    fn full_name() -> ::prost::alloc::string::String {
        "google.storage.v2.Segment".into()
    }
    fn type_url() -> ::prost::alloc::string::String {
        "type.googleapis.com/google.storage.v2.Segment".into()
    }
}
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct FinalizeObjectWriteSessionRequest {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "3")]
    pub common_object_request_params: ::core::option::Option<CommonObjectRequestParams>,
    #[prost(message, optional, tag = "4")]
    pub final_object_checksums: ::core::option::Option<ObjectChecksums>,
    #[prost(int32, tag = "5")]
    pub total_segment_count: i32,
    #[prost(string, tag = "6")]
    pub request_id: ::prost::alloc::string::String,
}
impl ::prost::Name for FinalizeObjectWriteSessionRequest {
    const NAME: &'static str = "FinalizeObjectWriteSessionRequest";
    const PACKAGE: &'static str = "google.storage.v2";
    fn full_name() -> ::prost::alloc::string::String {
        "google.storage.v2.FinalizeObjectWriteSessionRequest".into()
    }
    fn type_url() -> ::prost::alloc::string::String {
        "type.googleapis.com/google.storage.v2.FinalizeObjectWriteSessionRequest".into()
    }
}
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct AbortObjectWriteSessionRequest {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
}
impl ::prost::Name for AbortObjectWriteSessionRequest {
    const NAME: &'static str = "AbortObjectWriteSessionRequest";
    const PACKAGE: &'static str = "google.storage.v2";
    fn full_name() -> ::prost::alloc::string::String {
        "google.storage.v2.AbortObjectWriteSessionRequest".into()
    }
    fn type_url() -> ::prost::alloc::string::String {
        "type.googleapis.com/google.storage.v2.AbortObjectWriteSessionRequest".into()
    }
}
