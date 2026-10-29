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

//! Implementation details for Google Cloud clients.
//!
//! All the types, traits, and functions defined in this crate are **not**
//! intended for general use. This crate will remain unstable for the
//! foreseeable future, even if used in the implementation for stable client
//! libraries. We (the Google Cloud Client Libraries for Rust team) control
//! both and will change both if needed.
//!
//! The types, traits, and functions defined in this crate are undocumented.
//! This is intentional, as they are not intended for general use and will be
//! changed without notice.

#[cfg(feature = "_internal-common")]
pub mod api_header;

#[cfg(feature = "_internal-common")]
pub mod as_inner;

#[cfg(feature = "_internal-http-client")]
pub mod attempt_info;

#[cfg(feature = "_internal-common")]
pub mod path_parameter;

#[cfg(feature = "_internal-http-client")]
pub mod query_parameter;

#[cfg(feature = "_internal-http-client")]
pub mod http;

#[cfg(feature = "_internal-common")]
pub mod observability;

#[cfg(feature = "_internal-grpc-client")]
pub mod grpc;

#[cfg(feature = "_internal-grpc-client")]
pub mod prost;

#[cfg(feature = "_internal-grpc-client")]
pub mod attempt_interceptor;

#[cfg(feature = "_internal-common")]
pub mod options;

#[cfg(feature = "_internal-common")]
pub mod unimplemented;

#[cfg(feature = "_internal-common")]
pub mod routing_parameter;

#[cfg(any(feature = "_internal-http-client", feature = "_internal-grpc-client"))]
pub(crate) mod headers;

#[cfg(any(feature = "_internal-http-client", feature = "_internal-grpc-client"))]
pub(crate) mod host;

#[cfg(feature = "_internal-common")]
pub(crate) mod universe_domain;

#[cfg(feature = "_internal-grpc-client")]
pub(crate) mod google {
    pub mod rpc {
        include!("generated/protos/rpc/google.rpc.rs");
        include!("generated/convert/rpc/convert.rs");
    }
}

#[cfg(all(test, feature = "_internal-grpc-client"))]
mod to_proto_compatibility_tests {
    use super::google::rpc;
    use crate::prost::ToProto;

    #[test]
    fn external_model_conversions_remain_unambiguous() {
        let _: std::result::Result<i32, _> = google_cloud_rpc::model::Code::default().to_proto();
        let _: std::result::Result<rpc::ErrorInfo, _> =
            google_cloud_rpc::model::ErrorInfo::default().to_proto();
        let _: std::result::Result<rpc::RetryInfo, _> =
            google_cloud_rpc::model::RetryInfo::default().to_proto();
        let _: std::result::Result<rpc::DebugInfo, _> =
            google_cloud_rpc::model::DebugInfo::default().to_proto();
        let _: std::result::Result<rpc::QuotaFailure, _> =
            google_cloud_rpc::model::QuotaFailure::default().to_proto();
        let _: std::result::Result<rpc::PreconditionFailure, _> =
            google_cloud_rpc::model::PreconditionFailure::default().to_proto();
        let _: std::result::Result<rpc::BadRequest, _> =
            google_cloud_rpc::model::BadRequest::default().to_proto();
        let _: std::result::Result<rpc::RequestInfo, _> =
            google_cloud_rpc::model::RequestInfo::default().to_proto();
        let _: std::result::Result<rpc::ResourceInfo, _> =
            google_cloud_rpc::model::ResourceInfo::default().to_proto();
        let _: std::result::Result<rpc::Help, _> =
            google_cloud_rpc::model::Help::default().to_proto();
        let _: std::result::Result<rpc::LocalizedMessage, _> =
            google_cloud_rpc::model::LocalizedMessage::default().to_proto();
        let _: std::result::Result<rpc::HttpRequest, _> =
            google_cloud_rpc::model::HttpRequest::default().to_proto();
        let _: std::result::Result<rpc::HttpResponse, _> =
            google_cloud_rpc::model::HttpResponse::default().to_proto();
        let _: std::result::Result<rpc::HttpHeader, _> =
            google_cloud_rpc::model::HttpHeader::default().to_proto();
    }
}
