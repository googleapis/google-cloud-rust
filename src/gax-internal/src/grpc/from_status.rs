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

use crate::grpc::status::status_from_proto;
use gax::error::Error;
use gax::error::rpc::Status;
use prost::Message;
use std::error::Error as _;

fn to_gax_status(status: &tonic::Status) -> Status {
    let pb = crate::google::rpc::Status::decode(status.details()).unwrap_or_default();
    status_from_proto(pb)
        .set_code(status.code())
        .set_message(status.message())
        .into()
}

fn as_inner<T>(status: &tonic::Status) -> Option<&T>
where
    T: std::error::Error + 'static,
{
    let mut e = status.source()?;
    // Prevent infinite loops due to cycles in the `source()` errors. This seems
    // unlikely, and it would require effort to create, but it is easy to
    // prevent.
    for _ in 0..32 {
        if let Some(value) = e.downcast_ref::<T>() {
            return Some(value);
        }
        e = e.source()?;
    }
    None
}

pub fn to_gax_error(status: tonic::Status) -> Error {
    if as_inner::<tonic::TimeoutExpired>(&status).is_some() {
        return Error::timeout(status);
    }
    if as_inner::<tonic::ConnectError>(&status).is_some() {
        return Error::connect(status);
    }
    let headers = status.metadata().clone().into_headers();
    if as_inner::<tonic::transport::Error>(&status).is_some() {
        return Error::transport(headers, status);
    }

    let content_type = headers.get("content-type").map(|v| v.as_bytes());
    if content_type.is_some_and(|v| !v.starts_with("application/grpc".as_bytes())) {
        return Error::transport(headers, GrpcError::BadContentType(status));
    }

    let gax_status = to_gax_status(&status);
    gax::error::Error::service_full(gax_status, None, Some(headers), Some(Box::new(status)))
}

#[derive(Debug, thiserror::Error)]
enum GrpcError {
    BadContentType(#[source] tonic::Status),
}

impl std::fmt::Display for GrpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadContentType(_status) => write!(
                f,
                "unexpected value in content-type header, should start with application/grpc. In Google Cloud, this is a common problem when using an invalid endpoint, or an endpoint that does not support the target gRPC service."
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gax::error::rpc::{Code, StatusDetails};
    use test_case::test_case;

    #[test_case(tonic::Code::Ok, Code::Ok)]
    #[test_case(tonic::Code::Cancelled, Code::Cancelled)]
    #[test_case(tonic::Code::Unknown, Code::Unknown)]
    #[test_case(tonic::Code::InvalidArgument, Code::InvalidArgument)]
    #[test_case(tonic::Code::DeadlineExceeded, Code::DeadlineExceeded)]
    #[test_case(tonic::Code::NotFound, Code::NotFound)]
    #[test_case(tonic::Code::AlreadyExists, Code::AlreadyExists)]
    #[test_case(tonic::Code::PermissionDenied, Code::PermissionDenied)]
    #[test_case(tonic::Code::ResourceExhausted, Code::ResourceExhausted)]
    #[test_case(tonic::Code::FailedPrecondition, Code::FailedPrecondition)]
    #[test_case(tonic::Code::Aborted, Code::Aborted)]
    #[test_case(tonic::Code::OutOfRange, Code::OutOfRange)]
    #[test_case(tonic::Code::Unimplemented, Code::Unimplemented)]
    #[test_case(tonic::Code::Internal, Code::Internal)]
    #[test_case(tonic::Code::Unavailable, Code::Unavailable)]
    #[test_case(tonic::Code::DataLoss, Code::DataLoss)]
    #[test_case(tonic::Code::Unauthenticated, Code::Unauthenticated)]
    fn check_code(input: tonic::Code, want: Code) {
        let got = to_gax_status(&tonic::Status::new(input, "test-only"));
        assert_eq!(got.code, want);
        assert_eq!(&got.message, "test-only");
    }

    #[test]
    fn gax_error() {
        let mut input = tonic::Status::invalid_argument("test-only");
        input.metadata_mut().append(
            "content-type",
            tonic::metadata::AsciiMetadataValue::from_static("application/grpc"),
        );
        let got = to_gax_error(input.clone());
        assert!(got.status().is_some(), "{got:?}");
        let status = got.status().unwrap();
        assert_eq!(status.code, Code::InvalidArgument);
        assert_eq!(&status.message, "test-only");

        let source = got
            .source()
            .and_then(|e| e.downcast_ref::<tonic::Status>())
            .unwrap();
        assert_eq!(source.code(), input.code());
        assert_eq!(source.message(), input.message());
        assert_eq!(source.details(), input.details());
    }

    #[test]
    fn gax_error_bad_content_type() {
        let mut status = tonic::Status::internal("oh noes");
        status.metadata_mut().append(
            "content-type",
            tonic::metadata::AsciiMetadataValue::from_static("application/xml; charset=UTF-8"),
        );
        let got = to_gax_error(status);
        assert!(got.is_transport(), "{got:?}");
        assert!(got.status().is_none(), "{got:?}");
        let source = got
            .source()
            .and_then(|e| e.downcast_ref::<GrpcError>())
            .expect("want a GrpcError as source");
        assert!(matches!(source, GrpcError::BadContentType(_)), "{source:?}");

        let source = got
            .source()
            .and_then(|e| e.source())
            .and_then(|e| e.downcast_ref::<tonic::Status>())
            .expect("want a tonic::Status as source().source()");
        assert_eq!(source.code(), tonic::Code::Internal);

        let fmt = format!("{got}");
        assert!(
            fmt.contains("should start with application/grpc"),
            "fmt={fmt}, got={got:?}"
        );
    }

    #[test]
    fn gax_error_with_details() -> anyhow::Result<()> {
        let mut buf = bytes::BytesMut::with_capacity(256);
        let code = Code::InvalidArgument as i32;
        let status = crate::google::rpc::Status {
            code,
            message: "test-only".to_string(),
            details: vec![prost_types::Any::from_msg(
                &crate::google::rpc::ErrorInfo {
                    reason: "reason".into(),
                    domain: "domain".into(),
                    ..Default::default()
                },
            )?],
        };
        status.encode(&mut buf)?;

        let status = tonic::Status::with_details(code.into(), "test-only", buf.freeze());
        let got = to_gax_error(status);
        let status = got.status().unwrap();
        assert_eq!(status.code, Code::InvalidArgument);
        assert_eq!(&status.message, "test-only");
        assert_eq!(
            status.details,
            vec![StatusDetails::ErrorInfo(
                rpc::model::ErrorInfo::default()
                    .set_reason("reason")
                    .set_domain("domain")
            )]
        );
        Ok(())
    }
}
