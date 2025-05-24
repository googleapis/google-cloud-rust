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

use gax::error::Error;
use gax::error::rpc::{Code, Status};
use std::error::Error as _;

fn to_gax_status(status: &tonic::Status) -> Status {
    let code = Code::from(status.code() as i32);
    // TODO(#1699) - also convert the details
    Status::default()
        .set_code(code)
        .set_message(status.message())
}

fn contains_tonic_timeout(status: &tonic::Status) -> Option<()> {
    let mut e = status.source()?;
    loop {
        if e.downcast_ref::<tonic::TimeoutExpired>().is_some() {
            return Some(());
        }
        e = e.source()?;
    }
}

fn contains_tonic_transport(status: &tonic::Status) -> Option<()> {
    let mut e = status.source()?;
    loop {
        if e.downcast_ref::<tonic::transport::Error>().is_some() {
            return Some(());
        }
        e = e.source()?;
    }
}

pub fn to_gax_error(status: tonic::Status) -> Error {
    if contains_tonic_timeout(&status).is_some() {
        return Error::timeout(status);
    }
    let headers = status.metadata().clone().into_headers();
    if contains_tonic_transport(&status).is_some() {
        return Error::transport(None, headers, status);
    }

    let gax_status = to_gax_status(&status);
    gax::error::Error::service_with_http_metadata(gax_status, None, Some(headers))
}

#[cfg(test)]
mod test {
    use super::*;
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
        let status = tonic::Status::invalid_argument("test-only");
        let got = to_gax_error(status);
        let status = got.status().unwrap();
        assert_eq!(status.code, Code::InvalidArgument);
        assert_eq!(&status.message, "test-only");
    }
}
