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

fn to_gax_status(status: &tonic::Status) -> Status {
    let code = Code::from(status.code() as i32);
    // TODO(#1699) - also convert the details
    Status::default()
        .set_code(code)
        .set_message(status.message())
}

pub fn to_gax_error(status: tonic::Status) -> Error {
    let headers = status.metadata().clone().into_headers();
    let gax_status = to_gax_status(&status);
    gax::error::Error::service_with_http_metadata(gax_status, None, Some(headers))
}

#[cfg(test)]
mod test {
    use super::*;
    use gax::error::rpc;
    use test_case::test_case;

    #[test_case(tonic::Code::Ok, rpc::Code::Ok)]
    #[test_case(tonic::Code::Cancelled, rpc::Code::Cancelled)]
    #[test_case(tonic::Code::Unknown, rpc::Code::Unknown)]
    #[test_case(tonic::Code::InvalidArgument, rpc::Code::InvalidArgument)]
    #[test_case(tonic::Code::DeadlineExceeded, rpc::Code::DeadlineExceeded)]
    #[test_case(tonic::Code::NotFound, rpc::Code::NotFound)]
    #[test_case(tonic::Code::AlreadyExists, rpc::Code::AlreadyExists)]
    #[test_case(tonic::Code::PermissionDenied, rpc::Code::PermissionDenied)]
    #[test_case(tonic::Code::ResourceExhausted, rpc::Code::ResourceExhausted)]
    #[test_case(tonic::Code::FailedPrecondition, rpc::Code::FailedPrecondition)]
    #[test_case(tonic::Code::Aborted, rpc::Code::Aborted)]
    #[test_case(tonic::Code::OutOfRange, rpc::Code::OutOfRange)]
    #[test_case(tonic::Code::Unimplemented, rpc::Code::Unimplemented)]
    #[test_case(tonic::Code::Internal, rpc::Code::Internal)]
    #[test_case(tonic::Code::Unavailable, rpc::Code::Unavailable)]
    #[test_case(tonic::Code::DataLoss, rpc::Code::DataLoss)]
    #[test_case(tonic::Code::Unauthenticated, rpc::Code::Unauthenticated)]
    fn check_code(input: tonic::Code, want: rpc::Code) {
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
