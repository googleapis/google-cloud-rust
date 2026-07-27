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

// TODO(#5716): Lift to shared bidi module

use crate::Error;
use crate::google::rpc::Status as RpcStatus;
use crate::google::storage::v2::BidiWriteObjectRedirectedError;
use gaxi::as_inner::as_inner;
use gaxi::grpc::tonic::Status;
use google_cloud_gax::error::rpc::Code;
use prost::Message;

/// Determine if an error is a redirect error.
///
/// Redirect payloads are attached to an `ABORTED` status, as mentioned in
/// the [gRPC documentation](https://docs.cloud.google.com/storage/docs/reference/rpc/google.storage.v2#bidiwriteobjectredirectederror).
///
/// Checking `Code::Aborted` first safely avoids the overhead of decoding
/// `RpcStatus` details for other error codes.
pub fn is_redirect(error: &Error) -> bool {
    if error.status().is_none_or(|s| s.code != Code::Aborted) {
        return false;
    }
    let Some(status) = as_inner::<Status, _>(error) else {
        return false;
    };

    let Ok(status) = RpcStatus::decode(status.details()) else {
        return false;
    };
    status
        .details
        .iter()
        .any(|d| d.to_msg::<BidiWriteObjectRedirectedError>().is_ok())
}

#[cfg(test)]
mod tests {
    use super::super::tests::{permanent_error, redirect_error, transient_error};
    use super::*;
    use gaxi::grpc::from_status::to_gax_error;
    use gaxi::grpc::tonic::Code;
    use test_case::test_case;

    #[test_case(permanent_error(), false)]
    #[test_case(transient_error(), false)]
    #[test_case(non_grpc_abort_error(), false)]
    #[test_case(redirect_error("r1"), true)]
    #[test_case(to_gax_error(Status::aborted("without-details")), false)]
    #[test_case(
        to_gax_error(Status::with_details(
            Code::Aborted,
            "with bad details",
            bytes::Bytes::from_static(b"\x01")
        )),
        false
    )]
    #[test_case(deep_redirect("r2", 4), true)]
    #[test_case(deep_redirect("r2", 64), false)]
    fn redirect(input: Error, want: bool) {
        assert_eq!(is_redirect(&input), want, "{input:?}");
    }

    pub fn non_grpc_abort_error() -> Error {
        use google_cloud_gax::error::rpc::{Code, Status};
        Error::service(
            Status::default()
                .set_code(Code::Aborted)
                .set_message("aborted-not-gRPC"),
        )
    }

    pub fn deep_redirect(routing: &str, depth: i32) -> Error {
        use google_cloud_gax::error::rpc::{Code, Status};
        let status = Status::default()
            .set_code(Code::Aborted)
            .set_message("aborted-recurse");
        let mut err = redirect_error(routing);
        for _ in 0..depth {
            err = Error::service_full(status.clone(), None, None, Some(Box::new(err)));
        }
        err
    }
}
