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

mod builder;
mod pending_range;
mod redirect;
mod resume_redirect;
mod retry_redirect;

#[cfg(test)]
mod tests {
    use crate::Error;
    use crate::google::storage::v2::{BidiReadHandle, BidiReadObjectRedirectedError};
    use gax::error::rpc::{Code, Status};
    use prost::Message as _;

    pub(super) fn redirect_handle() -> BidiReadHandle {
        BidiReadHandle {
            handle: bytes::Bytes::from_static(b"test-handle-redirect"),
        }
    }

    pub(super) fn redirect_status(routing: &str) -> tonic::Status {
        use crate::google::rpc::Status as RpcStatus;
        let redirect = BidiReadObjectRedirectedError {
            routing_token: Some(routing.to_string()),
            read_handle: Some(redirect_handle()),
        };
        let redirect = prost_types::Any::from_msg(&redirect).unwrap();
        let status = RpcStatus {
            code: Code::Aborted as i32,
            message: "redirect".to_string(),
            details: vec![redirect],
        };
        let details = bytes::Bytes::from_owner(status.encode_to_vec());
        tonic::Status::with_details(tonic::Code::Aborted, "redirect", details)
    }

    pub(super) fn redirect_error(routing: &str) -> Error {
        gaxi::grpc::from_status::to_gax_error(redirect_status(routing))
    }

    pub(super) fn permanent_error() -> Error {
        Error::service(
            Status::default()
                .set_code(Code::PermissionDenied)
                .set_message("uh-oh"),
        )
    }

    pub(super) fn transient_error() -> Error {
        Error::service(
            Status::default()
                .set_code(Code::Unavailable)
                .set_message("try-again"),
        )
    }
}
