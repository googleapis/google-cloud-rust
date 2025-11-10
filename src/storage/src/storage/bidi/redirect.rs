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

use crate::Error;
use crate::google::rpc::Status as RpcStatus;
use crate::google::storage::v2::{BidiReadObjectRedirectedError, BidiReadObjectSpec};
use gax::error::rpc::Code;
use gaxi::grpc::from_status::to_gax_error;
use prost::Message;
use std::error::Error as _;
use std::sync::{Arc, Mutex};

pub fn handle_redirect(
    spec: Arc<Mutex<BidiReadObjectSpec>>,
    status: tonic::Status,
) -> crate::Error {
    if let Ok(status) = RpcStatus::decode(status.details()) {
        for d in status.details {
            if let Ok(redirect) = d.to_msg::<BidiReadObjectRedirectedError>() {
                let mut guard = spec.lock().expect("never poisoned");
                guard.routing_token = redirect.routing_token;
                guard.read_handle = redirect.read_handle;
                break;
            }
        }
    }
    to_gax_error(status)
}

/// Determine if an error is a redirect error.
pub fn is_redirect(error: &Error) -> bool {
    if error.status().is_none_or(|s| s.code != Code::Aborted) {
        return false;
    }
    let Some(status) = as_inner::<tonic::Status, Error>(&error) else {
        return false;
    };

    let Ok(status) = RpcStatus::decode(status.details()) else {
        return false;
    };
    status
        .details
        .iter()
        .any(|d| d.to_msg::<BidiReadObjectRedirectedError>().is_ok())
}

fn as_inner<T, E>(error: &E) -> Option<&T>
where
    T: std::error::Error + 'static,
    E: std::error::Error,
{
    let mut e = error.source()?;
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

#[cfg(test)]
mod tests {
    use super::super::tests::{permanent_error, redirect_error, transient_error};
    use super::*;
    use crate::google::storage::v2::BidiReadHandle;
    use test_case::test_case;
    use tonic::Code;

    #[test_case(Some("routing"), Some("handle"))]
    #[test_case(None, Some("handle"))]
    #[test_case(Some("routing"), None)]
    #[test_case(None, None)]
    fn reset(routing: Option<&str>, handle: Option<&str>) {
        let read_handle = handle.map(|s| BidiReadHandle {
            handle: bytes::Bytes::from_owner(s.to_string()),
        });
        let redirect = BidiReadObjectRedirectedError {
            routing_token: routing.map(str::to_string),
            read_handle: read_handle.clone(),
        };
        let redirect = prost_types::Any::from_msg(&redirect).unwrap();
        let status = RpcStatus {
            code: Code::Aborted as i32,
            message: "test-only".to_string(),
            details: vec![redirect],
        };
        let details = bytes::Bytes::from_owner(status.encode_to_vec());
        let status = tonic::Status::with_details(Code::Aborted, "test-only", details);
        let spec = BidiReadObjectSpec {
            routing_token: Some("initial-token".into()),
            read_handle: Some(BidiReadHandle {
                handle: bytes::Bytes::from_static(b"initial-handle"),
            }),
            ..Default::default()
        };
        let spec = Arc::new(Mutex::new(spec));

        let got = handle_redirect(spec.clone(), status);
        assert!(got.status().is_some(), "{got:?}");
        let guard = spec.lock().expect("not poisoned");
        assert_eq!(guard.routing_token.as_deref(), routing);
        assert_eq!(guard.read_handle, read_handle);
    }

    #[test]
    fn no_change() {
        let status = RpcStatus {
            code: Code::Aborted as i32,
            message: "test-only".to_string(),
            ..Default::default()
        };
        let details = bytes::Bytes::from_owner(status.encode_to_vec());
        let status = tonic::Status::with_details(Code::Aborted, "test-only", details);
        let initial_handle = BidiReadHandle {
            handle: bytes::Bytes::from_static(b"initial-handle"),
        };
        let spec = BidiReadObjectSpec {
            routing_token: Some("initial-token".into()),
            read_handle: Some(initial_handle.clone()),
            ..Default::default()
        };
        let spec = Arc::new(Mutex::new(spec));

        let got = handle_redirect(spec.clone(), status);
        assert!(got.status().is_some(), "{got:?}");
        let guard = spec.lock().expect("not poisoned");
        assert_eq!(guard.routing_token.as_deref(), Some("initial-token"));
        assert_eq!(guard.read_handle, Some(initial_handle));
    }

    #[test_case(permanent_error(), false)]
    #[test_case(transient_error(), false)]
    #[test_case(non_grpc_abort_error(), false)]
    #[test_case(redirect_error("r1"), true)]
    #[test_case(to_gax_error(tonic::Status::aborted("without-details")), false)]
    #[test_case(
        to_gax_error(tonic::Status::with_details(
            Code::Aborted,
            "with bad details",
            bytes::Bytes::from_static(b"\x01")
        )),
        false
    )]
    fn redirect(input: Error, want: bool) {
        assert_eq!(is_redirect(&input), want, "{input:?}");
    }

    pub fn non_grpc_abort_error() -> Error {
        use gax::error::rpc::{Code, Status};
        Error::service(
            Status::default()
                .set_code(Code::Aborted)
                .set_message("aborted-not-gRPC"),
        )
    }
}
