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

use super::connector::AppendObjectSpecState;
use crate::Error;
use crate::google::rpc::Status as RpcStatus;
use crate::google::storage::v2::{AppendObjectSpec, BidiWriteObjectRedirectedError};
use gaxi::as_inner::as_inner;
use gaxi::grpc::from_status::to_gax_error;
use gaxi::grpc::tonic::Status;
use google_cloud_gax::error::rpc::Code;
use prost::Message;
use std::sync::{Arc, Mutex};

pub fn handle_redirect(state: Arc<Mutex<AppendObjectSpecState>>, status: Status) -> crate::Error {
    let Ok(rpc_status) = RpcStatus::decode(status.details()) else {
        return to_gax_error(status);
    };
    if let Some(redirect) = rpc_status
        .details
        .into_iter()
        .find_map(|d| d.to_msg::<BidiWriteObjectRedirectedError>().ok())
    {
        let mut guard = state.lock().expect("never poisoned");

        let (bucket, object) = match &*guard {
            AppendObjectSpecState::Write(spec) => {
                let bucket = spec
                    .resource
                    .as_ref()
                    .map(|r| r.bucket.clone())
                    .unwrap_or_default();
                let object = spec
                    .resource
                    .as_ref()
                    .map(|r| r.name.clone())
                    .unwrap_or_default();
                (bucket, object)
            }
            AppendObjectSpecState::Append(spec) => (spec.bucket.clone(), spec.object.clone()),
        };

        let mut new_spec = AppendObjectSpec {
            bucket,
            object,
            generation: redirect.generation.unwrap_or(0),
            routing_token: redirect.routing_token,
            write_handle: redirect.write_handle,
            ..Default::default()
        };

        if let AppendObjectSpecState::Append(old_spec) = &*guard
            && redirect.generation.is_none()
        {
            new_spec.generation = old_spec.generation;
        }

        *guard = AppendObjectSpecState::Append(new_spec);
        break;
    }
    to_gax_error(status)
}

/// Determine if an error is a redirect error.
///
/// Redirect payloads are attached to an `ABORTED` status, as mentioned in
/// the [gRPC documentation](https://docs.cloud.google.com/storage/docs/reference/rpc/google.storage.v2#bidiwriteobjectredirectederror).
///
/// Checking `Code::Aborted` first safely avoids the overhead of decoding
/// `RpcStatus` details for other error codes.
#[allow(dead_code)]
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
    use crate::google::storage::v2::BidiWriteHandle;
    use gaxi::grpc::tonic::Code;
    use test_case::test_case;

    #[test_case(Some("routing"), Some("handle"))]
    #[test_case(None, Some("handle"))]
    #[test_case(Some("routing"), None)]
    #[test_case(None, None)]
    fn reset(routing: Option<&str>, handle: Option<&str>) {
        let write_handle = handle.map(|s| BidiWriteHandle {
            handle: bytes::Bytes::from_owner(s.to_string()),
        });
        let redirect = BidiWriteObjectRedirectedError {
            routing_token: routing.map(str::to_string),
            write_handle: write_handle.clone(),
            generation: Some(42),
        };
        let redirect = prost_types::Any::from_msg(&redirect).unwrap();
        let status = RpcStatus {
            code: Code::Aborted as i32,
            message: "test-only".to_string(),
            details: vec![redirect],
        };
        let details = bytes::Bytes::from_owner(status.encode_to_vec());
        let status = Status::with_details(Code::Aborted, "test-only", details);
        let spec = AppendObjectSpec {
            bucket: "test-bucket".into(),
            object: "test-object".into(),
            generation: 1,
            routing_token: Some("initial-token".into()),
            write_handle: Some(BidiWriteHandle {
                handle: bytes::Bytes::from_static(b"initial-handle"),
            }),
            ..Default::default()
        };
        let state = Arc::new(Mutex::new(AppendObjectSpecState::Append(spec)));

        let got = handle_redirect(state.clone(), status);
        assert!(got.status().is_some(), "{got:?}");
        let guard = state.lock().expect("not poisoned");
        if let AppendObjectSpecState::Append(ref spec) = *guard {
            assert_eq!(spec.routing_token.as_deref(), routing);
            assert_eq!(spec.write_handle, write_handle);
            assert_eq!(spec.generation, 42);
        } else {
            panic!("Expected AppendObjectSpecState::Append");
        }
    }

    #[test]
    fn no_change() {
        let status = RpcStatus {
            code: Code::Aborted as i32,
            message: "test-only".to_string(),
            ..Default::default()
        };
        let details = bytes::Bytes::from_owner(status.encode_to_vec());
        let status = Status::with_details(Code::Aborted, "test-only", details);
        let initial_handle = BidiWriteHandle {
            handle: bytes::Bytes::from_static(b"initial-handle"),
        };
        let spec = AppendObjectSpec {
            bucket: "test-bucket".into(),
            object: "test-object".into(),
            generation: 1,
            routing_token: Some("initial-token".into()),
            write_handle: Some(initial_handle.clone()),
            ..Default::default()
        };
        let state = Arc::new(Mutex::new(AppendObjectSpecState::Append(spec)));

        let got = handle_redirect(state.clone(), status);
        assert!(got.status().is_some(), "{got:?}");
        let guard = state.lock().expect("not poisoned");
        if let AppendObjectSpecState::Append(ref spec) = *guard {
            assert_eq!(spec.routing_token.as_deref(), Some("initial-token"));
            assert_eq!(spec.write_handle, Some(initial_handle));
            assert_eq!(spec.generation, 1);
        } else {
            panic!("Expected AppendObjectSpecState::Append");
        }
    }

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
