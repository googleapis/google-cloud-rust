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

use crate::google::rpc::Status as RpcStatus;
use crate::google::storage::v2::{BidiReadObjectRedirectedError, BidiReadObjectSpec};
use gaxi::grpc::from_status::to_gax_error;
use prost::Message;
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

#[cfg(test)]
mod tests {
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
}
