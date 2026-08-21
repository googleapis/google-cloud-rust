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

use crate::google::rpc::Status as RpcStatus;
use crate::google::storage::v2::{
    AppendObjectSpec, BidiWriteObjectRedirectedError, BidiWriteObjectResponse, WriteObjectSpec,
    bidi_write_object_response::WriteStatus,
};
use gaxi::grpc::from_status::to_gax_error;
use gaxi::grpc::tonic::Status;
use prost::Message;

/// Represents the state of the initial request in the stream.
#[derive(Clone, Debug)]
pub enum AppendObjectSpecState {
    Write(Box<WriteObjectSpec>, Option<String>, Option<bytes::Bytes>),
    Append(AppendObjectSpec, Option<bytes::Bytes>),
}

impl AppendObjectSpecState {
    pub(crate) fn handle_response(&mut self, m: &BidiWriteObjectResponse) {
        let mut new_generation = None;
        if let Some(WriteStatus::Resource(resource)) = &m.write_status {
            new_generation = Some(resource.generation);
        }

        match self {
            AppendObjectSpecState::Write(spec, token, data) => {
                let (bucket, object) = spec
                    .resource
                    .take()
                    .map(|r| (r.bucket, r.name))
                    .unwrap_or_default();
                *self = AppendObjectSpecState::Append(
                    AppendObjectSpec {
                        bucket,
                        object,
                        generation: new_generation.unwrap_or(0),
                        write_handle: m.write_handle.clone(),
                        if_metageneration_match: spec.if_metageneration_match,
                        if_metageneration_not_match: spec.if_metageneration_not_match,
                        routing_token: token.take(),
                    },
                    data.take(),
                );
            }
            AppendObjectSpecState::Append(spec, _data) => {
                if let Some(g) = new_generation {
                    spec.generation = g;
                }
                if m.write_handle.is_some() {
                    spec.write_handle = m.write_handle.clone();
                }
            }
        }
    }

    pub(crate) fn handle_redirect(&mut self, status: Status) -> crate::Error {
        let Ok(rpc_status) = RpcStatus::decode(status.details()) else {
            return to_gax_error(status);
        };
        if let Some(redirect) = rpc_status
            .details
            .into_iter()
            .find_map(|d| d.to_msg::<BidiWriteObjectRedirectedError>().ok())
        {
            match self {
                AppendObjectSpecState::Write(spec, token, data) => {
                    if let Some(generation) = redirect.generation {
                        let (bucket, object) = spec
                            .resource
                            .take()
                            .map(|r| (r.bucket, r.name))
                            .unwrap_or_default();
                        let new_spec = AppendObjectSpec {
                            bucket,
                            object,
                            generation,
                            if_metageneration_match: spec.if_metageneration_match,
                            if_metageneration_not_match: spec.if_metageneration_not_match,
                            routing_token: redirect.routing_token,
                            write_handle: redirect.write_handle,
                        };
                        *self = AppendObjectSpecState::Append(new_spec, data.take());
                    } else {
                        *token = redirect.routing_token;
                    }
                }
                AppendObjectSpecState::Append(spec, _data) => {
                    spec.routing_token = redirect.routing_token;
                    spec.write_handle = redirect.write_handle;
                    if let Some(g) = redirect.generation {
                        spec.generation = g;
                    }
                }
            }
        }
        to_gax_error(status)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::google::storage::v2::BidiWriteHandle;
    use gaxi::grpc::tonic::Code;
    use test_case::test_case;

    #[test]
    fn handle_response_write_to_append() {
        use crate::google::storage::v2::{Object, WriteObjectSpec};
        let write_spec = WriteObjectSpec {
            resource: Some(Object {
                bucket: "test-bucket".into(),
                name: "test-object".into(),
                ..Default::default()
            }),
            if_metageneration_match: Some(10),
            if_metageneration_not_match: Some(20),
            ..Default::default()
        };
        let mut state =
            AppendObjectSpecState::Write(Box::new(write_spec), Some("token".into()), None);

        let response = BidiWriteObjectResponse {
            write_status: Some(WriteStatus::Resource(Object {
                generation: 42,
                ..Default::default()
            })),
            write_handle: Some(BidiWriteHandle {
                handle: bytes::Bytes::from_static(b"new-handle"),
            }),
            ..Default::default()
        };

        state.handle_response(&response);

        if let AppendObjectSpecState::Append(ref spec, ref data) = state {
            assert_eq!(spec.bucket, "test-bucket");
            assert_eq!(spec.object, "test-object");
            assert_eq!(spec.generation, 42);
            assert_eq!(spec.if_metageneration_match, Some(10));
            assert_eq!(spec.if_metageneration_not_match, Some(20));
            assert_eq!(spec.routing_token.as_deref(), Some("token"));
            assert_eq!(
                spec.write_handle,
                Some(BidiWriteHandle {
                    handle: bytes::Bytes::from_static(b"new-handle"),
                })
            );
            assert!(data.is_none());
        } else {
            panic!("Expected AppendObjectSpecState::Append");
        }
    }

    #[test]
    fn handle_response_append_updates() {
        use crate::google::storage::v2::Object;
        let mut state = AppendObjectSpecState::Append(
            AppendObjectSpec {
                bucket: "test-bucket".into(),
                object: "test-object".into(),
                generation: 1,
                routing_token: Some("token".into()),
                write_handle: Some(BidiWriteHandle {
                    handle: bytes::Bytes::from_static(b"initial-handle"),
                }),
                ..Default::default()
            },
            None,
        );

        let response = BidiWriteObjectResponse {
            write_status: Some(WriteStatus::Resource(Object {
                generation: 42,
                ..Default::default()
            })),
            write_handle: Some(BidiWriteHandle {
                handle: bytes::Bytes::from_static(b"new-handle"),
            }),
            ..Default::default()
        };

        state.handle_response(&response);

        if let AppendObjectSpecState::Append(ref spec, ref data) = state {
            assert_eq!(spec.generation, 42);
            assert_eq!(
                spec.write_handle,
                Some(BidiWriteHandle {
                    handle: bytes::Bytes::from_static(b"new-handle"),
                })
            );
            assert_eq!(spec.routing_token.as_deref(), Some("token"));
            assert!(data.is_none());
        } else {
            panic!("Expected AppendObjectSpecState::Append");
        }
    }

    #[test_case(Some("routing"), Some("handle"))]
    #[test_case(None, Some("handle"))]
    #[test_case(Some("routing"), None)]
    #[test_case(None, None)]
    fn handle_redirect_reset(routing: Option<&str>, handle: Option<&str>) {
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
            if_metageneration_match: Some(10),
            if_metageneration_not_match: Some(20),
        };
        let mut state = AppendObjectSpecState::Append(spec, None);

        let got = state.handle_redirect(status);
        assert!(got.status().is_some(), "{got:?}");
        if let AppendObjectSpecState::Append(ref spec, _) = state {
            assert_eq!(spec.routing_token.as_deref(), routing);
            assert_eq!(spec.write_handle, write_handle);
            assert_eq!(spec.generation, 42);
            assert_eq!(spec.if_metageneration_match, Some(10));
            assert_eq!(spec.if_metageneration_not_match, Some(20));
        } else {
            panic!("Expected AppendObjectSpecState::Append");
        }
    }

    #[test]
    fn handle_redirect_preserves_preconditions_on_write() {
        use crate::google::storage::v2::WriteObjectSpec;
        let redirect = BidiWriteObjectRedirectedError {
            routing_token: Some("new-routing".to_string()),
            write_handle: Some(BidiWriteHandle {
                handle: bytes::Bytes::from_static(b"new-handle"),
            }),
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

        let write_spec = WriteObjectSpec {
            if_metageneration_match: Some(11),
            if_metageneration_not_match: Some(22),
            ..Default::default()
        };
        let mut state = AppendObjectSpecState::Write(Box::new(write_spec), None, None);

        let got = state.handle_redirect(status);
        assert!(got.status().is_some(), "{got:?}");
        if let AppendObjectSpecState::Append(ref spec, _) = state {
            assert_eq!(spec.routing_token.as_deref(), Some("new-routing"));
            assert_eq!(spec.generation, 42);
            assert_eq!(spec.if_metageneration_match, Some(11));
            assert_eq!(spec.if_metageneration_not_match, Some(22));
        } else {
            panic!("Expected AppendObjectSpecState::Append");
        }
    }

    #[test]
    fn handle_redirect_preserves_initial_data_on_write() {
        use crate::google::storage::v2::WriteObjectSpec;
        let redirect = BidiWriteObjectRedirectedError {
            routing_token: Some("new-routing".to_string()),
            write_handle: Some(BidiWriteHandle {
                handle: bytes::Bytes::from_static(b"new-handle"),
            }),
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

        let write_spec = WriteObjectSpec::default();
        let initial_chunk = bytes::Bytes::from_static(b"initial-payload");
        let mut state =
            AppendObjectSpecState::Write(Box::new(write_spec), None, Some(initial_chunk.clone()));

        let got = state.handle_redirect(status);
        assert!(got.status().is_some(), "{got:?}");
        if let AppendObjectSpecState::Append(ref _spec, ref data) = state {
            assert_eq!(data.as_ref(), Some(&initial_chunk));
        } else {
            panic!("Expected AppendObjectSpecState::Append");
        }
    }

    #[test]
    fn handle_redirect_no_change() {
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
        let mut state = AppendObjectSpecState::Append(spec, None);

        let got = state.handle_redirect(status);
        assert!(got.status().is_some(), "{got:?}");
        if let AppendObjectSpecState::Append(ref spec, _) = state {
            assert_eq!(spec.routing_token.as_deref(), Some("initial-token"));
            assert_eq!(spec.write_handle, Some(initial_handle));
            assert_eq!(spec.generation, 1);
        } else {
            panic!("Expected AppendObjectSpecState::Append");
        }
    }

    #[test]
    fn handle_redirect_write_no_generation() {
        use crate::google::storage::v2::WriteObjectSpec;
        let redirect = BidiWriteObjectRedirectedError {
            routing_token: Some("new-routing".to_string()),
            write_handle: None,
            generation: None,
        };
        let redirect = prost_types::Any::from_msg(&redirect).unwrap();
        let status = RpcStatus {
            code: Code::Aborted as i32,
            message: "test-only".to_string(),
            details: vec![redirect],
        };
        let details = bytes::Bytes::from_owner(status.encode_to_vec());
        let status = Status::with_details(Code::Aborted, "test-only", details);

        let write_spec = WriteObjectSpec::default();
        let mut state = AppendObjectSpecState::Write(Box::new(write_spec), None, None);

        let got = state.handle_redirect(status);
        assert!(got.status().is_some(), "{got:?}");
        if let AppendObjectSpecState::Write(_, ref token, _) = state {
            assert_eq!(token.as_deref(), Some("new-routing"));
        } else {
            panic!("Expected AppendObjectSpecState::Write");
        }
    }
}
