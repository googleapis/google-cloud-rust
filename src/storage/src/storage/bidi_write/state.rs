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
    Write {
        spec: Box<WriteObjectSpec>,
        routing_token: Option<String>,
        initial_chunk: Option<bytes::Bytes>,
    },
    Append {
        spec: AppendObjectSpec,
        initial_chunk: Option<bytes::Bytes>,
    },
}

impl AppendObjectSpecState {
    pub(crate) fn handle_response(&mut self, m: &BidiWriteObjectResponse) {
        let mut new_generation = None;
        if let Some(WriteStatus::Resource(resource)) = &m.write_status {
            new_generation = Some(resource.generation);
        }

        match self {
            AppendObjectSpecState::Write {
                spec,
                routing_token,
                ..
            } => {
                let (bucket, object) = spec
                    .resource
                    .take()
                    .map(|r| (r.bucket, r.name))
                    .unwrap_or_default();
                *self = AppendObjectSpecState::Append {
                    spec: AppendObjectSpec {
                        bucket,
                        object,
                        generation: new_generation.unwrap_or(0),
                        write_handle: m.write_handle.clone(),
                        if_metageneration_match: spec.if_metageneration_match,
                        if_metageneration_not_match: spec.if_metageneration_not_match,
                        routing_token: routing_token.take(),
                    },
                    initial_chunk: None,
                };
            }
            AppendObjectSpecState::Append {
                spec,
                initial_chunk,
            } => {
                if let Some(g) = new_generation {
                    spec.generation = g;
                }
                if m.write_handle.is_some() {
                    spec.write_handle = m.write_handle.clone();
                }
                *initial_chunk = None;
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
                AppendObjectSpecState::Write {
                    spec,
                    routing_token,
                    initial_chunk,
                } => {
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
                        *self = AppendObjectSpecState::Append {
                            spec: new_spec,
                            initial_chunk: initial_chunk.take(),
                        };
                    } else {
                        *routing_token = redirect.routing_token;
                    }
                }
                AppendObjectSpecState::Append { spec, .. } => {
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
        // Arrange: Initial state before any response is received.
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
        let mut state = AppendObjectSpecState::Write {
            spec: Box::new(write_spec),
            routing_token: Some("token".into()),
            initial_chunk: Some(bytes::Bytes::from_static(b"initial-payload")),
        };

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

        // Act: Handle initial server response acknowledging the stream.
        state.handle_response(&response);

        // Assert: Transitions to Append state and releases initial_chunk memory buffer.
        if let AppendObjectSpecState::Append {
            ref spec,
            ref initial_chunk,
        } = state
        {
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
            // Verify buffer memory was released once server acknowledged the open request.
            assert!(initial_chunk.is_none());
        } else {
            panic!("Expected AppendObjectSpecState::Append");
        }
    }

    #[test]
    fn handle_response_append_updates() {
        use crate::google::storage::v2::Object;
        // Arrange: State already in Append mode with a pending initial chunk.
        let mut state = AppendObjectSpecState::Append {
            spec: AppendObjectSpec {
                bucket: "test-bucket".into(),
                object: "test-object".into(),
                generation: 1,
                routing_token: Some("token".into()),
                write_handle: Some(BidiWriteHandle {
                    handle: bytes::Bytes::from_static(b"initial-handle"),
                }),
                ..Default::default()
            },
            initial_chunk: Some(bytes::Bytes::from_static(b"initial-payload")),
        };

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

        // Act: Process subsequent response.
        state.handle_response(&response);

        // Assert: Updates generation and write handle, and clears any remaining initial_chunk.
        if let AppendObjectSpecState::Append {
            ref spec,
            ref initial_chunk,
        } = state
        {
            assert_eq!(spec.generation, 42);
            assert_eq!(
                spec.write_handle,
                Some(BidiWriteHandle {
                    handle: bytes::Bytes::from_static(b"new-handle"),
                })
            );
            assert_eq!(spec.routing_token.as_deref(), Some("token"));
            assert!(initial_chunk.is_none());
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
        let mut state = AppendObjectSpecState::Append {
            spec,
            initial_chunk: None,
        };

        let got = state.handle_redirect(status);
        assert!(got.status().is_some(), "{got:?}");
        if let AppendObjectSpecState::Append { ref spec, .. } = state {
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
        let mut state = AppendObjectSpecState::Write {
            spec: Box::new(write_spec),
            routing_token: None,
            initial_chunk: None,
        };

        let got = state.handle_redirect(status);
        assert!(got.status().is_some(), "{got:?}");
        if let AppendObjectSpecState::Append { ref spec, .. } = state {
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

        // Arrange: State in Write mode with an initial chunk pending transmission.
        let write_spec = WriteObjectSpec::default();
        let initial_chunk = bytes::Bytes::from_static(b"initial-payload");
        let mut state = AppendObjectSpecState::Write {
            spec: Box::new(write_spec),
            routing_token: None,
            initial_chunk: Some(initial_chunk.clone()),
        };

        // Act: Handle redirect status from server.
        let got = state.handle_redirect(status);
        assert!(got.status().is_some(), "{got:?}");

        // Assert: When transitioning from Write to Append on redirect, the initial_chunk
        // must be preserved so retry attempts can re-send the unacknowledged initial payload.
        if let AppendObjectSpecState::Append {
            initial_chunk: Some(ref data),
            ..
        } = state
        {
            assert_eq!(data, &initial_chunk);
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
        let mut state = AppendObjectSpecState::Append {
            spec,
            initial_chunk: None,
        };

        let got = state.handle_redirect(status);
        assert!(got.status().is_some(), "{got:?}");
        if let AppendObjectSpecState::Append { ref spec, .. } = state {
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
        let mut state = AppendObjectSpecState::Write {
            spec: Box::new(write_spec),
            routing_token: None,
            initial_chunk: None,
        };

        let got = state.handle_redirect(status);
        assert!(got.status().is_some(), "{got:?}");
        if let AppendObjectSpecState::Write {
            ref routing_token, ..
        } = state
        {
            assert_eq!(routing_token.as_deref(), Some("new-routing"));
        } else {
            panic!("Expected AppendObjectSpecState::Write");
        }
    }
}
