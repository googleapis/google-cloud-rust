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

use bytes::{Buf, Bytes};
use grpc::StatusCodeError;
use grpc::core::{RecvMessage, SendMessage, Trailers};

/// A [`SendMessage`] adapter that encodes Prost protobuf messages for `grpc-rust`.
// TODO(#5991): Will be used by bidi streaming in an upcoming commit.
#[allow(dead_code)]
pub(super) struct GrpcRustSend<T>(pub(super) T);

impl<T> SendMessage for GrpcRustSend<T>
where
    T: prost::Message,
{
    fn encode(&self) -> std::result::Result<Box<dyn Buf + Send + Sync>, String> {
        Ok(Box::new(Bytes::from(self.0.encode_to_vec())))
    }
}

/// A [`RecvMessage`] adapter that decodes raw byte payloads into Prost protobuf messages.
// TODO(#5991): Will be used by bidi streaming in an upcoming commit.
pub(super) struct GrpcRustRecv<T>(Option<T>);

impl<T> Default for GrpcRustRecv<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> GrpcRustRecv<T> {
    /// Creates a new unpopulated [`GrpcRustRecv`] receiver.
    #[allow(dead_code)]
    pub(super) const fn new() -> Self {
        Self(None)
    }

    /// Takes the decoded message.
    #[allow(dead_code)]
    pub(super) fn take(&mut self) -> tonic::Result<T> {
        self.0.take().ok_or_else(|| {
            tonic::Status::internal("grpc-rust response message missing or already taken")
        })
    }
}

impl<T> RecvMessage for GrpcRustRecv<T>
where
    T: prost::Message + Default,
{
    fn decode(&mut self, data: &mut dyn Buf) -> std::result::Result<(), String> {
        self.0 = Some(T::decode(data).map_err(|e| e.to_string())?);
        Ok(())
    }
}

/// Converts gRPC response [`Trailers`] into a [`tonic::Status`], preserving status code, message, metadata, and `grpc-status-details-bin`.
// TODO(#5991): Will be used by bidi streaming in an upcoming commit.
#[allow(dead_code)]
pub(super) fn trailers_to_tonic_status(trailers: Trailers) -> Option<tonic::Status> {
    let status = trailers.status().as_ref().err()?;
    let metadata: tonic::metadata::MetadataMap = trailers.metadata().clone().into();
    let details = metadata
        .get_bin("grpc-status-details-bin")
        .and_then(|value| value.to_bytes().ok())
        .unwrap_or_default();
    Some(tonic::Status::with_details_and_metadata(
        grpc_rust_error_to_tonic_code(status.code()),
        status.message().to_string(),
        details,
        metadata,
    ))
}

/// Maps a `grpc-rust` [`StatusCodeError`] to the corresponding [`tonic::Code`].
// TODO(#5991): Will be used by bidi streaming in an upcoming commit.
// TODO(#5991): Consider skipping conversion to `tonic::Code` by mapping
// directly to `google_cloud_gax` error types
// (https://github.com/googleapis/google-cloud-rust/pull/6082#discussion_r3603786027).
#[allow(dead_code)]
fn grpc_rust_error_to_tonic_code(code: StatusCodeError) -> tonic::Code {
    match code {
        StatusCodeError::Cancelled => tonic::Code::Cancelled,
        StatusCodeError::Unknown => tonic::Code::Unknown,
        StatusCodeError::InvalidArgument => tonic::Code::InvalidArgument,
        StatusCodeError::DeadlineExceeded => tonic::Code::DeadlineExceeded,
        StatusCodeError::NotFound => tonic::Code::NotFound,
        StatusCodeError::AlreadyExists => tonic::Code::AlreadyExists,
        StatusCodeError::PermissionDenied => tonic::Code::PermissionDenied,
        StatusCodeError::ResourceExhausted => tonic::Code::ResourceExhausted,
        StatusCodeError::FailedPrecondition => tonic::Code::FailedPrecondition,
        StatusCodeError::Aborted => tonic::Code::Aborted,
        StatusCodeError::OutOfRange => tonic::Code::OutOfRange,
        StatusCodeError::Unimplemented => tonic::Code::Unimplemented,
        StatusCodeError::Internal => tonic::Code::Internal,
        StatusCodeError::Unavailable => tonic::Code::Unavailable,
        StatusCodeError::DataLoss => tonic::Code::DataLoss,
        StatusCodeError::Unauthenticated => tonic::Code::Unauthenticated,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use grpc::StatusError;
    use grpc::metadata::MetadataValue;
    use pretty_assertions::assert_eq;
    use test_case::test_case;

    #[derive(Clone, PartialEq, prost::Message)]
    struct TestMessage {
        #[prost(string, tag = "1")]
        value: String,
    }

    #[test]
    fn prost_message_adapter_codecs() -> anyhow::Result<()> {
        // Arrange
        let want = TestMessage {
            value: "hello".to_string(),
        };

        // Act
        let mut encoded = GrpcRustSend(want.clone())
            .encode()
            .map_err(anyhow::Error::msg)?;
        let mut decoded = GrpcRustRecv::<TestMessage>::new();
        decoded.decode(&mut encoded).map_err(anyhow::Error::msg)?;
        let got = decoded.take()?;

        // Assert
        assert_eq!(got, want);
        Ok(())
    }

    #[test]
    fn trailers_to_tonic_status_preserves_details_and_metadata() {
        // Arrange
        const TEST_HEADER: &str = "x-test-header";
        const TEST_VALUE: &str = "test-value";
        const GRPC_STATUS_DETAILS_BIN: &str = "grpc-status-details-bin";
        const ERROR_MESSAGE_DENIED: &str = "denied";

        let details = b"status-details";
        let mut metadata = grpc::metadata::MetadataMap::new();
        metadata.insert_bin(GRPC_STATUS_DETAILS_BIN, MetadataValue::from_bytes(details));
        metadata.insert(TEST_HEADER, MetadataValue::from_static(TEST_VALUE));
        let trailers = Trailers::new(Err(StatusError::new(
            StatusCodeError::PermissionDenied,
            ERROR_MESSAGE_DENIED,
        )))
        .with_metadata(metadata);

        // Act
        let got = trailers_to_tonic_status(trailers).expect("non-OK trailers should map to status");

        // Assert
        assert_eq!(got.code(), tonic::Code::PermissionDenied);
        assert_eq!(got.message(), ERROR_MESSAGE_DENIED);
        assert_eq!(got.details(), details);
        assert_eq!(
            got.metadata()
                .get(TEST_HEADER)
                .and_then(|value| value.to_str().ok()),
            Some(TEST_VALUE)
        );
    }

    #[test_case(StatusCodeError::Cancelled, tonic::Code::Cancelled)]
    #[test_case(StatusCodeError::Unknown, tonic::Code::Unknown)]
    #[test_case(StatusCodeError::InvalidArgument, tonic::Code::InvalidArgument)]
    #[test_case(StatusCodeError::DeadlineExceeded, tonic::Code::DeadlineExceeded)]
    #[test_case(StatusCodeError::NotFound, tonic::Code::NotFound)]
    #[test_case(StatusCodeError::AlreadyExists, tonic::Code::AlreadyExists)]
    #[test_case(StatusCodeError::PermissionDenied, tonic::Code::PermissionDenied)]
    #[test_case(StatusCodeError::ResourceExhausted, tonic::Code::ResourceExhausted)]
    #[test_case(StatusCodeError::FailedPrecondition, tonic::Code::FailedPrecondition)]
    #[test_case(StatusCodeError::Aborted, tonic::Code::Aborted)]
    #[test_case(StatusCodeError::OutOfRange, tonic::Code::OutOfRange)]
    #[test_case(StatusCodeError::Unimplemented, tonic::Code::Unimplemented)]
    #[test_case(StatusCodeError::Internal, tonic::Code::Internal)]
    #[test_case(StatusCodeError::Unavailable, tonic::Code::Unavailable)]
    #[test_case(StatusCodeError::DataLoss, tonic::Code::DataLoss)]
    #[test_case(StatusCodeError::Unauthenticated, tonic::Code::Unauthenticated)]
    fn grpc_rust_error_to_tonic_code_maps_correctly(input: StatusCodeError, want: tonic::Code) {
        // Act & Assert
        assert_eq!(grpc_rust_error_to_tonic_code(input), want);
    }
}
