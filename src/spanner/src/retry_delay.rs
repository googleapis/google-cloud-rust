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

//! Helpers for extracting server-recommended retry delays from errors, status objects, and trailers.

// TODO(location-aware-routing): Remove allow(dead_code) once integrated into LocationRouter and TransactionRetryPolicy.
#![allow(dead_code)]

use crate::Error;
use crate::google::rpc::Status as ProtoStatus;
use base64::Engine as _;
use base64::prelude::{BASE64_STANDARD, BASE64_STANDARD_NO_PAD};
use gaxi::grpc::tonic::Status as TonicStatus;
use google_cloud_gax::error::rpc::{Status, StatusDetails};
use http::HeaderMap;
use prost::Message;
use prost_types::Duration as ProtoDuration;
use std::error::Error as _;
use std::time::Duration;

/// Fully-qualified protobuf type URL for `google.rpc.RetryInfo`.
pub(crate) const RETRY_INFO_TYPE_URL: &str = "type.googleapis.com/google.rpc.RetryInfo";

/// Binary metadata header key for `google.rpc.RetryInfo`.
pub(crate) const RETRY_INFO_BINARY_HEADER: &str = "google.rpc.retryinfo-bin";

/// Minimal protobuf representation of `google.rpc.RetryInfo` for unpacking server retry delay hints.
#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct ProtoRetryInfo {
    /// Minimum delay that clients should wait before retrying the operation.
    #[prost(message, optional, tag = "1")]
    pub retry_delay: Option<ProtoDuration>,
}

/// Extracts the server-recommended retry delay from an [`Error`], if present.
pub(crate) fn extract_retry_delay_from_error(error: &Error) -> Option<Duration> {
    if let Some(delay) = error.status().and_then(extract_retry_delay_from_status) {
        return Some(delay);
    }

    let mut current_source = error.source();
    while let Some(source) = current_source {
        if let Some(delay) = source
            .downcast_ref::<TonicStatus>()
            .and_then(extract_retry_delay_from_tonic_status)
        {
            return Some(delay);
        }
        current_source = source.source();
    }

    error
        .http_headers()
        .and_then(extract_retry_delay_from_headers)
}

/// Extracts the server-recommended retry delay from HTTP headers, if the `google.rpc.retryinfo-bin` header is present.
pub(crate) fn extract_retry_delay_from_headers(headers: &HeaderMap) -> Option<Duration> {
    let header_value = headers.get(RETRY_INFO_BINARY_HEADER)?;
    let decoded_bytes = BASE64_STANDARD
        .decode(header_value.as_bytes())
        .or_else(|_| BASE64_STANDARD_NO_PAD.decode(header_value.as_bytes()))
        .ok()?;
    extract_retry_delay_from_retry_info_bytes(&decoded_bytes)
}

/// Extracts the server-recommended retry delay from a GAX [`Status`], if present in its details.
pub(crate) fn extract_retry_delay_from_status(status: &Status) -> Option<Duration> {
    extract_retry_delay_from_status_details(&status.details)
}

/// Extracts the maximum valid server-recommended retry delay from an iterable collection of [`StatusDetails`].
pub(crate) fn extract_retry_delay_from_status_details<'a>(
    details: impl IntoIterator<Item = &'a StatusDetails>,
) -> Option<Duration> {
    details
        .into_iter()
        .filter_map(|detail| {
            let StatusDetails::RetryInfo(retry_info) = detail else {
                return None;
            };
            let wkt_duration = retry_info.retry_delay?;
            Duration::try_from(wkt_duration).ok()
        })
        .max()
}

/// Extracts the server-recommended retry delay from a gRPC [`TonicStatus`], if present in its details or metadata.
pub(crate) fn extract_retry_delay_from_tonic_status(status: &TonicStatus) -> Option<Duration> {
    let details_bytes = status.details();
    if !details_bytes.is_empty() {
        let delay_from_details = extract_retry_delay_from_proto_status_bytes(details_bytes)
            .or_else(|| extract_retry_delay_from_retry_info_bytes(details_bytes));
        if delay_from_details.is_some() {
            return delay_from_details;
        }
    }

    let trailer = status.metadata().get_bin(RETRY_INFO_BINARY_HEADER)?;
    let trailer_bytes = trailer.to_bytes().ok()?;
    extract_retry_delay_from_retry_info_bytes(&trailer_bytes)
}

/// Extracts the maximum valid server-recommended retry delay from serialized `google.rpc.Status` bytes.
pub(crate) fn extract_retry_delay_from_proto_status_bytes(bytes: &[u8]) -> Option<Duration> {
    let proto_status = ProtoStatus::decode(bytes).ok()?;
    proto_status
        .details
        .iter()
        .filter(|detail| detail.type_url == RETRY_INFO_TYPE_URL)
        .filter_map(|detail| extract_retry_delay_from_retry_info_bytes(&detail.value))
        .max()
}

/// Extracts the server-recommended retry delay from serialized `google.rpc.RetryInfo` bytes.
pub(crate) fn extract_retry_delay_from_retry_info_bytes(bytes: &[u8]) -> Option<Duration> {
    let retry_info = ProtoRetryInfo::decode(bytes).ok()?;
    let duration = retry_info.retry_delay?;
    Duration::try_from(duration).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use base64::prelude::{BASE64_STANDARD, BASE64_STANDARD_NO_PAD};
    use bytes::Bytes;
    use gaxi::grpc::tonic::{Code as TonicCode, MetadataMap};
    use google_cloud_gax::error::rpc::Code;
    use google_cloud_rpc::model::{ErrorInfo, RetryInfo};
    use http::HeaderMap;
    use http::header::{HeaderName, HeaderValue};
    use prost_types::Any as ProtoAny;
    use static_assertions::assert_impl_all;
    use std::fmt::Debug;

    assert_impl_all!(ProtoRetryInfo: Send, Sync, Debug, Clone);

    fn encode_test_retry_info(seconds: i64, nanos: i32) -> Vec<u8> {
        let mut bytes = Vec::new();
        ProtoRetryInfo {
            retry_delay: Some(ProtoDuration { seconds, nanos }),
        }
        .encode(&mut bytes)
        .expect("serialization of ProtoRetryInfo must succeed");
        bytes
    }

    #[test]
    fn extract_retry_delay_from_error_and_status() {
        let retry_info = RetryInfo::default().set_retry_delay(wkt::Duration::clamp(3, 250_000_000));
        let status = Status::default()
            .set_code(Code::ResourceExhausted)
            .set_message("Resource exhausted")
            .set_details(vec![StatusDetails::RetryInfo(retry_info)]);

        let extracted_from_status = extract_retry_delay_from_status(&status);
        assert_eq!(
            extracted_from_status,
            Some(Duration::new(3, 250_000_000)),
            "must extract matching duration from status"
        );

        let error = Error::service(status);
        let extracted_from_error = extract_retry_delay_from_error(&error);
        assert_eq!(
            extracted_from_error,
            Some(Duration::new(3, 250_000_000)),
            "must extract matching duration from Error"
        );
    }

    #[test]
    fn extract_retry_delay_from_proto_status_bytes_and_tonic() {
        let retry_info_bytes = encode_test_retry_info(4, 123_456_789);

        let direct_delay = extract_retry_delay_from_retry_info_bytes(&retry_info_bytes);
        assert_eq!(
            direct_delay,
            Some(Duration::new(4, 123_456_789)),
            "must extract matching duration from raw retry info bytes"
        );

        let any_detail = ProtoAny {
            type_url: RETRY_INFO_TYPE_URL.to_string(),
            value: retry_info_bytes,
        };
        let proto_status = ProtoStatus {
            code: 8, // RESOURCE_EXHAUSTED
            message: "Quota exceeded".to_string(),
            details: vec![any_detail],
        };
        let mut status_bytes = Vec::new();
        proto_status
            .encode(&mut status_bytes)
            .expect("serialization of ProtoStatus must succeed");

        let status_delay = extract_retry_delay_from_proto_status_bytes(&status_bytes);
        assert_eq!(
            status_delay,
            Some(Duration::new(4, 123_456_789)),
            "must extract matching duration from serialized status bytes"
        );

        let tonic_status = TonicStatus::with_details_and_metadata(
            TonicCode::ResourceExhausted,
            "Quota exceeded",
            status_bytes.into(),
            MetadataMap::new(),
        );
        let tonic_delay = extract_retry_delay_from_tonic_status(&tonic_status);
        assert_eq!(
            tonic_delay,
            Some(Duration::new(4, 123_456_789)),
            "must extract matching duration from tonic::Status details"
        );
    }

    #[test]
    fn extract_retry_delay_edge_cases() {
        // Empty details
        let empty_status = Status::default().set_code(Code::ResourceExhausted);
        assert_eq!(
            extract_retry_delay_from_status(&empty_status),
            None,
            "status without details should yield None"
        );

        // Details with other types (e.g. ErrorInfo)
        let error_info = ErrorInfo::default()
            .set_reason("RATE_LIMIT_EXCEEDED")
            .set_domain("spanner.googleapis.com");
        let other_status = Status::default()
            .set_code(Code::ResourceExhausted)
            .set_details(vec![StatusDetails::ErrorInfo(error_info)]);
        assert_eq!(
            extract_retry_delay_from_status(&other_status),
            None,
            "status with only ErrorInfo should yield None"
        );

        // Negative duration in ProtoRetryInfo
        let negative_bytes = encode_test_retry_info(-1, 0);
        assert_eq!(
            extract_retry_delay_from_retry_info_bytes(&negative_bytes),
            None,
            "negative duration in retry info should yield None"
        );

        // Malformed bytes
        let malformed_bytes = vec![0xFF, 0xFE, 0xFD];
        assert_eq!(
            extract_retry_delay_from_proto_status_bytes(&malformed_bytes),
            None,
            "malformed status bytes should yield None"
        );
        assert_eq!(
            extract_retry_delay_from_retry_info_bytes(&malformed_bytes),
            None,
            "malformed retry info bytes should yield None"
        );

        // Empty tonic status
        let empty_tonic = TonicStatus::new(TonicCode::ResourceExhausted, "Quota exceeded");
        assert_eq!(
            extract_retry_delay_from_tonic_status(&empty_tonic),
            None,
            "tonic status without details should yield None"
        );

        // Multiple RetryInfo details selects the maximum duration
        let retry_info_small = RetryInfo::default().set_retry_delay(wkt::Duration::clamp(1, 0));
        let retry_info_large =
            RetryInfo::default().set_retry_delay(wkt::Duration::clamp(5, 500_000_000));
        let multiple_status = Status::default().set_details(vec![
            StatusDetails::RetryInfo(retry_info_small),
            StatusDetails::RetryInfo(retry_info_large),
        ]);
        assert_eq!(
            extract_retry_delay_from_status(&multiple_status),
            Some(Duration::new(5, 500_000_000)),
            "multiple retry info details must select maximum duration"
        );
    }

    #[test]
    fn extract_retry_delay_from_tonic_binary_trailer() {
        let retry_info_bytes = encode_test_retry_info(7, 890_000_000);

        let mut headers = HeaderMap::new();
        let base64_encoded = BASE64_STANDARD.encode(&retry_info_bytes);
        headers.insert(
            HeaderName::from_static(RETRY_INFO_BINARY_HEADER),
            HeaderValue::from_str(&base64_encoded).expect("valid header value"),
        );
        let metadata = MetadataMap::from_headers(headers);

        let tonic_status =
            TonicStatus::with_metadata(TonicCode::ResourceExhausted, "Rate limited", metadata);

        let extracted_delay = extract_retry_delay_from_tonic_status(&tonic_status);
        assert_eq!(
            extracted_delay,
            Some(Duration::new(7, 890_000_000)),
            "must extract matching duration from binary metadata trailer"
        );
    }

    #[test]
    fn extract_retry_delay_from_http_headers_and_error() {
        let retry_info_bytes = encode_test_retry_info(5, 500_000_000);

        let mut headers = HeaderMap::new();
        let base64_encoded = BASE64_STANDARD.encode(&retry_info_bytes);
        headers.insert(
            HeaderName::from_static(RETRY_INFO_BINARY_HEADER),
            HeaderValue::from_str(&base64_encoded).expect("valid header value"),
        );

        let delay_from_headers = extract_retry_delay_from_headers(&headers);
        assert_eq!(
            delay_from_headers,
            Some(Duration::new(5, 500_000_000)),
            "must extract matching duration from HeaderMap"
        );

        // Error::http populates ErrorKind::Transport with http_headers, but has no status() or TonicStatus
        let http_error = Error::http(429, headers, Bytes::new());
        let delay_from_error = extract_retry_delay_from_error(&http_error);
        assert_eq!(
            delay_from_error,
            Some(Duration::new(5, 500_000_000)),
            "must extract matching duration from http_headers fallback on Error"
        );
    }

    #[test]
    fn extract_retry_delay_from_unpadded_base64_http_headers() {
        let retry_info_bytes = encode_test_retry_info(3, 250_000_000);

        let mut headers = HeaderMap::new();
        let unpadded_base64 = BASE64_STANDARD_NO_PAD.encode(&retry_info_bytes);
        headers.insert(
            HeaderName::from_static(RETRY_INFO_BINARY_HEADER),
            HeaderValue::from_str(&unpadded_base64).expect("valid header value"),
        );

        let delay_from_headers = extract_retry_delay_from_headers(&headers);
        assert_eq!(
            delay_from_headers,
            Some(Duration::new(3, 250_000_000)),
            "must extract matching duration from unpadded base64 HeaderMap"
        );
    }

    #[test]
    fn extract_retry_delay_from_error_with_tonic_status_in_source() {
        let retry_info_bytes = encode_test_retry_info(6, 750_000_000);
        let tonic_status = TonicStatus::with_details_and_metadata(
            TonicCode::ResourceExhausted,
            "Rate limited",
            retry_info_bytes.into(),
            MetadataMap::new(),
        );
        let transport_error = Error::transport(HeaderMap::new(), tonic_status);
        let delay = extract_retry_delay_from_error(&transport_error);
        assert_eq!(
            delay,
            Some(Duration::new(6, 750_000_000)),
            "must extract delay from TonicStatus inside error source chain"
        );
    }

    #[derive(Debug)]
    struct NestedErrorWrapper(TonicStatus);

    impl std::fmt::Display for NestedErrorWrapper {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "nested error wrapper")
        }
    }

    impl std::error::Error for NestedErrorWrapper {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(&self.0)
        }
    }

    #[test]
    fn extract_retry_delay_from_error_with_nested_source() {
        let retry_info_bytes = encode_test_retry_info(2, 500_000_000);
        let tonic_status = TonicStatus::with_details_and_metadata(
            TonicCode::ResourceExhausted,
            "Nested rate limit",
            retry_info_bytes.into(),
            MetadataMap::new(),
        );
        let nested = NestedErrorWrapper(tonic_status);
        let timeout_error = Error::timeout(nested);
        let delay = extract_retry_delay_from_error(&timeout_error);
        assert_eq!(
            delay,
            Some(Duration::new(2, 500_000_000)),
            "must extract delay through multi-level error source chain"
        );
    }

    #[test]
    fn extract_retry_delay_from_error_returns_none_for_clean_error() {
        let exhausted_error = Error::exhausted("an error without status or headers");
        assert_eq!(
            extract_retry_delay_from_error(&exhausted_error),
            None,
            "clean error without status, tonic source, or headers must yield None"
        );
    }

    #[test]
    fn extract_retry_delay_from_headers_edge_cases() {
        // 1. Empty headers
        assert_eq!(
            extract_retry_delay_from_headers(&HeaderMap::new()),
            None,
            "empty HeaderMap should yield None"
        );

        // 2. Invalid base64 in header
        let mut invalid_headers = HeaderMap::new();
        invalid_headers.insert(
            HeaderName::from_static(RETRY_INFO_BINARY_HEADER),
            HeaderValue::from_static("!!!not-valid-base64!!!"),
        );
        assert_eq!(
            extract_retry_delay_from_headers(&invalid_headers),
            None,
            "invalid base64 in header should yield None"
        );

        // 3. Valid base64 but corrupted protobuf payload
        let mut corrupted_headers = HeaderMap::new();
        let corrupted_base64 = BASE64_STANDARD.encode([0xFF, 0xFE, 0xFD]);
        corrupted_headers.insert(
            HeaderName::from_static(RETRY_INFO_BINARY_HEADER),
            HeaderValue::from_str(&corrupted_base64).expect("valid header value"),
        );
        assert_eq!(
            extract_retry_delay_from_headers(&corrupted_headers),
            None,
            "corrupted protobuf payload in base64 header should yield None"
        );

        // 4. Valid base64 and valid ProtoRetryInfo but retry_delay is None
        let mut empty_retry_info_bytes = Vec::new();
        ProtoRetryInfo { retry_delay: None }
            .encode(&mut empty_retry_info_bytes)
            .expect("encoding empty ProtoRetryInfo must succeed");
        let mut none_delay_headers = HeaderMap::new();
        let none_delay_base64 = BASE64_STANDARD.encode(&empty_retry_info_bytes);
        none_delay_headers.insert(
            HeaderName::from_static(RETRY_INFO_BINARY_HEADER),
            HeaderValue::from_str(&none_delay_base64).expect("valid header value"),
        );
        assert_eq!(
            extract_retry_delay_from_headers(&none_delay_headers),
            None,
            "ProtoRetryInfo without retry_delay in header should yield None"
        );
    }

    #[test]
    fn extract_retry_delay_from_status_details_more_edge_cases() {
        // RetryInfo with retry_delay == None
        let empty_retry_info = RetryInfo::default();
        let status =
            Status::default().set_details(vec![StatusDetails::RetryInfo(empty_retry_info)]);
        assert_eq!(
            extract_retry_delay_from_status(&status),
            None,
            "RetryInfo without retry_delay must yield None"
        );

        // Empty iterator
        assert_eq!(
            extract_retry_delay_from_status_details(Vec::<StatusDetails>::new().iter()),
            None,
            "empty status details iterator must yield None"
        );
    }

    #[test]
    fn extract_retry_delay_from_tonic_status_direct_retry_info_details() {
        let retry_info_bytes = encode_test_retry_info(8, 100_000_000);
        // Direct ProtoRetryInfo bytes in tonic::Status details (not wrapped in ProtoStatus)
        let tonic_status = TonicStatus::with_details_and_metadata(
            TonicCode::ResourceExhausted,
            "Direct retry info in details",
            retry_info_bytes.into(),
            MetadataMap::new(),
        );
        let extracted = extract_retry_delay_from_tonic_status(&tonic_status);
        assert_eq!(
            extracted,
            Some(Duration::new(8, 100_000_000)),
            "must extract delay from direct ProtoRetryInfo details bytes fallback"
        );
    }

    #[test]
    fn extract_retry_delay_from_proto_status_bytes_with_non_matching_type_url() {
        let any_other = ProtoAny {
            type_url: "type.googleapis.com/google.rpc.BadRequest".to_string(),
            value: vec![1, 2, 3],
        };
        let proto_status = ProtoStatus {
            code: 8,
            message: "Some error".to_string(),
            details: vec![any_other],
        };
        let mut status_bytes = Vec::new();
        proto_status
            .encode(&mut status_bytes)
            .expect("encoding proto status must succeed");
        assert_eq!(
            extract_retry_delay_from_proto_status_bytes(&status_bytes),
            None,
            "proto status without RetryInfo type_url must yield None"
        );
    }

    #[test]
    fn extract_retry_delay_from_retry_info_bytes_with_none_duration() {
        let mut bytes = Vec::new();
        ProtoRetryInfo { retry_delay: None }
            .encode(&mut bytes)
            .expect("encoding ProtoRetryInfo with None delay must succeed");
        assert_eq!(
            extract_retry_delay_from_retry_info_bytes(&bytes),
            None,
            "ProtoRetryInfo without retry_delay must yield None"
        );
    }
}
