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

// TODO(#5991): Switch to grpc-rust's `tonic::metadata::MetadataMap` conversions
// once `grpc` comes from crates.io.

use google_cloud_gax::Result as GaxResult;
use google_cloud_gax::error::Error;
use grpc::metadata::{
    Ascii as GrpcAscii, AsciiMetadataValue as GrpcAsciiMetadataValue, Binary as GrpcBinary,
    KeyAndValueRef as GrpcKeyAndValueRef, MetadataKey as GrpcMetadataKey,
    MetadataMap as GrpcMetadataMap, MetadataValue as GrpcMetadataValue,
};
use http::HeaderMap;
use tonic::metadata::{
    KeyAndValueRef as TonicKeyAndValueRef, MetadataKey as TonicMetadataKey,
    MetadataMap as TonicMetadataMap, MetadataValue as TonicMetadataValue,
};

/// Converts a [`HeaderMap`] into a [`GrpcMetadataMap`].
///
/// Decodes base64 `-bin` values into raw bytes so `grpc-rust` doesn't
/// double-encode them on the wire.
pub(super) fn from_header_map(headers: HeaderMap) -> GaxResult<GrpcMetadataMap> {
    let mut grpc_map = GrpcMetadataMap::new();
    let tonic_map = TonicMetadataMap::from_headers(headers);
    for item in tonic_map.iter() {
        match item {
            TonicKeyAndValueRef::Binary(key, value) => {
                let k = GrpcMetadataKey::<GrpcBinary>::from_bytes(key.as_str().as_bytes())
                    .map_err(invalid_metadata("name", key.as_str()))?;
                let raw = value
                    .to_bytes()
                    .map_err(invalid_metadata("base64 value", key.as_str()))?;
                grpc_map.append_bin(k, GrpcMetadataValue::from_bytes(&raw));
            }
            TonicKeyAndValueRef::Ascii(key, value) => {
                // The gRPC wire specification requires ASCII metadata values to have
                // a non-zero length.
                if value.is_empty() {
                    continue;
                }
                let k = GrpcMetadataKey::<GrpcAscii>::from_bytes(key.as_str().as_bytes())
                    .map_err(invalid_metadata("name", key.as_str()))?;
                let val = GrpcAsciiMetadataValue::try_from(value.as_encoded_bytes())
                    .map_err(invalid_metadata("ASCII value", key.as_str()))?;
                grpc_map.append(k, val);
            }
        }
    }
    Ok(grpc_map)
}

/// Wraps a metadata error in [`Error::ser`], naming `key` without exposing its
/// potentially sensitive value.
fn invalid_metadata<E: std::fmt::Display>(
    what: &'static str,
    key: &str,
) -> impl FnOnce(E) -> Error {
    move |e| Error::ser(format!("invalid {what} for metadata key `{key}`: {e}"))
}

/// Converts a [`GrpcMetadataMap`] into a [`TonicMetadataMap`].
pub(super) fn to_tonic_map(metadata: &GrpcMetadataMap) -> TonicMetadataMap {
    let mut tonic_map = TonicMetadataMap::new();
    for item in metadata.iter() {
        match item {
            GrpcKeyAndValueRef::Ascii(key, val) => {
                let name = TonicMetadataKey::from_bytes(key.as_str().as_bytes())
                    .expect("grpc-rust guarantees valid metadata keys");
                let val = TonicMetadataValue::try_from(val.to_str().as_bytes())
                    .expect("grpc-rust guarantees valid ascii metadata values");
                tonic_map.append(name, val);
            }
            GrpcKeyAndValueRef::Binary(key, val) => {
                let name = TonicMetadataKey::from_bytes(key.as_str().as_bytes())
                    .expect("grpc-rust guarantees valid metadata keys");
                let val = TonicMetadataValue::from_bytes(val.as_bytes());
                tonic_map.append_bin(name, val);
            }
        }
    }
    tonic_map
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::header::{HeaderName, HeaderValue};
    use pretty_assertions::assert_eq;
    use test_case::test_case;

    const ASCII_KEY: &str = "x-test-header";
    const ASCII_VAL: &str = "test-value";
    const BIN_KEY: &str = "x-test-header-bin";
    const BIN_VAL: &[u8] = b"test-binary-data";

    #[test_case("dGVzdC1iaW5hcnktZGF0YQ"; "unpadded base64")]
    #[test_case("dGVzdC1iaW5hcnktZGF0YQ=="; "padded base64")]
    fn test_from_header_map(bin_wire_val: &'static str) {
        // Arrange
        let mut headers = HeaderMap::new();

        headers.insert(
            HeaderName::from_static(ASCII_KEY),
            HeaderValue::from_static(ASCII_VAL),
        );
        // `HeaderMap` holds `-bin` values in base64 wire form.
        headers.insert(
            HeaderName::from_static(BIN_KEY),
            HeaderValue::from_static(bin_wire_val),
        );

        // Act
        let grpc_map = from_header_map(headers).expect("conversion succeeded");

        // Assert
        let ascii_val = grpc_map.get(ASCII_KEY).expect("ascii header present");
        assert_eq!(ascii_val.to_str(), ASCII_VAL);

        let bin_val = grpc_map.get_bin(BIN_KEY).expect("binary header present");
        assert_eq!(bin_val.as_bytes(), BIN_VAL);
    }

    #[test]
    fn test_to_tonic_map() {
        // Arrange
        let mut grpc_map = GrpcMetadataMap::new();

        let ascii_key =
            GrpcMetadataKey::<GrpcAscii>::from_bytes(ASCII_KEY.as_bytes()).expect("valid key");
        let ascii_val = GrpcAsciiMetadataValue::try_from(ASCII_VAL).expect("valid ascii value");
        grpc_map.append(ascii_key, ascii_val);

        let bin_key =
            GrpcMetadataKey::<GrpcBinary>::from_bytes(BIN_KEY.as_bytes()).expect("valid key");
        let bin_val = GrpcMetadataValue::from_bytes(BIN_VAL);
        grpc_map.append_bin(bin_key, bin_val);

        // Act
        let tonic_map = to_tonic_map(&grpc_map);

        // Assert
        let tonic_ascii = tonic_map.get(ASCII_KEY).expect("ascii header in tonic");
        assert_eq!(tonic_ascii.to_str().expect("valid str"), ASCII_VAL);

        let tonic_bin = tonic_map.get_bin(BIN_KEY).expect("binary header in tonic");
        assert_eq!(
            tonic_bin.to_bytes().expect("valid bytes"),
            bytes::Bytes::from_static(BIN_VAL)
        );
    }

    #[test_case(&[0xFA]; "non-ascii byte")]
    #[test_case(b"  "; "whitespace only")]
    fn test_from_header_map_invalid_ascii_value_returns_error(value: &[u8]) {
        // Arrange
        const KEY: &str = "x-invalid-ascii";
        let mut headers = HeaderMap::new();
        let invalid_val = HeaderValue::from_bytes(value).expect("valid http header value");
        headers.insert(HeaderName::from_static(KEY), invalid_val);

        // Act
        let result = from_header_map(headers);

        // Assert
        let err = result.expect_err("invalid ascii value should be rejected");
        assert!(err.is_serialization(), "{err:?}");
        assert!(err.to_string().contains(KEY), "{err}");
    }

    #[test]
    fn test_from_header_map_invalid_base64_value_returns_error() {
        // Arrange
        const INVALID_VAL: &str = "not base64!";
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static(BIN_KEY),
            HeaderValue::from_static(INVALID_VAL),
        );

        // Act
        let result = from_header_map(headers);

        // Assert
        let err = result.expect_err("invalid base64 should be rejected");
        assert!(err.is_serialization(), "{err:?}");
        let msg = err.to_string();
        assert!(msg.contains(BIN_KEY) && msg.contains("base64"), "{msg}");
        // Never leak potentially sensitive header values in errors.
        assert!(!msg.contains(INVALID_VAL), "{msg}");
    }

    // `!` is valid in HTTP header names, but not in gRPC metadata keys.
    #[test_case("x-invalid!"; "ascii key")]
    #[test_case("x-invalid!-bin"; "binary key")]
    fn test_from_header_map_invalid_key_returns_error(key: &'static str) {
        // Arrange
        let mut headers = HeaderMap::new();
        // Use a value that is valid as both ASCII and base64 so only the key fails.
        headers.insert(
            HeaderName::from_static(key),
            HeaderValue::from_static("dGVzdA"),
        );

        // Act
        let result = from_header_map(headers);

        // Assert
        let err = result.expect_err("invalid key should be rejected");
        assert!(err.is_serialization(), "{err:?}");
        assert!(err.to_string().contains(key), "{err}");
    }

    #[test]
    fn test_from_header_map_round_trips_to_tonic_map() {
        // Arrange
        const RAW_BIN_VAL: &[u8] = &[0x00, 0xFA, 0xFF];
        let mut grpc_map = GrpcMetadataMap::new();
        grpc_map.append(
            GrpcMetadataKey::<GrpcAscii>::from_bytes(ASCII_KEY.as_bytes()).expect("valid key"),
            GrpcAsciiMetadataValue::try_from(ASCII_VAL).expect("valid ascii value"),
        );
        grpc_map.append_bin(
            GrpcMetadataKey::<GrpcBinary>::from_bytes(BIN_KEY.as_bytes()).expect("valid key"),
            GrpcMetadataValue::from_bytes(RAW_BIN_VAL),
        );

        // Act
        let got =
            from_header_map(to_tonic_map(&grpc_map).into_headers()).expect("conversion succeeded");

        // Assert
        assert_eq!(got.len(), grpc_map.len());
        let ascii_val = got.get(ASCII_KEY).expect("ascii header present");
        assert_eq!(ascii_val.to_str(), ASCII_VAL);
        let bin_val = got.get_bin(BIN_KEY).expect("binary header present");
        assert_eq!(bin_val.as_bytes(), RAW_BIN_VAL);
    }

    #[test]
    fn test_from_header_map_skips_empty_ascii_value() {
        // Arrange
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("tracestate"),
            HeaderValue::from_static(""),
        );
        headers.insert(
            HeaderName::from_static(ASCII_KEY),
            HeaderValue::from_static(ASCII_VAL),
        );

        // Act
        let grpc_map = from_header_map(headers).expect("conversion succeeded");

        // Assert
        assert!(grpc_map.get("tracestate").is_none());
        assert_eq!(
            grpc_map
                .get(ASCII_KEY)
                .expect("ascii header present")
                .to_str(),
            ASCII_VAL
        );
    }

    #[test]
    fn test_from_header_map_preserves_empty_binary_value() {
        // Arrange
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static(BIN_KEY),
            HeaderValue::from_static(""),
        );

        // Act
        let grpc_map = from_header_map(headers).expect("conversion succeeded");

        // Assert
        let bin_val = grpc_map.get_bin(BIN_KEY).expect("binary header present");
        assert_eq!(bin_val.as_bytes(), b"");
    }
}
