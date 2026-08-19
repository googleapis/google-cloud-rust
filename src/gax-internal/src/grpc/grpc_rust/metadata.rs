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

// TODO(#5991): Replace with `grpc-rust`'s built-in header conversion methods
// when available in a future release.

use google_cloud_gax::Result as GaxResult;
use google_cloud_gax::error::Error;
use grpc::metadata::{
    Ascii as GrpcAscii, AsciiMetadataValue as GrpcAsciiMetadataValue, Binary as GrpcBinary,
    KeyAndValueRef as GrpcKeyAndValueRef, MetadataKey as GrpcMetadataKey,
    MetadataMap as GrpcMetadataMap, MetadataValue as GrpcMetadataValue,
};
use http::HeaderMap;
use tonic::metadata::{
    MetadataKey as TonicMetadataKey, MetadataMap as TonicMetadataMap,
    MetadataValue as TonicMetadataValue,
};

/// Converts a [`HeaderMap`] into a [`GrpcMetadataMap`].
pub(super) fn from_header_map(headers: &HeaderMap) -> GaxResult<GrpcMetadataMap> {
    let mut grpc_map = GrpcMetadataMap::new();
    for (key, value) in headers {
        if key.as_str().ends_with("-bin") {
            let k = GrpcMetadataKey::<GrpcBinary>::from_bytes(key.as_str().as_bytes())
                .map_err(Error::ser)?;
            grpc_map.append_bin(k, GrpcMetadataValue::from_bytes(value.as_bytes()));
        } else {
            let k = GrpcMetadataKey::<GrpcAscii>::from_bytes(key.as_str().as_bytes())
                .map_err(Error::ser)?;
            let val = GrpcAsciiMetadataValue::try_from(value.as_bytes()).map_err(Error::ser)?;
            grpc_map.append(k, val);
        }
    }
    Ok(grpc_map)
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
    use pretty_assertions::assert_eq;

    const ASCII_KEY: &str = "x-test-header";
    const ASCII_VAL: &str = "test-value";
    const BIN_KEY: &str = "x-test-header-bin";
    const BIN_VAL: &[u8] = b"test-binary-data";

    #[test]
    fn test_from_header_map() {
        // Arrange
        let mut headers = HeaderMap::new();

        headers.insert(
            http::header::HeaderName::from_static(ASCII_KEY),
            http::header::HeaderValue::from_static(ASCII_VAL),
        );
        headers.insert(
            http::header::HeaderName::from_static(BIN_KEY),
            http::header::HeaderValue::from_bytes(BIN_VAL).expect("valid header value"),
        );

        // Act
        let grpc_map = from_header_map(&headers).expect("conversion succeeded");

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

    #[test]
    fn test_from_header_map_invalid_ascii_value_returns_error() {
        // Arrange
        let mut headers = HeaderMap::new();
        let invalid_val =
            http::header::HeaderValue::from_bytes(&[0xFA]).expect("valid http header value");
        headers.insert(
            http::header::HeaderName::from_static("x-invalid-ascii"),
            invalid_val,
        );

        // Act
        let result = from_header_map(&headers);

        // Assert
        assert!(result.is_err());
    }
}
