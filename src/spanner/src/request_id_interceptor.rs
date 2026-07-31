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

//! An attempt interceptor that maintains the attempt counter suffix in `x-goog-spanner-request-id` headers.

use gaxi::attempt_interceptor::AttemptInterceptor;
use http::HeaderMap;
use http::header::{HeaderName, HeaderValue};
use std::io::Write as _;

#[allow(dead_code)]
pub(crate) static REQUEST_ID_HEADER: HeaderName =
    HeaderName::from_static("x-goog-spanner-request-id");

/// Intercepts outgoing Spanner RPC attempts and appends or updates the attempt suffix on the
/// `x-goog-spanner-request-id` header.
///
/// This implementation uses stack-allocated formatting and ASCII byte slices to eliminate heap
/// allocations on the hot path.
#[allow(dead_code)]
#[derive(Debug, Clone, Default)]
pub(crate) struct SpannerRequestIdInterceptor;

impl AttemptInterceptor for SpannerRequestIdInterceptor {
    fn intercept(&self, headers: &mut HeaderMap, attempt: u32) {
        let Some(val) = headers.get(&REQUEST_ID_HEADER) else {
            return;
        };
        let bytes = val.as_bytes();
        if !bytes.is_ascii() {
            return;
        }
        let Some(dot_index) = bytes.iter().rposition(|&byte| byte == b'.') else {
            return;
        };
        let base_prefix = &bytes[..=dot_index];

        // We use a fixed-size 128-byte stack-allocated array as a scratch buffer to format
        // the updated header value without heap allocation.
        // A valid Spanner Request ID is at most ~84 ASCII bytes in practice. If `base_prefix`
        // exceeds this buffer capacity (e.g., due to a malformed or oversized header), we safely
        // return early without mutating the header.
        let mut buffer = [0u8; 128];
        let max_len = buffer.len();
        if base_prefix.len() + 10 > max_len {
            return;
        }

        // `copy_from_slice` copies ASCII bytes from `base_prefix` into our stack buffer.
        // Because `buffer` is stack-allocated, this is an inline byte copy with zero heap allocation.
        buffer[..base_prefix.len()].copy_from_slice(base_prefix);

        // `std::io::Write` implemented for `&mut [u8]` formats ASCII digits directly into the
        // slice on the stack with zero heap allocation.
        // Under the hood, Rust's standard library `core::fmt` integer formatting uses specialized
        // lookup tables and inlined algorithms (similar to `itoa`), achieving the same zero-allocation
        // efficiency without custom integer-to-ASCII division loops.
        // Additionally, `write!` updates the slice cursor (`tail`) in place to point to the unwritten
        // remainder, so `max_len - tail.len()` gives the exact total bytes written.
        let mut tail = &mut buffer[base_prefix.len()..];
        if write!(tail, "{attempt}").is_err() {
            return;
        }
        let total_len = max_len - tail.len();

        // The `http` crate does not support in-place mutation of `HeaderValue` bytes because
        // values may share underlying immutable storage (`Bytes`).
        // Constructing a new `HeaderValue` from bytes uses an inline small-buffer optimization
        // for short ASCII strings. Additionally, cloning `REQUEST_ID_HEADER` (a `from_static`
        // header name) is a zero-allocation pointer copy, and `headers.insert(...)` replaces the
        // existing map entry in place.
        if let Ok(new_val) = HeaderValue::from_bytes(&buffer[..total_len]) {
            headers.insert(REQUEST_ID_HEADER.clone(), new_val);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_attempt_append() {
        let interceptor = SpannerRequestIdInterceptor;
        let mut headers = HeaderMap::new();
        headers.insert(
            REQUEST_ID_HEADER.clone(),
            HeaderValue::from_static("1.a1b2c3d4e5f60718.1.1.42."),
        );

        interceptor.intercept(&mut headers, 1);

        let value = headers
            .get(&REQUEST_ID_HEADER)
            .expect("header should be present")
            .to_str()
            .expect("header should be valid ASCII");
        assert_eq!(value, "1.a1b2c3d4e5f60718.1.1.42.1");
    }

    #[test]
    fn retry_attempt_overwrite() {
        let interceptor = SpannerRequestIdInterceptor;
        let mut headers = HeaderMap::new();
        headers.insert(
            REQUEST_ID_HEADER.clone(),
            HeaderValue::from_static("1.a1b2c3d4e5f60718.1.1.42.1"),
        );

        interceptor.intercept(&mut headers, 2);

        let value = headers
            .get(&REQUEST_ID_HEADER)
            .expect("header should be present")
            .to_str()
            .expect("header should be valid ASCII");
        assert_eq!(value, "1.a1b2c3d4e5f60718.1.1.42.2");
    }

    #[test]
    fn multiple_retries_sequence() {
        let interceptor = SpannerRequestIdInterceptor;
        let mut headers = HeaderMap::new();
        headers.insert(
            REQUEST_ID_HEADER.clone(),
            HeaderValue::from_static("1.a1b2c3d4e5f60718.1.1.42."),
        );

        interceptor.intercept(&mut headers, 1);
        let val1 = headers
            .get(&REQUEST_ID_HEADER)
            .expect("header should be present")
            .to_str()
            .expect("header should be valid ASCII");
        assert_eq!(val1, "1.a1b2c3d4e5f60718.1.1.42.1");

        interceptor.intercept(&mut headers, 2);
        let val2 = headers
            .get(&REQUEST_ID_HEADER)
            .expect("header should be present")
            .to_str()
            .expect("header should be valid ASCII");
        assert_eq!(val2, "1.a1b2c3d4e5f60718.1.1.42.2");

        interceptor.intercept(&mut headers, 10);
        let val10 = headers
            .get(&REQUEST_ID_HEADER)
            .expect("header should be present")
            .to_str()
            .expect("header should be valid ASCII");
        assert_eq!(val10, "1.a1b2c3d4e5f60718.1.1.42.10");
    }

    #[test]
    fn missing_header_ignored() {
        let interceptor = SpannerRequestIdInterceptor;
        let mut headers = HeaderMap::new();

        interceptor.intercept(&mut headers, 1);

        assert!(headers.get(&REQUEST_ID_HEADER).is_none());
    }

    #[test]
    fn malformed_header_ignored() {
        let interceptor = SpannerRequestIdInterceptor;
        let mut headers = HeaderMap::new();
        headers.insert(
            REQUEST_ID_HEADER.clone(),
            HeaderValue::from_static("invalid_request_id_without_dots"),
        );

        interceptor.intercept(&mut headers, 1);

        let value = headers
            .get(&REQUEST_ID_HEADER)
            .expect("header should be present")
            .to_str()
            .expect("header should be valid ASCII");
        assert_eq!(value, "invalid_request_id_without_dots");
    }

    #[test]
    fn oversized_header_ignored() {
        let interceptor = SpannerRequestIdInterceptor;
        let mut headers = HeaderMap::new();
        // A header value where base_prefix is 120 bytes (120 + 10 = 130 > 128 max_len)
        let oversized = format!("1.{}.", "a".repeat(118));
        headers.insert(
            REQUEST_ID_HEADER.clone(),
            HeaderValue::from_str(&oversized).expect("valid ascii"),
        );

        interceptor.intercept(&mut headers, 1);

        let value = headers
            .get(&REQUEST_ID_HEADER)
            .expect("header should be present")
            .to_str()
            .expect("header should be valid ASCII");
        assert_eq!(value, oversized);
    }

    #[test]
    fn non_ascii_header_ignored() {
        let interceptor = SpannerRequestIdInterceptor;
        let mut headers = HeaderMap::new();
        let non_ascii = b"1.a1b2c3\x80.1.";
        headers.insert(
            REQUEST_ID_HEADER.clone(),
            HeaderValue::from_bytes(non_ascii).expect("valid header bytes"),
        );

        interceptor.intercept(&mut headers, 1);

        let value = headers
            .get(&REQUEST_ID_HEADER)
            .expect("header should be present")
            .as_bytes();
        assert_eq!(value, non_ascii);
    }
}
