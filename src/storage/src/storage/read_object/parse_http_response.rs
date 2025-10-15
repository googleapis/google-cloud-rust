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

use crate::Result;
use crate::error::ReadError;
use crate::model_ext::ObjectHighlights;
use crate::storage::v1;
use base64::Engine;
use reqwest::header::HeaderMap;
use serde_with::DeserializeAs;

pub fn object_highlights(generation: i64, headers: &HeaderMap) -> Result<ObjectHighlights> {
    let get_as_i64 = |header_name: &str| -> i64 {
        headers
            .get(header_name)
            .and_then(|s| s.to_str().ok())
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or_default()
    };
    let get_as_string = |header_name: &str| -> String {
        headers
            .get(header_name)
            .and_then(|sc| sc.to_str().ok())
            .map(|sc| sc.to_string())
            .unwrap_or_default()
    };
    Ok(ObjectHighlights {
        generation,
        metageneration: get_as_i64("x-goog-metageneration"),
        size: get_as_i64("x-goog-stored-content-length"),
        content_encoding: get_as_string("x-goog-stored-content-encoding"),
        storage_class: get_as_string("x-goog-storage-class"),
        content_type: get_as_string("content-type"),
        content_language: get_as_string("content-language"),
        content_disposition: get_as_string("content-disposition"),
        etag: get_as_string("etag"),
        checksums: headers.get("x-goog-hash").map(|_| {
            crate::model::ObjectChecksums::new()
                .set_or_clear_crc32c(headers_to_crc32c(headers))
                .set_md5_hash(headers_to_md5_hash(headers))
        }),
    })
}

pub(crate) fn headers_to_crc32c(headers: &HeaderMap) -> Option<u32> {
    headers
        .get("x-goog-hash")
        .and_then(|hash| hash.to_str().ok())
        .and_then(|hash| hash.split(",").find(|v| v.starts_with("crc32c")))
        .and_then(|hash| {
            let hash = hash.trim_start_matches("crc32c=");
            v1::Crc32c::deserialize_as(serde_json::json!(hash)).ok()
        })
}

pub(crate) fn headers_to_md5_hash(headers: &HeaderMap) -> Vec<u8> {
    headers
        .get("x-goog-hash")
        .and_then(|hash| hash.to_str().ok())
        .and_then(|hash| hash.split(",").find(|v| v.starts_with("md5")))
        .and_then(|hash| {
            let hash = hash.trim_start_matches("md5=");
            base64::prelude::BASE64_STANDARD.decode(hash).ok()
        })
        .unwrap_or_default()
}

pub(crate) fn response_generation(
    response: &reqwest::Response,
) -> std::result::Result<i64, ReadError> {
    let header = required_header(response, "x-goog-generation")?;
    header
        .parse::<i64>()
        .map_err(|e| ReadError::BadHeaderFormat("x-goog-generation", e.into()))
}

pub(crate) fn required_header<'a>(
    response: &'a reqwest::Response,
    name: &'static str,
) -> std::result::Result<&'a str, ReadError> {
    let header = response
        .headers()
        .get(name)
        .ok_or_else(|| ReadError::MissingHeader(name))?;
    header
        .to_str()
        .map_err(|e| ReadError::BadHeaderFormat(name, e.into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::Storage;
    use auth::credentials::anonymous::Builder as Anonymous;
    use base64::Engine;
    use httptest::{Expectation, Server, matchers::*, responders::status_code};
    use std::error::Error as _;
    use test_case::test_case;

    type Result = anyhow::Result<()>;

    #[tokio::test]
    async fn read_object_metadata() -> Result {
        const CONTENTS: &str = "the quick brown fox jumps over the lazy dog";
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::method_path("GET", "//storage/v1/b/test-bucket/o/test-object"),
                request::query(url_decoded(contains(("alt", "media")))),
            ])
            .respond_with(
                status_code(200)
                    .body(CONTENTS)
                    .append_header(
                        "x-goog-hash",
                        "crc32c=PBj01g==,md5=d63R1fQSI9VYL8pzalyzNQ==",
                    )
                    .append_header("x-goog-generation", 500)
                    .append_header("x-goog-metageneration", "1")
                    .append_header("x-goog-stored-content-length", 30)
                    .append_header("x-goog-stored-content-encoding", "identity")
                    .append_header("x-goog-storage-class", "STANDARD")
                    .append_header("content-language", "en")
                    .append_header("content-type", "text/plain")
                    .append_header("content-disposition", "inline")
                    .append_header("etag", "etagval"),
            ),
        );

        let endpoint = server.url("");
        let client = Storage::builder()
            .with_endpoint(endpoint.to_string())
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;
        let reader = client
            .read_object("projects/_/buckets/test-bucket", "test-object")
            .send()
            .await?;
        let object = reader.object();
        assert_eq!(object.generation, 500);
        assert_eq!(object.metageneration, 1);
        assert_eq!(object.size, 30);
        assert_eq!(object.content_encoding, "identity");
        assert_eq!(
            object.checksums.as_ref().unwrap().crc32c.unwrap(),
            crc32c::crc32c(CONTENTS.as_bytes())
        );
        assert_eq!(
            object.checksums.as_ref().unwrap().md5_hash,
            base64::prelude::BASE64_STANDARD.decode("d63R1fQSI9VYL8pzalyzNQ==")?
        );

        Ok(())
    }

    #[test]
    fn document_crc32c_values() {
        let bytes = (1234567890_u32).to_be_bytes();
        let base64 = base64::prelude::BASE64_STANDARD.encode(bytes);
        assert_eq!(base64, "SZYC0g==", "{bytes:?}");
    }

    #[test_case("", None; "no header")]
    #[test_case("crc32c=hello", None; "invalid value")]
    #[test_case("crc32c=AAAAAA==", Some(0); "zero value")]
    #[test_case("crc32c=SZYC0g==", Some(1234567890_u32); "value")]
    #[test_case("crc32c=SZYC0g==,md5=something", Some(1234567890_u32); "md5 after crc32c")]
    #[test_case("md5=something,crc32c=SZYC0g==", Some(1234567890_u32); "md5 before crc32c")]
    fn test_headers_to_crc(val: &str, want: Option<u32>) -> Result {
        let mut headers = http::HeaderMap::new();
        if !val.is_empty() {
            headers.insert("x-goog-hash", http::HeaderValue::from_str(val)?);
        }
        let got = headers_to_crc32c(&headers);
        assert_eq!(got, want);
        Ok(())
    }

    #[test_case("", None; "no header")]
    #[test_case("md5=invalid", None; "invalid value")]
    #[test_case("md5=AAAAAAAAAAAAAAAAAA==",Some("AAAAAAAAAAAAAAAAAA=="); "zero value")]
    #[test_case("md5=d63R1fQSI9VYL8pzalyzNQ==", Some("d63R1fQSI9VYL8pzalyzNQ=="); "value")]
    #[test_case("crc32c=something,md5=d63R1fQSI9VYL8pzalyzNQ==", Some("d63R1fQSI9VYL8pzalyzNQ=="); "md5 after crc32c")]
    #[test_case("md5=d63R1fQSI9VYL8pzalyzNQ==,crc32c=something", Some("d63R1fQSI9VYL8pzalyzNQ=="); "md5 before crc32c")]
    fn test_headers_to_md5(val: &str, want: Option<&str>) -> Result {
        let mut headers = http::HeaderMap::new();
        if !val.is_empty() {
            headers.insert("x-goog-hash", http::HeaderValue::from_str(val)?);
        }
        let got = headers_to_md5_hash(&headers);
        match want {
            Some(w) => assert_eq!(got, base64::prelude::BASE64_STANDARD.decode(w)?),
            None => assert!(got.is_empty()),
        }
        Ok(())
    }

    #[test_case(0)]
    #[test_case(1024)]
    fn response_generation_success(value: i64) -> Result {
        let response = http::Response::builder()
            .status(200)
            .header("x-goog-generation", value)
            .body(Vec::new())?;
        let response = reqwest::Response::from(response);
        let got = response_generation(&response)?;
        assert_eq!(got, value);
        Ok(())
    }

    #[test]
    fn response_generation_missing() -> Result {
        let response = http::Response::builder().status(200).body(Vec::new())?;
        let response = reqwest::Response::from(response);
        let err =
            response_generation(&response).expect_err("missing header should result in an error");
        assert!(
            matches!(err, ReadError::MissingHeader(h) if h == "x-goog-generation"),
            "{err:?}"
        );
        Ok(())
    }

    #[test_case("")]
    #[test_case("abc")]
    fn response_generation_format(value: &'static str) -> Result {
        let response = http::Response::builder()
            .status(200)
            .header("x-goog-generation", value)
            .body(Vec::new())?;
        let response = reqwest::Response::from(response);
        let err =
            response_generation(&response).expect_err("header value should result in an error");
        assert!(
            matches!(err, ReadError::BadHeaderFormat(h, _) if h == "x-goog-generation"),
            "{err:?}"
        );
        assert!(err.source().is_some(), "{err:?}");
        Ok(())
    }

    #[test]
    fn required_header_not_str() -> Result {
        let name = "x-goog-test";
        let response = http::Response::builder()
            .status(200)
            .header(name, http::HeaderValue::from_bytes(b"invalid\xfa")?)
            .body(Vec::new())?;
        let response = reqwest::Response::from(response);
        let err =
            required_header(&response, name).expect_err("header value should result in an error");
        assert!(
            matches!(err, ReadError::BadHeaderFormat(h, _) if h == name),
            "{err:?}"
        );
        assert!(err.source().is_some(), "{err:?}");
        Ok(())
    }
}
