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

//! Verify the client library correctly detects mismatched checksums on uploads.

use super::*;
use crate::storage::upload_source::BytesSource;
use httptest::{Expectation, Server, matchers::*, responders::*};
use serde_json::{Value, json};

const VEXING: &str = "how vexingly quick daft zebras jump";

mod buffered_single_shot {
    use super::*;

    fn prepare_server(body: Value) -> Server {
        super::single_shot_server(body)
    }
    async fn start_upload(server: &Server) -> anyhow::Result<UploadObject<BytesSource>> {
        super::start_single_shot(server).await
    }

    #[tokio::test]
    async fn computed_mismatch() -> Result {
        let server = prepare_server(bad_checksums_body());
        let err = start_upload(&server)
            .await?
            .send()
            .await
            .expect_err("expected a checksum error");
        assert!(err.is_serialization(), "{err:?}");
        Ok(())
    }

    #[tokio::test]
    async fn computed_match() -> Result {
        let server = prepare_server(good_checksums_body());
        let object = start_upload(&server).await?.send().await?;
        assert_eq!(object.name, "test-object");
        Ok(())
    }

    #[tokio::test]
    async fn precomputed_mismatch() -> Result {
        let server = prepare_server(bad_checksums_body());
        let err = start_upload(&server)
            .await?
            .with_known_crc32c(vexing_crc32c())
            .with_known_md5_hash(vexing_md5())
            .send()
            .await
            .expect_err("expected a checksum error");
        assert!(err.is_serialization(), "{err:?}");
        Ok(())
    }

    #[tokio::test]
    async fn precomputed_match() -> Result {
        let server = prepare_server(good_checksums_body());
        let object = start_upload(&server)
            .await?
            .with_known_crc32c(vexing_crc32c())
            .with_known_md5_hash(vexing_md5())
            .send()
            .await?;
        assert_eq!(object.name, "test-object");
        Ok(())
    }
}

mod buffered_resumable {
    use super::*;

    fn prepare_server(body: Value) -> Server {
        super::resumable_server(body)
    }
    async fn start_upload(server: &Server) -> anyhow::Result<UploadObject<BytesSource>> {
        super::start_resumable(server).await
    }

    #[tokio::test]
    async fn computed_mismatch() -> Result {
        let server = prepare_server(bad_checksums_body());
        let err = start_upload(&server)
            .await?
            .send()
            .await
            .expect_err("expected a checksum error");
        assert!(err.is_serialization(), "{err:?}");
        Ok(())
    }

    #[tokio::test]
    async fn computed_match() -> Result {
        let server = prepare_server(good_checksums_body());
        let object = start_upload(&server).await?.send().await?;
        assert_eq!(object.name, "test-object");
        Ok(())
    }

    #[tokio::test]
    async fn precomputed_mismatch() -> Result {
        let server = prepare_server(bad_checksums_body());
        let err = start_upload(&server)
            .await?
            .with_known_crc32c(vexing_crc32c())
            .with_known_md5_hash(vexing_md5())
            .send()
            .await
            .expect_err("expected a checksum error");
        assert!(err.is_serialization(), "{err:?}");
        Ok(())
    }

    #[tokio::test]
    async fn precomputed_match() -> Result {
        let server = prepare_server(good_checksums_body());
        let object = start_upload(&server)
            .await?
            .with_known_crc32c(vexing_crc32c())
            .with_known_md5_hash(vexing_md5())
            .send()
            .await?;
        assert_eq!(object.name, "test-object");
        Ok(())
    }
}

mod unbuffered_single_shot {
    use super::*;

    fn prepare_server(body: Value) -> Server {
        super::single_shot_server(body)
    }
    async fn start_upload(server: &Server) -> anyhow::Result<UploadObject<BytesSource>> {
        super::start_single_shot(server).await
    }

    #[tokio::test]
    async fn computed_mismatch() -> Result {
        let server = prepare_server(bad_checksums_body());
        let err = start_upload(&server)
            .await?
            .send()
            .await
            .expect_err("expected a checksum error");
        assert!(err.is_serialization(), "{err:?}");
        Ok(())
    }

    #[tokio::test]
    async fn computed_match() -> Result {
        let server = prepare_server(good_checksums_body());
        let object = start_upload(&server).await?.send().await?;
        assert_eq!(object.name, "test-object");
        Ok(())
    }

    #[tokio::test]
    async fn precomputed_mismatch() -> Result {
        let server = prepare_server(bad_checksums_body());
        let err = start_upload(&server)
            .await?
            .with_known_crc32c(vexing_crc32c())
            .with_known_md5_hash(vexing_md5())
            .send_unbuffered()
            .await
            .expect_err("expected a checksum error");
        assert!(err.is_serialization(), "{err:?}");
        Ok(())
    }

    #[tokio::test]
    async fn precomputed_match() -> Result {
        let server = prepare_server(good_checksums_body());
        let object = start_upload(&server)
            .await?
            .with_known_crc32c(vexing_crc32c())
            .with_known_md5_hash(vexing_md5())
            .send_unbuffered()
            .await?;
        assert_eq!(object.name, "test-object");
        Ok(())
    }
}

mod unbuffered_resumable {
    use super::*;

    fn prepare_server(body: Value) -> Server {
        super::resumable_server(body)
    }
    async fn start_upload(server: &Server) -> anyhow::Result<UploadObject<BytesSource>> {
        super::start_resumable(server).await
    }

    #[tokio::test]
    async fn computed_mismatch() -> Result {
        let server = prepare_server(bad_checksums_body());
        let err = start_upload(&server)
            .await?
            .send()
            .await
            .expect_err("expected a checksum error");
        assert!(err.is_serialization(), "{err:?}");
        Ok(())
    }

    #[tokio::test]
    async fn computed_match() -> Result {
        let server = prepare_server(good_checksums_body());
        let object = start_upload(&server).await?.send_unbuffered().await?;
        assert_eq!(object.name, "test-object");
        Ok(())
    }

    #[tokio::test]
    async fn precomputed_mismatch() -> Result {
        let server = prepare_server(bad_checksums_body());
        let err = start_upload(&server)
            .await?
            .with_known_crc32c(vexing_crc32c())
            .with_known_md5_hash(vexing_md5())
            .send_unbuffered()
            .await
            .expect_err("expected a checksum error");
        assert!(err.is_serialization(), "{err:?}");
        Ok(())
    }

    #[tokio::test]
    async fn precomputed_match() -> Result {
        let server = prepare_server(good_checksums_body());
        let object = start_upload(&server)
            .await?
            .with_known_crc32c(vexing_crc32c())
            .with_known_md5_hash(vexing_md5())
            .send_unbuffered()
            .await?;
        assert_eq!(object.name, "test-object");
        Ok(())
    }
}

async fn start_resumable(server: &Server) -> anyhow::Result<UploadObject<BytesSource>> {
    let client = test_builder()
        .with_endpoint(format!("http://{}", server.addr()))
        .build()
        .await?;
    Ok(client
        .upload_object("projects/_/buckets/test-bucket", "test-object", VEXING)
        .with_resumable_upload_threshold(0_usize))
}

async fn start_single_shot(server: &Server) -> anyhow::Result<UploadObject<BytesSource>> {
    let client = test_builder()
        .with_endpoint(format!("http://{}", server.addr()))
        .build()
        .await?;
    Ok(client
        .upload_object("projects/_/buckets/test-bucket", "test-object", VEXING)
        .with_resumable_upload_threshold(1024 * 1024_usize))
}

fn single_shot_server(body: Value) -> Server {
    let server = Server::run();
    server.expect(
        Expectation::matching(all_of![
            request::method_path("POST", "/upload/storage/v1/b/test-bucket/o"),
            request::query(url_decoded(contains(("name", "test-object")))),
            request::query(url_decoded(contains(("uploadType", "multipart")))),
        ])
        .respond_with(status_code(200).body(body.to_string())),
    );
    server
}

fn resumable_server(body: Value) -> Server {
    let server = Server::run();
    let session = server.url("/upload/session/test-only-001");
    let path = session.path().to_string();
    server.expect(
        Expectation::matching(all_of![
            request::method_path("POST", "/upload/storage/v1/b/test-bucket/o"),
            request::query(url_decoded(contains(("name", "test-object")))),
            request::query(url_decoded(contains(("uploadType", "resumable")))),
        ])
        .respond_with(status_code(200).append_header("location", session.to_string())),
    );
    let len = VEXING.len();
    assert_ne!(len, 0);
    server.expect(
        Expectation::matching(all_of![
            request::method_path("PUT", path.clone()),
            request::headers(contains((
                "content-range",
                format!("bytes 0-{}/{len}", len - 1)
            )))
        ])
        .respond_with(
            status_code(200)
                .append_header("content-type", "application/json")
                .body(body.to_string()),
        ),
    );
    server
}

fn vexing_crc32c() -> u32 {
    crc32c::crc32c(VEXING.as_bytes())
}

fn vexing_md5() -> bytes::Bytes {
    bytes::Bytes::from_owner(md5::compute(VEXING.as_bytes()).0)
}

fn bad_checksums_body() -> Value {
    // The magic strings can be regenerated using:
    //     rm empty.txt
    //     touch empty.txt
    //     gcloud hash empty.txt
    json!({
        "bucket": "/projects/_/buckets/test-bucket",
        "name": "test-object",
        "crc32c": "AAAAAA==",
        "md5Hash": "1B2M2Y8AsgTpgAmY7PhCfg==",
        "size": 0,
    })
}

fn good_checksums_body() -> Value {
    // The magic strings can be regenerated using:
    //     echo -n 'how vexingly quick daft zebras jump' > vexing.txt
    //     gcloud hash vexing.txt
    json!({
        "bucket": "/projects/_/buckets/test-bucket",
        "name": "test-object",
        "crc32c": "9esWHQ==",
        "md5Hash": "XsWNN3ATlnzCdna3JNYDJw==",
        "size": 35,
    })
}
