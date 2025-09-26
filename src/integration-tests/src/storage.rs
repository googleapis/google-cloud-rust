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

use crate::{Error, Result};
use gax::exponential_backoff::{ExponentialBackoff, ExponentialBackoffBuilder};
use gax::options::RequestOptionsBuilder;
use gax::paginator::ItemPaginator as _;
use gax::retry_policy::RetryPolicyExt;
use lro::Poller;
use std::time::Duration;
use storage::client::StorageControl;
use storage::model::Bucket;
use storage::model::bucket::iam_config::UniformBucketLevelAccess;
use storage::model::bucket::{HierarchicalNamespace, IamConfig};
use storage::model_ext::KeyAes256;
use storage::read_object::ReadObjectResponse;
use storage::streaming_source::{Seek, SizeHint, StreamingSource};
use storage_samples::cleanup_bucket;

/// An upload data source used in tests.
#[derive(Clone, Debug)]
struct TestDataSource {
    size: u64,
    hint: SizeHint,
    offset: u64,
    abort: u64,
}

impl TestDataSource {
    const LINE_SIZE: u64 = 128;

    fn new(size: u64) -> Self {
        Self {
            size,
            hint: SizeHint::with_exact(size),
            offset: 0,
            abort: u64::MAX,
        }
    }

    fn without_size_hint(mut self) -> Self {
        let mut hint = SizeHint::new();
        hint.set_lower(self.hint.lower());
        self.hint = hint;
        self
    }

    fn with_abort(mut self, abort: u64) -> Self {
        self.abort = abort;
        self
    }
}

impl StreamingSource for TestDataSource {
    type Error = std::io::Error;
    async fn next(&mut self) -> Option<std::result::Result<bytes::Bytes, Self::Error>> {
        match self.offset {
            n if n >= self.size => None,
            n if n >= self.abort => {
                self.offset = self.size; // Next call with return None
                Some(Err(Self::Error::new(
                    std::io::ErrorKind::ConnectionAborted,
                    "simulated error",
                )))
            }
            n if n + Self::LINE_SIZE < self.size => {
                let line = self.offset / Self::LINE_SIZE;
                let w = Self::LINE_SIZE as usize - 30 - 2;
                let data =
                    bytes::Bytes::from_owner(format!("{line:030} {:width$}\n", "", width = w));
                self.offset += Self::LINE_SIZE;
                Some(Ok(data))
            }
            n => {
                let w = (self.size - n) as usize;
                let data = bytes::Bytes::from_owner(format!("{:width$}", "", width = w));
                self.offset = self.size;
                Some(Ok(data))
            }
        }
    }
    async fn size_hint(&self) -> std::result::Result<SizeHint, Self::Error> {
        Ok(self.hint.clone())
    }
}

impl Seek for TestDataSource {
    type Error = std::io::Error;
    async fn seek(&mut self, offset: u64) -> std::result::Result<(), Self::Error> {
        if offset % Self::LINE_SIZE != 0 {
            return Err(Self::Error::new(
                std::io::ErrorKind::InvalidInput,
                "bad offset",
            ));
        }
        self.offset = offset;
        Ok(())
    }
}

pub async fn objects(builder: storage::builder::storage::ClientBuilder) -> Result<()> {
    // Create a temporary bucket for the test.
    let (control, bucket) = create_test_bucket().await?;

    let client = builder.build().await?;
    tracing::info!("testing insert_object()");
    const CONTENTS: &str = "the quick brown fox jumps over the lazy dog";
    let insert = client
        .write_object(&bucket.name, "quick.text", CONTENTS)
        .set_metadata([("verify-metadata-works", "yes")])
        .set_content_type("text/plain")
        .set_content_language("en")
        .set_storage_class("STANDARD")
        .send_unbuffered()
        .await?;
    tracing::info!("success with insert={insert:?}");
    assert_eq!(
        insert
            .metadata
            .get("verify-metadata-works")
            .map(String::as_str),
        Some("yes")
    );

    tracing::info!("testing read_object()");
    let mut response = client
        .read_object(&bucket.name, &insert.name)
        .send()
        .await?;

    // Retrieve the metadata before reading the data.
    let object = response.object();
    assert!(object.generation > 0);
    assert!(object.metageneration > 0);
    assert_eq!(object.size, CONTENTS.len() as i64);
    assert_eq!(object.content_encoding, "identity");
    assert_eq!(
        object.checksums.unwrap().crc32c,
        Some(crc32c::crc32c(CONTENTS.as_bytes()))
    );
    assert_eq!(object.storage_class, "STANDARD");
    assert_eq!(object.content_language, "en");
    assert_eq!(object.content_type, "text/plain");
    assert!(!object.content_disposition.is_empty());
    assert!(!object.etag.is_empty());

    let mut contents = Vec::new();
    while let Some(b) = response.next().await.transpose()? {
        contents.extend_from_slice(&b);
    }
    let contents = bytes::Bytes::from_owner(contents);
    assert_eq!(contents, CONTENTS.as_bytes());
    tracing::info!("success with contents={contents:?}");

    control
        .delete_object()
        .set_bucket(&insert.bucket)
        .set_object(&insert.name)
        .set_generation(insert.generation)
        .send()
        .await?;
    control
        .delete_bucket()
        .set_name(&bucket.name)
        .send()
        .await?;

    Ok(())
}

pub async fn objects_customer_supplied_encryption(
    builder: storage::builder::storage::ClientBuilder,
) -> Result<()> {
    // Create a temporary bucket for the test.
    let (control, bucket) = create_test_bucket().await?;

    let client = builder.build().await?;

    tracing::info!("testing insert_object() with key");
    const CONTENTS: &str = "the quick brown fox jumps over the lazy dog";
    let key = vec![b'a'; 32];
    let insert = client
        .write_object(&bucket.name, "quick.text", CONTENTS)
        .set_key(KeyAes256::new(&key)?)
        .send_unbuffered()
        .await?;
    tracing::info!("success with insert={insert:?}");

    tracing::info!("testing read_object() with key");
    let mut resp = client
        .read_object(&bucket.name, &insert.name)
        .set_key(KeyAes256::new(&key)?)
        .send()
        .await?;
    let mut contents = Vec::new();
    while let Some(chunk) = resp.next().await.transpose()? {
        contents.extend_from_slice(&chunk);
    }
    let contents = bytes::Bytes::from(contents);
    assert_eq!(contents, CONTENTS.as_bytes());
    tracing::info!("success with contents={contents:?}");

    control
        .delete_object()
        .set_bucket(&insert.bucket)
        .set_object(&insert.name)
        .set_generation(insert.generation)
        .send()
        .await?;
    control
        .delete_bucket()
        .set_name(&bucket.name)
        .send()
        .await?;

    Ok(())
}

pub async fn objects_large_file(builder: storage::builder::storage::ClientBuilder) -> Result<()> {
    // Create a temporary bucket for the test.
    let (control, bucket) = create_test_bucket().await?;

    let client = builder.build().await?;

    // Create a large enough file that will require multiple chunks to download.
    const BLOCK_SIZE: usize = 500;
    let mut contents = Vec::new();
    for i in 0..16 {
        contents.extend_from_slice(&[i as u8; BLOCK_SIZE]);
    }

    tracing::info!("testing insert_object()");
    let insert = client
        .write_object(
            &bucket.name,
            "quick.text",
            bytes::Bytes::from_owner(contents.clone()),
        )
        .send_unbuffered()
        .await?;
    tracing::info!("success with insert={insert:?}");

    tracing::info!("testing read_object() streaming");
    let mut resp = client
        .read_object(&bucket.name, &insert.name)
        .send()
        .await?;

    // This should take multiple chunks to download.
    let mut got = bytes::BytesMut::new();
    let mut count = 0;
    while let Some(chunk) = resp.next().await.transpose()? {
        got.extend_from_slice(&chunk);
        count += 1;
    }
    assert_eq!(got, contents);
    assert!(count > 1, "{count:?}");
    tracing::info!("success with large contents");

    // Use futures::StreamExt for the download.
    tracing::info!("testing read_object() using into_stream()");
    use futures::StreamExt;
    let mut stream = client
        .read_object(&bucket.name, &insert.name)
        .send()
        .await?
        .into_stream()
        .enumerate();

    // This should take multiple chunks to download.
    got.clear();
    let mut iteration = 0;
    while let Some((i, chunk)) = stream.next().await {
        got.extend_from_slice(&chunk?);
        iteration = i;
    }
    assert_eq!(got, contents);
    assert!(iteration > 1, "{iteration:?}");
    tracing::info!("success with into_stream() large contents");

    control
        .delete_object()
        .set_bucket(&insert.bucket)
        .set_object(&insert.name)
        .set_generation(insert.generation)
        .send()
        .await?;
    control
        .delete_bucket()
        .set_name(&bucket.name)
        .send()
        .await?;

    Ok(())
}

pub async fn upload_buffered(builder: storage::builder::storage::ClientBuilder) -> Result<()> {
    // Create a temporary bucket for the test.
    let (control, bucket) = create_test_bucket().await?;
    let client = builder.build().await?;

    tracing::info!("testing upload_object_buffered() [1]");
    let insert = client
        .write_object(&bucket.name, "empty.txt", "")
        .set_if_generation_match(0)
        .send_buffered()
        .await?;
    tracing::info!("success with insert={insert:?}");
    assert_eq!(insert.size, 0_i64);

    let payload = bytes::Bytes::from_owner(Vec::from_iter((0..128 * 1024).map(|_| 0_u8)));
    tracing::info!("testing upload_object_buffered() [2]");
    let insert = client
        .write_object(&bucket.name, "128K.txt", payload)
        .set_if_generation_match(0)
        .send_buffered()
        .await?;
    tracing::info!("success with insert={insert:?}");
    assert_eq!(insert.size, 128 * 1024_i64);

    let payload = bytes::Bytes::from_owner(Vec::from_iter((0..512 * 1024).map(|_| 0_u8)));
    tracing::info!("testing upload_object_buffered() [3]");
    let insert = client
        .write_object(&bucket.name, "512K.txt", payload)
        .set_if_generation_match(0)
        .send_buffered()
        .await?;
    tracing::info!("success with insert={insert:?}");
    assert_eq!(insert.size, 512 * 1024_i64);

    cleanup_bucket(control, bucket.name).await?;

    Ok(())
}

pub async fn upload_buffered_resumable_known_size(
    builder: storage::builder::storage::ClientBuilder,
) -> Result<()> {
    // Create a temporary bucket for the test.
    let (control, bucket) = create_test_bucket().await?;
    let client = builder.build().await?;

    tracing::info!("testing send_unbuffered() [1]");
    let payload = TestDataSource::new(0_u64);
    let insert = client
        .write_object(&bucket.name, "empty.txt", payload)
        .set_if_generation_match(0)
        .with_resumable_upload_threshold(0_usize)
        .send_buffered()
        .await?;
    tracing::info!("success with insert={insert:?}");
    assert_eq!(insert.size, 0_i64);

    let payload = TestDataSource::new(128 * 1024_u64);
    tracing::info!("testing upload_object_buffered() [2]");
    let insert = client
        .write_object(&bucket.name, "128K.txt", payload)
        .set_if_generation_match(0)
        .with_resumable_upload_threshold(0_usize)
        .send_buffered()
        .await?;
    tracing::info!("success with insert={insert:?}");
    assert_eq!(insert.size, 128 * 1024_i64);

    let payload = TestDataSource::new(512 * 1024_u64);
    tracing::info!("testing upload_object_buffered() [3]");
    let insert = client
        .write_object(&bucket.name, "512K.txt", payload)
        .set_if_generation_match(0)
        .with_resumable_upload_threshold(0_usize)
        .send_buffered()
        .await?;
    tracing::info!("success with insert={insert:?}");
    assert_eq!(insert.size, 512 * 1024_i64);

    cleanup_bucket(control, bucket.name).await?;

    Ok(())
}

pub async fn upload_buffered_resumable_unknown_size(
    builder: storage::builder::storage::ClientBuilder,
) -> Result<()> {
    // Create a temporary bucket for the test.
    let (control, bucket) = create_test_bucket().await?;
    let client = builder.build().await?;

    tracing::info!("testing send_unbuffered() [1]");
    let payload = TestDataSource::new(0_u64).without_size_hint();
    let insert = client
        .write_object(&bucket.name, "empty.txt", payload)
        .set_if_generation_match(0)
        .with_resumable_upload_threshold(0_usize)
        .send_buffered()
        .await?;
    tracing::info!("success with insert={insert:?}");
    assert_eq!(insert.size, 0_i64);

    let payload = TestDataSource::new(128 * 1024_u64).without_size_hint();
    tracing::info!("testing upload_object_buffered() [2]");
    let insert = client
        .write_object(&bucket.name, "128K.txt", payload)
        .set_if_generation_match(0)
        .with_resumable_upload_threshold(0_usize)
        .send_buffered()
        .await?;
    tracing::info!("success with insert={insert:?}");
    assert_eq!(insert.size, 128 * 1024_i64);

    let payload = TestDataSource::new(512 * 1024_u64).without_size_hint();
    tracing::info!("testing upload_object_buffered() [3]");
    let insert = client
        .write_object(&bucket.name, "512K.txt", payload)
        .set_if_generation_match(0)
        .with_resumable_upload_threshold(0_usize)
        .send_buffered()
        .await?;
    tracing::info!("success with insert={insert:?}");
    assert_eq!(insert.size, 512 * 1024_i64);

    let payload = TestDataSource::new(500 * 1024_u64).without_size_hint();
    tracing::info!("testing upload_object_buffered() [4]");
    let insert = client
        .write_object(&bucket.name, "500K.txt", payload)
        .set_if_generation_match(0)
        .with_resumable_upload_threshold(0_usize)
        .send_buffered()
        .await?;
    tracing::info!("success with insert={insert:?}");
    assert_eq!(insert.size, 500 * 1024_i64);

    cleanup_bucket(control, bucket.name).await?;

    Ok(())
}

pub async fn upload_unbuffered_resumable_known_size(
    builder: storage::builder::storage::ClientBuilder,
) -> Result<()> {
    // Create a temporary bucket for the test.
    let (control, bucket) = create_test_bucket().await?;
    let client = builder.build().await?;

    tracing::info!("testing send_unbuffered() [1]");
    let payload = TestDataSource::new(0_u64);
    let insert = client
        .write_object(&bucket.name, "empty.txt", payload)
        .set_if_generation_match(0)
        .with_resumable_upload_threshold(0_usize)
        .send_unbuffered()
        .await?;
    tracing::info!("success with insert={insert:?}");
    assert_eq!(insert.size, 0_i64);

    let payload = TestDataSource::new(128 * 1024_u64);
    tracing::info!("testing upload_object_buffered() [2]");
    let insert = client
        .write_object(&bucket.name, "128K.txt", payload)
        .set_if_generation_match(0)
        .with_resumable_upload_threshold(0_usize)
        .send_unbuffered()
        .await?;
    tracing::info!("success with insert={insert:?}");
    assert_eq!(insert.size, 128 * 1024_i64);

    let payload = TestDataSource::new(512 * 1024_u64);
    tracing::info!("testing upload_object_buffered() [3]");
    let insert = client
        .write_object(&bucket.name, "512K.txt", payload)
        .set_if_generation_match(0)
        .with_resumable_upload_threshold(0_usize)
        .send_unbuffered()
        .await?;
    tracing::info!("success with insert={insert:?}");
    assert_eq!(insert.size, 512 * 1024_i64);

    cleanup_bucket(control, bucket.name).await?;

    Ok(())
}

pub async fn upload_unbuffered_resumable_unknown_size(
    builder: storage::builder::storage::ClientBuilder,
) -> Result<()> {
    // Create a temporary bucket for the test.
    let (control, bucket) = create_test_bucket().await?;
    let client = builder.build().await?;

    tracing::info!("testing send_unbuffered() [1]");
    let payload = TestDataSource::new(0_u64).without_size_hint();
    let insert = client
        .write_object(&bucket.name, "empty.txt", payload)
        .set_if_generation_match(0)
        .with_resumable_upload_threshold(0_usize)
        .send_unbuffered()
        .await?;
    tracing::info!("success with insert={insert:?}");
    assert_eq!(insert.size, 0_i64);

    let payload = TestDataSource::new(128 * 1024_u64).without_size_hint();
    tracing::info!("testing upload_object_buffered() [2]");
    let insert = client
        .write_object(&bucket.name, "128K.txt", payload)
        .set_if_generation_match(0)
        .with_resumable_upload_threshold(0_usize)
        .send_unbuffered()
        .await?;
    tracing::info!("success with insert={insert:?}");
    assert_eq!(insert.size, 128 * 1024_i64);

    let payload = TestDataSource::new(512 * 1024_u64).without_size_hint();
    tracing::info!("testing upload_object_buffered() [3]");
    let insert = client
        .write_object(&bucket.name, "512K.txt", payload)
        .set_if_generation_match(0)
        .with_resumable_upload_threshold(0_usize)
        .send_unbuffered()
        .await?;
    tracing::info!("success with insert={insert:?}");
    assert_eq!(insert.size, 512 * 1024_i64);

    let payload = TestDataSource::new(500 * 1024_u64).without_size_hint();
    tracing::info!("testing upload_object_buffered() [4]");
    let insert = client
        .write_object(&bucket.name, "500K.txt", payload)
        .set_if_generation_match(0)
        .with_resumable_upload_threshold(0_usize)
        .send_unbuffered()
        .await?;
    tracing::info!("success with insert={insert:?}");
    assert_eq!(insert.size, 500 * 1024_i64);

    cleanup_bucket(control, bucket.name).await?;

    Ok(())
}

const ABORT_TEST_STOP: u64 = 512 * 1024;
const ABORT_TEST_SIZE: u64 = 1024 * 1024;

pub async fn abort_upload(
    builder: storage::builder::storage::ClientBuilder,
    bucket_name: &str,
) -> Result<()> {
    tracing::info!("abort_upload test, using bucket {}", bucket_name);

    // Create a temporary bucket for the test.
    let client = builder.build().await?;

    abort_upload_unbuffered(client.clone(), bucket_name).await?;
    abort_upload_buffered(client.clone(), bucket_name).await?;
    Ok(())
}

struct AbortUploadTestCase {
    name: String,
    upload: storage::builder::storage::WriteObject<TestDataSource>,
}

fn abort_upload_test_cases(
    client: &storage::client::Storage,
    bucket_name: &str,
    prefix: &str,
) -> Vec<AbortUploadTestCase> {
    let sources = [
        (
            "known-size",
            TestDataSource::new(ABORT_TEST_SIZE).with_abort(ABORT_TEST_STOP),
        ),
        (
            "unknown-size",
            TestDataSource::new(ABORT_TEST_SIZE)
                .with_abort(ABORT_TEST_STOP)
                .without_size_hint(),
        ),
    ];
    let thresholds = [
        ("single-shot", 2 * ABORT_TEST_SIZE as usize),
        ("resumable", 0_usize),
    ];
    let mut uploads = Vec::new();
    for s in sources.into_iter() {
        for t in thresholds {
            let name = format!("{prefix}-{}-{}.txt", s.0, t.0);
            let upload = client
                .write_object(bucket_name, &name, s.1.clone())
                .set_if_generation_match(0)
                .with_resumable_upload_threshold(t.1);
            uploads.push(AbortUploadTestCase { name, upload });
        }
    }
    uploads
}

async fn abort_upload_unbuffered(
    client: storage::client::Storage,
    bucket_name: &str,
) -> Result<()> {
    let test_cases = abort_upload_test_cases(&client, bucket_name, "unbuffered");

    for (number, AbortUploadTestCase { name, upload }) in test_cases.into_iter().enumerate() {
        tracing::info!("[{number}] {name}");
        let err = upload
            .send_unbuffered()
            .await
            .expect_err(&format!("[{number}] {name} - expected error"));
        tracing::info!("[{number}] {name} - got error {err:?}");
        assert!(err.is_serialization(), "[{number}] {name} - {err:?}");
        let err = client
            .read_object(bucket_name, &name)
            .send()
            .await
            .expect_err(&format!(
                "[{number}] {name} - expected error on read_object()"
            ));
        assert_eq!(err.http_status_code(), Some(404), "{err:?}");
    }

    Ok(())
}

async fn abort_upload_buffered(client: storage::client::Storage, bucket_name: &str) -> Result<()> {
    let test_cases = abort_upload_test_cases(&client, bucket_name, "buffered");

    for (number, AbortUploadTestCase { name, upload }) in test_cases.into_iter().enumerate() {
        tracing::info!("[{number}] {name}");
        let err = upload
            .send_buffered()
            .await
            .expect_err(&format!("[{number}] {name} - expected error"));
        tracing::info!("[{number}] {name} - got error {err:?}");
        assert!(err.is_serialization(), "[{number}] {name} - {err:?}");
        let err = client
            .read_object(bucket_name, &name)
            .send()
            .await
            .expect_err(&format!(
                "[{number}] {name} - expected error on read_object()"
            ));
        assert_eq!(err.http_status_code(), Some(404), "{err:?}");
    }

    Ok(())
}

pub async fn ranged_reads(
    builder: storage::builder::storage::ClientBuilder,
    bucket_name: &str,
) -> Result<()> {
    tracing::info!("ranged reads test, using bucket {bucket_name}");
    let client = builder.build().await?;
    const VEXING: &str = "how vexingly quick daft zebras jump";

    let object = client
        .write_object(bucket_name, "ranged_reads", VEXING)
        .set_if_generation_match(0)
        .send_unbuffered()
        .await?;
    tracing::info!("created object {object:?}");

    use storage::model_ext::ReadRange;
    let want = VEXING.as_bytes();
    tracing::info!("running with ReadRange::head");
    let response = client
        .read_object(&object.bucket, &object.name)
        .set_generation(object.generation)
        .set_read_range(ReadRange::head(4))
        .send()
        .await?;
    let got = read_all(response).await?;
    assert_eq!(&got, &want[0..4]);

    tracing::info!("running with ReadRange::tail");
    let response = client
        .read_object(&object.bucket, &object.name)
        .set_generation(object.generation)
        .set_read_range(ReadRange::tail(4))
        .send()
        .await?;
    let got = read_all(response).await?;
    assert_eq!(&got, &want[(VEXING.len() - 4)..]);

    tracing::info!("running with ReadRange::offset");
    let response = client
        .read_object(&object.bucket, &object.name)
        .set_generation(object.generation)
        .set_read_range(ReadRange::offset(4))
        .send()
        .await?;
    let got = read_all(response).await?;
    assert_eq!(&got, &want[4..]);

    tracing::info!("running with ReadRange::segment");
    let response = client
        .read_object(&object.bucket, &object.name)
        .set_generation(object.generation)
        .set_read_range(ReadRange::segment(4, 4))
        .send()
        .await?;
    let got = read_all(response).await?;
    assert_eq!(&got, &want[4..8]);

    tracing::info!("DONE");
    Ok(())
}

async fn read_all(mut response: ReadObjectResponse) -> Result<Vec<u8>> {
    let mut contents = Vec::new();
    while let Some(b) = response.next().await.transpose()? {
        contents.extend_from_slice(&b);
    }
    Ok(contents)
}

pub async fn checksums(
    builder: storage::builder::storage::ClientBuilder,
    bucket_name: &str,
) -> Result<()> {
    tracing::info!("checksums test, using bucket {bucket_name}");

    let client = builder.build().await?;
    const VEXING: &str = "how vexingly quick daft zebras jump";

    type ObjectResult = storage::Result<storage::model::Object>;
    type Boxed = futures::future::BoxFuture<'static, ObjectResult>;
    let uploads: Vec<(&str, Boxed)> = vec![
        (
            "verify/default",
            Box::pin(
                client
                    .write_object(bucket_name, "verify/default", VEXING)
                    .set_if_generation_match(0)
                    .send_buffered(),
            ),
        ),
        (
            "verify/md5",
            Box::pin(
                client
                    .write_object(bucket_name, "verify/md5", VEXING)
                    .set_if_generation_match(0)
                    .compute_md5()
                    .send_buffered(),
            ),
        ),
        (
            "computed/default",
            Box::pin(
                client
                    .write_object(bucket_name, "computed/default", VEXING)
                    .set_if_generation_match(0)
                    .precompute_checksums()
                    .await?
                    .send_buffered(),
            ),
        ),
        (
            "computed/md5",
            Box::pin(
                client
                    .write_object(bucket_name, "computed/md5", VEXING)
                    .set_if_generation_match(0)
                    .compute_md5()
                    .precompute_checksums()
                    .await?
                    .send_buffered(),
            ),
        ),
    ];

    for (name, upload) in uploads.into_iter() {
        tracing::info!("waiting for {name}");
        match upload.await {
            Ok(_) => {}
            Err(e) => {
                println!("error running in {name}: {e:?}");
                return Err(e.into());
            }
        }
    }

    Ok(())
}

pub async fn object_names(
    builder: storage::builder::storage::ClientBuilder,
    control: storage::client::StorageControl,
    bucket_name: &str,
) -> Result<()> {
    tracing::info!("object names test, using bucket {bucket_name}");

    let client = builder.build().await?;

    let names = ["a/1", "a/2", "b/c/d/e", "b/c/d/e#1", "b/c/d/f", "b/c/d/g"];

    for name in names {
        let upload = client
            .write_object(bucket_name, name, "")
            .set_if_generation_match(0)
            .send_unbuffered()
            .await?;
        assert_eq!(upload.bucket, bucket_name);
        assert_eq!(upload.name, name);
        assert_eq!(upload.size, 0_i64);

        let reader = client
            .read_object(&upload.bucket, &upload.name)
            .set_generation(upload.generation)
            .send()
            .await?;
        let highlights = reader.object();
        assert_eq!(highlights.storage_class, "STANDARD");
        assert_eq!(highlights.generation, upload.generation);
        assert_eq!(highlights.metageneration, upload.metageneration);
        assert_eq!(highlights.etag, upload.etag);
        assert_eq!(highlights.size, upload.size);

        let get = control
            .get_object()
            .set_bucket(&upload.bucket)
            .set_object(&upload.name)
            .set_generation(upload.generation)
            .send()
            .await?;
        // Not all fields match (the service returns different values with
        // equivalent semantics).
        assert_eq!(get.bucket, upload.bucket);
        assert_eq!(get.name, upload.name);
        assert_eq!(get.etag, upload.etag);
        assert_eq!(get.generation, upload.generation);
        assert_eq!(get.metageneration, upload.metageneration);
        assert_eq!(get.storage_class, upload.storage_class);
        assert_eq!(get.size, upload.size);
        assert_eq!(get.create_time, upload.create_time);
        assert_eq!(get.checksums, upload.checksums);
    }

    use gax::paginator::ItemPaginator;
    let mut list = control
        .list_objects()
        .set_parent(bucket_name)
        .set_prefix("b/c")
        .by_item();
    let mut from_list = Vec::new();
    while let Some(o) = list.next().await.transpose()? {
        from_list.push(o.name);
    }
    use std::collections::BTreeSet;
    let got = from_list
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let want = names
        .iter()
        .filter_map(|n| n.strip_prefix("b/c").map(|_| *n))
        .collect::<BTreeSet<_>>();

    assert_eq!(got, want);

    for name in names {
        control
            .delete_object()
            .set_bucket(bucket_name)
            .set_object(name)
            .with_idempotency(true)
            .send()
            .await?;
    }

    Ok(())
}

pub async fn create_test_bucket() -> Result<(StorageControl, Bucket)> {
    let project_id = crate::project_id()?;
    let client = StorageControl::builder()
        .with_tracing()
        .with_backoff_policy(
            gax::exponential_backoff::ExponentialBackoffBuilder::new()
                .with_initial_delay(Duration::from_secs(2))
                .with_maximum_delay(Duration::from_secs(8))
                .build()
                .unwrap(),
        )
        .with_retry_policy(
            gax::retry_policy::AlwaysRetry
                .with_attempt_limit(5)
                .with_time_limit(Duration::from_secs(16)),
        )
        .build()
        .await?;
    cleanup_stale_buckets(&client, &project_id).await?;

    let bucket_id = crate::random_bucket_id();

    tracing::info!("\nTesting create_bucket()");
    let create = client
        .create_bucket()
        .set_parent("projects/_")
        .set_bucket_id(bucket_id)
        .set_bucket(
            Bucket::new()
                .set_project(format!("projects/{project_id}"))
                .set_location("us-central1")
                .set_labels([("integration-test", "true")]),
        )
        .with_backoff_policy(test_backoff())
        .with_idempotency(true)
        .send()
        .await?;
    tracing::info!("SUCCESS on create_bucket: {create:?}");
    Ok((client, create))
}

pub async fn buckets(builder: storage::builder::storage_control::ClientBuilder) -> Result<()> {
    let project_id = crate::project_id()?;
    let client = builder.build().await?;

    cleanup_stale_buckets(&client, &project_id).await?;

    let bucket_id = crate::random_bucket_id();
    let bucket_name = format!("projects/_/buckets/{bucket_id}");

    println!("\nTesting create_bucket()");
    let create = client
        .create_bucket()
        .set_parent("projects/_")
        .set_bucket_id(bucket_id)
        .set_bucket(
            Bucket::new()
                .set_project(format!("projects/{project_id}"))
                .set_labels([("integration-test", "true")])
                // We need to set these properties on the bucket to use it with
                // the Folders API.
                .set_hierarchical_namespace(HierarchicalNamespace::new().set_enabled(true))
                .set_iam_config(IamConfig::new().set_uniform_bucket_level_access(
                    UniformBucketLevelAccess::new().set_enabled(true),
                )),
        )
        .with_backoff_policy(test_backoff())
        .with_idempotency(true)
        .send()
        .await?;
    println!("SUCCESS on create_bucket: {create:?}");
    assert_eq!(create.name, bucket_name);

    println!("\nTesting get_bucket()");
    let get = client.get_bucket().set_name(&bucket_name).send().await?;
    println!("SUCCESS on get_bucket: {get:?}");
    assert_eq!(get.name, bucket_name);

    println!("\nTesting list_buckets()");
    let mut buckets = client
        .list_buckets()
        .set_parent(format!("projects/{project_id}"))
        .by_item();
    let mut bucket_names = Vec::new();
    while let Some(bucket) = buckets.next().await {
        bucket_names.push(bucket?.name);
    }
    println!("SUCCESS on list_buckets");
    assert!(
        bucket_names.iter().any(|name| name == &bucket_name),
        "missing bucket name {bucket_name} in {bucket_names:?}"
    );

    buckets_iam(&client, &bucket_name).await?;
    folders(&client, &bucket_name).await?;

    println!("\nTesting delete_bucket()");
    client
        .delete_bucket()
        .set_name(bucket_name)
        .with_idempotency(true)
        .send()
        .await?;
    println!("SUCCESS on delete_bucket");

    Ok(())
}

async fn buckets_iam(client: &StorageControl, bucket_name: &str) -> Result<()> {
    let service_account = crate::test_service_account()?;

    println!("\nTesting get_iam_policy()");
    let policy = client
        .get_iam_policy()
        .set_resource(bucket_name)
        .send()
        .await?;
    println!("SUCCESS on get_iam_policy = {policy:?}");

    println!("\nTesting test_iam_permissions()");
    let response = client
        .test_iam_permissions()
        .set_resource(bucket_name)
        .set_permissions(["storage.buckets.get"])
        .send()
        .await?;
    println!("SUCCESS on test_iam_permissions = {response:?}");

    println!("\nTesting set_iam_policy()");
    let mut new_policy = policy.clone();
    new_policy.bindings.push(
        iam_v1::model::Binding::new()
            .set_role("roles/storage.legacyBucketReader")
            .set_members([format!("serviceAccount:{service_account}")]),
    );
    let policy = client
        .set_iam_policy()
        .set_resource(bucket_name)
        .set_update_mask(wkt::FieldMask::default().set_paths(["bindings"]))
        .set_policy(new_policy)
        .with_idempotency(true)
        .send()
        .await?;
    println!("SUCCESS on set_iam_policy = {policy:?}");

    Ok(())
}

async fn folders(client: &StorageControl, bucket_name: &str) -> Result<()> {
    let folder_name = format!("{bucket_name}/folders/test-folder/");
    let folder_rename = format!("{bucket_name}/folders/renamed-test-folder/");

    println!("\nTesting create_folder()");
    let create = client
        .create_folder()
        .set_parent(bucket_name)
        .set_folder_id("test-folder/")
        .send()
        .await?;
    println!("SUCCESS on create_folder: {create:?}");
    assert_eq!(create.name, folder_name);

    println!("\nTesting get_folder()");
    let get = client.get_folder().set_name(&folder_name).send().await?;
    println!("SUCCESS on get_folder: {get:?}");
    assert_eq!(get.name, folder_name);

    println!("\nTesting list_folders()");
    let mut folders = client.list_folders().set_parent(bucket_name).by_item();
    let mut folder_names = Vec::new();
    while let Some(folder) = folders.next().await {
        folder_names.push(folder?.name);
    }
    println!("SUCCESS on list_folders");
    assert!(
        folder_names.iter().any(|name| name == &folder_name),
        "missing folder name {folder_name} in {folder_names:?}"
    );

    println!("\nTesting rename_folder()");
    let rename = client
        .rename_folder()
        .set_name(folder_name)
        .set_destination_folder_id("renamed-test-folder/")
        .poller()
        .until_done()
        .await?;
    println!("SUCCESS on rename_folder: {rename:?}");
    assert_eq!(rename.name, folder_rename);

    println!("\nTesting delete_folder()");
    client
        .delete_folder()
        .set_name(folder_rename)
        .send()
        .await?;
    println!("SUCCESS on delete_folder");

    Ok(())
}

async fn cleanup_stale_buckets(client: &StorageControl, project_id: &str) -> Result<()> {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    let stale_deadline = SystemTime::now().duration_since(UNIX_EPOCH)?;
    let stale_deadline = stale_deadline - Duration::from_secs(48 * 60 * 60);
    let stale_deadline = wkt::Timestamp::clamp(stale_deadline.as_secs() as i64, 0);

    let mut buckets = client
        .list_buckets()
        .set_parent(format!("projects/{project_id}"))
        .by_item();
    let mut pending = Vec::new();
    let mut names = Vec::new();
    while let Some(bucket) = buckets.next().await {
        let bucket = bucket?;
        if bucket
            .labels
            .get("integration-test")
            .is_some_and(|v| v == "true")
            && bucket.create_time.is_some_and(|v| v < stale_deadline)
        {
            let client = client.clone();
            let name = bucket.name.clone();
            pending.push(tokio::spawn(
                async move { cleanup_bucket(client, name).await },
            ));
            names.push(bucket.name);
        }
    }

    let r: std::result::Result<Vec<_>, _> = futures::future::join_all(pending)
        .await
        .into_iter()
        .collect();
    r.map_err(Error::from)?
        .into_iter()
        .zip(names)
        .for_each(|(r, name)| println!("deleting bucket {name} resulted in {r:?}"));

    Ok(())
}

fn test_backoff() -> ExponentialBackoff {
    ExponentialBackoffBuilder::new()
        .with_initial_delay(Duration::from_secs(2))
        .with_maximum_delay(Duration::from_secs(10))
        .build()
        .unwrap()
}
