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
use storage::client::Storage;
use storage::streaming_source::{Seek, SizeHint, StreamingSource};

pub async fn repro(
    builder: storage::builder::storage::ClientBuilder,
    bucket_name: &str,
) -> Result<()> {
    // Run all the upload tests in parallel, using the same bucket.
    // Creating a new bucket is rate-limited, and slow. Creating objects
    // is relatively cheap.
    let client = builder.build().await?;
    repro_3608_buffered(&client, bucket_name).await?;
    Ok(())
}

pub async fn run(
    builder: storage::builder::storage::ClientBuilder,
    bucket_name: &str,
) -> Result<()> {
    // Run all the upload tests in parallel, using the same bucket.
    // Creating a new bucket is rate-limited, and slow. Creating objects
    // is relatively cheap.
    let client = builder.build().await?;
    let pending: Vec<std::pin::Pin<Box<dyn Future<Output = Result<()>>>>> = vec![
        Box::pin(upload_buffered(&client, bucket_name)),
        Box::pin(upload_buffered_resumable_known_size(&client, bucket_name)),
        Box::pin(upload_buffered_resumable_unknown_size(&client, bucket_name)),
        Box::pin(upload_unbuffered_resumable_known_size(&client, bucket_name)),
        Box::pin(upload_unbuffered_resumable_unknown_size(
            &client,
            bucket_name,
        )),
        Box::pin(repro_3608_buffered(&client, bucket_name)),
        Box::pin(repro_3608_unbuffered(&client, bucket_name)),
        Box::pin(abort_upload_buffered(&client, bucket_name)),
        Box::pin(abort_upload_unbuffered(&client, bucket_name)),
        Box::pin(checksums(&client, bucket_name)),
    ];
    let result: Result<Vec<_>> = futures::future::join_all(pending.into_iter())
        .await
        .into_iter()
        .collect();
    let _ = result?;
    Ok(())
}

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

pub(super) async fn upload_buffered(client: &Storage, bucket_name: &str) -> Result<()> {
    tracing::info!("testing upload_object_buffered() [1]");
    let insert = client
        .write_object(bucket_name, "upload_buffered/empty.txt", "")
        .set_if_generation_match(0)
        .send_buffered()
        .await?;
    tracing::info!("success with insert={insert:?}");
    assert_eq!(insert.size, 0_i64);

    let payload = bytes::Bytes::from_owner(Vec::from_iter((0..128 * 1024).map(|_| 0_u8)));
    tracing::info!("testing upload_object_buffered() [2]");
    let insert = client
        .write_object(bucket_name, "upload_buffered/128K.txt", payload)
        .set_if_generation_match(0)
        .send_buffered()
        .await?;
    tracing::info!("success with insert={insert:?}");
    assert_eq!(insert.size, 128 * 1024_i64);

    let payload = bytes::Bytes::from_owner(Vec::from_iter((0..512 * 1024).map(|_| 0_u8)));
    tracing::info!("testing upload_object_buffered() [3]");
    let insert = client
        .write_object(bucket_name, "upload_buffered/512K.txt", payload)
        .set_if_generation_match(0)
        .send_buffered()
        .await?;
    tracing::info!("success with insert={insert:?}");
    assert_eq!(insert.size, 512 * 1024_i64);

    Ok(())
}

pub(super) async fn upload_buffered_resumable_known_size(
    client: &Storage,
    bucket_name: &str,
) -> Result<()> {
    tracing::info!("testing send_unbuffered() [1]");
    let payload = TestDataSource::new(0_u64);
    let insert = client
        .write_object(
            bucket_name,
            "upload_buffered_resumable_known_size/empty.txt",
            payload,
        )
        .set_if_generation_match(0)
        .with_resumable_upload_threshold(0_usize)
        .send_buffered()
        .await?;
    tracing::info!("success with insert={insert:?}");
    assert_eq!(insert.size, 0_i64);

    let payload = TestDataSource::new(128 * 1024_u64);
    tracing::info!("testing upload_object_buffered() [2]");
    let insert = client
        .write_object(
            bucket_name,
            "upload_buffered_resumable_known_size/128K.txt",
            payload,
        )
        .set_if_generation_match(0)
        .with_resumable_upload_threshold(0_usize)
        .send_buffered()
        .await?;
    tracing::info!("success with insert={insert:?}");
    assert_eq!(insert.size, 128 * 1024_i64);

    let payload = TestDataSource::new(512 * 1024_u64);
    tracing::info!("testing upload_object_buffered() [3]");
    let insert = client
        .write_object(
            bucket_name,
            "upload_buffered_resumable_known_size/512K.txt",
            payload,
        )
        .set_if_generation_match(0)
        .with_resumable_upload_threshold(0_usize)
        .send_buffered()
        .await?;
    tracing::info!("success with insert={insert:?}");
    assert_eq!(insert.size, 512 * 1024_i64);

    Ok(())
}

pub(super) async fn upload_buffered_resumable_unknown_size(
    client: &Storage,
    bucket_name: &str,
) -> Result<()> {
    tracing::info!("testing send_buffered() [1]");
    let payload = TestDataSource::new(0_u64).without_size_hint();
    let insert = client
        .write_object(
            bucket_name,
            "upload_buffered_resumable_unknown_size/empty.txt",
            payload,
        )
        .set_if_generation_match(0)
        .with_resumable_upload_threshold(0_usize)
        .send_buffered()
        .await?;
    tracing::info!("success with insert={insert:?}");
    assert_eq!(insert.size, 0_i64);

    let payload = TestDataSource::new(128 * 1024_u64).without_size_hint();
    tracing::info!("testing send_buffered() [2]");
    let insert = client
        .write_object(
            bucket_name,
            "upload_buffered_resumable_unknown_size/128K.txt",
            payload,
        )
        .set_if_generation_match(0)
        .with_resumable_upload_threshold(0_usize)
        .send_buffered()
        .await?;
    tracing::info!("success with insert={insert:?}");
    assert_eq!(insert.size, 128 * 1024_i64);

    let payload = TestDataSource::new(512 * 1024_u64).without_size_hint();
    tracing::info!("testing upload_object_buffered() [3]");
    let insert = client
        .write_object(
            bucket_name,
            "upload_buffered_resumable_unknown_size/512K.txt",
            payload,
        )
        .set_if_generation_match(0)
        .with_resumable_upload_threshold(0_usize)
        .send_buffered()
        .await?;
    tracing::info!("success with insert={insert:?}");
    assert_eq!(insert.size, 512 * 1024_i64);

    let payload = TestDataSource::new(500 * 1024_u64).without_size_hint();
    tracing::info!("testing send_buffered() [4]");
    let insert = client
        .write_object(
            bucket_name,
            "upload_buffered_resumable_unknown_size/500K.txt",
            payload,
        )
        .set_if_generation_match(0)
        .with_resumable_upload_threshold(0_usize)
        .send_buffered()
        .await?;
    tracing::info!("success with insert={insert:?}");
    assert_eq!(insert.size, 500 * 1024_i64);

    Ok(())
}

async fn repro_3608_buffered(client: &Storage, bucket_name: &str) -> Result<()> {
    const BUFFER_SIZE: u64 = 256 * 1024;
    const OBJECT_SIZE: u64 = 4 * BUFFER_SIZE;
    let payload = TestDataSource::new(OBJECT_SIZE).without_size_hint();
    tracing::info!("testing repro_3608()");
    let insert = client
        .write_object(bucket_name, "repro-3608/buffered.txt", payload)
        .set_if_generation_match(0)
        .with_resumable_upload_buffer_size(BUFFER_SIZE as usize)
        .send_buffered()
        .await?;
    tracing::info!("success with insert={insert:?}");
    assert_eq!(insert.size, OBJECT_SIZE as i64);

    Ok(())
}

async fn repro_3608_unbuffered(client: &Storage, bucket_name: &str) -> Result<()> {
    const BUFFER_SIZE: u64 = 256 * 1024;
    const OBJECT_SIZE: u64 = 4 * BUFFER_SIZE;
    let payload = TestDataSource::new(OBJECT_SIZE).without_size_hint();
    tracing::info!("testing repro_3608()");
    let insert = client
        .write_object(bucket_name, "repro-3608/unbuffered.txt", payload)
        .set_if_generation_match(0)
        .with_resumable_upload_buffer_size(BUFFER_SIZE as usize)
        .send_unbuffered()
        .await?;
    tracing::info!("success with insert={insert:?}");
    assert_eq!(insert.size, OBJECT_SIZE as i64);

    Ok(())
}

async fn upload_unbuffered_resumable_known_size(client: &Storage, bucket_name: &str) -> Result<()> {
    tracing::info!("testing send_unbuffered() [1]");
    let payload = TestDataSource::new(0_u64);
    let insert = client
        .write_object(
            bucket_name,
            "upload_unbuffered_resumable_known_size/empty.txt",
            payload,
        )
        .set_if_generation_match(0)
        .with_resumable_upload_threshold(0_usize)
        .send_unbuffered()
        .await?;
    tracing::info!("success with insert={insert:?}");
    assert_eq!(insert.size, 0_i64);

    let payload = TestDataSource::new(128 * 1024_u64);
    tracing::info!("testing upload_object_buffered() [2]");
    let insert = client
        .write_object(
            bucket_name,
            "upload_unbuffered_resumable_known_size/128K.txt",
            payload,
        )
        .set_if_generation_match(0)
        .with_resumable_upload_threshold(0_usize)
        .send_unbuffered()
        .await?;
    tracing::info!("success with insert={insert:?}");
    assert_eq!(insert.size, 128 * 1024_i64);

    let payload = TestDataSource::new(512 * 1024_u64);
    tracing::info!("testing upload_object_buffered() [3]");
    let insert = client
        .write_object(
            bucket_name,
            "upload_unbuffered_resumable_known_size/512K.txt",
            payload,
        )
        .set_if_generation_match(0)
        .with_resumable_upload_threshold(0_usize)
        .send_unbuffered()
        .await?;
    tracing::info!("success with insert={insert:?}");
    assert_eq!(insert.size, 512 * 1024_i64);

    Ok(())
}

async fn upload_unbuffered_resumable_unknown_size(
    client: &Storage,
    bucket_name: &str,
) -> Result<()> {
    tracing::info!("testing send_unbuffered() [1]");
    let payload = TestDataSource::new(0_u64).without_size_hint();
    let insert = client
        .write_object(
            bucket_name,
            "upload_unbuffered_resumable_unknown_size/empty.txt",
            payload,
        )
        .set_if_generation_match(0)
        .with_resumable_upload_threshold(0_usize)
        .send_unbuffered()
        .await?;
    tracing::info!("success with insert={insert:?}");
    assert_eq!(insert.size, 0_i64);

    let payload = TestDataSource::new(128 * 1024_u64).without_size_hint();
    tracing::info!("testing upload_object_buffered() [2]");
    let insert = client
        .write_object(
            bucket_name,
            "upload_unbuffered_resumable_unknown_size/128K.txt",
            payload,
        )
        .set_if_generation_match(0)
        .with_resumable_upload_threshold(0_usize)
        .send_unbuffered()
        .await?;
    tracing::info!("success with insert={insert:?}");
    assert_eq!(insert.size, 128 * 1024_i64);

    let payload = TestDataSource::new(512 * 1024_u64).without_size_hint();
    tracing::info!("testing upload_object_buffered() [3]");
    let insert = client
        .write_object(
            bucket_name,
            "upload_unbuffered_resumable_unknown_size/512K.txt",
            payload,
        )
        .set_if_generation_match(0)
        .with_resumable_upload_threshold(0_usize)
        .send_unbuffered()
        .await?;
    tracing::info!("success with insert={insert:?}");
    assert_eq!(insert.size, 512 * 1024_i64);

    let payload = TestDataSource::new(500 * 1024_u64).without_size_hint();
    tracing::info!("testing upload_object_buffered() [4]");
    let insert = client
        .write_object(
            bucket_name,
            "upload_unbuffered_resumable_unknown_size/500K.txt",
            payload,
        )
        .set_if_generation_match(0)
        .with_resumable_upload_threshold(0_usize)
        .send_unbuffered()
        .await?;
    tracing::info!("success with insert={insert:?}");
    assert_eq!(insert.size, 500 * 1024_i64);

    Ok(())
}

const ABORT_TEST_STOP: u64 = 512 * 1024;
const ABORT_TEST_SIZE: u64 = 1024 * 1024;

struct AbortUploadTestCase {
    name: String,
    upload: storage::builder::storage::WriteObject<TestDataSource>,
}

fn abort_upload_test_cases(
    client: &Storage,
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
            let name = format!("abort-upload/{prefix}/{}-{}.txt", s.0, t.0);
            let upload = client
                .write_object(bucket_name, &name, s.1.clone())
                .set_if_generation_match(0)
                .with_resumable_upload_threshold(t.1);
            uploads.push(AbortUploadTestCase { name, upload });
        }
    }
    uploads
}

async fn abort_upload_unbuffered(client: &Storage, bucket_name: &str) -> Result<()> {
    let test_cases = abort_upload_test_cases(client, bucket_name, "unbuffered");

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

async fn abort_upload_buffered(client: &Storage, bucket_name: &str) -> Result<()> {
    let test_cases = abort_upload_test_cases(client, bucket_name, "buffered");

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

pub async fn checksums(client: &Storage, bucket_name: &str) -> Result<()> {
    tracing::info!("checksums test, using bucket {bucket_name}");

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
