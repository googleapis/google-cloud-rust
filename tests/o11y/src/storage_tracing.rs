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

use super::Anonymous;
use google_cloud_storage::client::Storage;
use google_cloud_test_utils::test_layer::{AttributeValue, TestLayer, TestLayerGuard};
use httptest::{Expectation, Server, cycle, matchers::*, responders::status_code};
use std::collections::{BTreeMap, HashMap};

const CONTENTS: &str = "the quick brown fox jumps over the lazy dog";

pub async fn success_testlayer() -> anyhow::Result<()> {
    let (guard, mut server, client) = setup_fake_storage().await;

    // Test each method. Each one requires a different server set up and
    // different expectations. Better to refactor them and reset the
    // server expectations after each.
    read_object(&guard, &server, client.clone()).await?;
    server.verify_and_clear();
    write_object_single_shot(&guard, &server, client.clone()).await?;
    server.verify_and_clear();
    write_object_resumable(&guard, &server, client.clone()).await?;
    server.verify_and_clear();
    Ok(())
}

async fn read_object(
    guard: &TestLayerGuard,
    server: &Server,
    client: Storage,
) -> anyhow::Result<()> {
    // 2. Configure the fake server to expect a `read_object()` request.
    server.expect(
        Expectation::matching(all_of![
            request::method_path("GET", "/storage/v1/b/test-bucket/o/test-object"),
            request::query(url_decoded(contains(("alt", "media")))),
        ])
        .times(2)
        .respond_with(cycle![
            status_code(429),
            status_code(200)
                .body(CONTENTS)
                .append_header(
                    "x-goog-hash",
                    "crc32c=PBj01g==,md5=d63R1fQSI9VYL8pzalyzNQ==",
                )
                .append_header("x-goog-generation", 123456789)
                .append_header("x-goog-metageneration", 234)
                .append_header("x-goog-stored-content-length", CONTENTS.len())
                .append_header("x-goog-stored-content-encoding", "identity")
                .append_header("x-goog-storage-class", "STANDARD")
                .append_header("content-language", "en")
                .append_header("content-type", "text/plain")
                .append_header("content-disposition", "inline")
                .append_header("etag", "etagval"),
        ]),
    );

    // 3. Make the read_object() request and get all the data.
    let mut reader = client
        .read_object("projects/_/buckets/test-bucket", "test-object")
        .send()
        .await?;
    while reader.next().await.transpose()?.is_some() {}

    // 4. Capture all the spans.
    let spans = TestLayer::capture(guard);

    const EXPECTED_NAME: &str = "http_request";
    // 5. Find the span for the underlying HTTP requests.
    let found = spans
        .iter()
        .filter(|s| s.name == EXPECTED_NAME)
        .collect::<Vec<_>>();
    let (retry, success) = match found[..] {
        [retry, success] => (retry, success),
        _ => panic!("expected two http_request spans, got={spans:?}"),
    };

    // The actual version changes as the library changes, we don't want to hard-code its value.
    let version = retry.attributes.get("gcp.client.version").unwrap();
    let common = [
        ("http.request.method", "GET".into()),
        ("gcp.client.service", "storage".into()),
        ("gcp.client.version", version.clone()),
        ("gcp.client.repo", "googleapis/google-cloud-rust".into()),
        ("gcp.client.artifact", "google-cloud-storage".into()),
        ("gcp.client.language", "rust".into()),
        ("otel.name", "GET /storage/v1/b/{bucket}/o/{object}".into()),
        ("otel.kind", "Client".into()),
        ("otel.status_code", "UNSET".into()),
        ("rpc.system", "http".into()),
        ("server.address", server.addr().ip().to_string().into()),
        ("server.port", (server.addr().port() as i64).into()),
        ("url.domain", "storage.googleapis.com".into()),
        (
            "url.full",
            format!(
                "http://{}/storage/v1/b/test-bucket/o/test-object?alt=media",
                server.addr()
            )
            .into(),
        ),
        ("url.scheme", "http".into()),
        ("url.template", "/storage/v1/b/{bucket}/o/{object}".into()),
        (
            "gcp.resource.name",
            "//storage.googleapis.com/projects/_/buckets/test-bucket".into(),
        ),
    ];

    let want = join_want(
        common.clone(),
        [
            ("http.response.status_code", 429_i64.into()),
            ("http.response.body.size", 0_i64.into()),
        ],
    );
    let unexpected = find_unexpected(&retry.attributes, &want);
    let missing = find_missing(&retry.attributes, &want);
    assert_eq!(
        BTreeMap::from_iter(retry.attributes.clone().into_iter()),
        want,
        "\nmissing={missing:?}\nunexpected={unexpected:?}"
    );

    let want = join_want(
        common.clone(),
        [
            ("http.response.status_code", 200_i64.into()),
            ("http.response.body.size", (CONTENTS.len() as i64).into()),
            ("http.request.resend_count", 1_i64.into()),
        ],
    );
    let unexpected = find_unexpected(&success.attributes, &want);
    let missing = find_missing(&success.attributes, &want);
    assert_eq!(
        BTreeMap::from_iter(success.attributes.clone().into_iter()),
        want,
        "\nmissing={missing:?}\nunexpected={unexpected:?}"
    );

    Ok(())
}

async fn write_object_single_shot(
    guard: &TestLayerGuard,
    server: &Server,
    client: Storage,
) -> anyhow::Result<()> {
    server.expect(
        Expectation::matching(all_of![request::method_path(
            "POST",
            "/upload/storage/v1/b/test-bucket/o"
        ),])
        .times(2)
        .respond_with(cycle![
            status_code(429),
            status_code(200)
                .body("{}")
                .append_header("content-type", "application/json")
        ]),
    );

    let object = client
        .write_object("projects/_/buckets/test-bucket", "test-object", CONTENTS)
        .set_if_generation_match(0)
        .send_unbuffered()
        .await;
    assert!(object.is_ok(), "{object:?}");

    let spans = TestLayer::capture(guard);

    const EXPECTED_NAME: &str = "http_request";
    // 5. Find the span for the underlying HTTP requests.
    let found = spans
        .iter()
        .filter(|s| s.name == EXPECTED_NAME)
        .collect::<Vec<_>>();
    let (retry, success) = match found[..] {
        [retry, success] => (retry, success),
        _ => panic!("expected two http_request spans, got={spans:?}"),
    };

    // The actual version changes as the library changes, we don't want to hard-code its value.
    let version = retry.attributes.get("gcp.client.version").unwrap();
    let common = [
        ("http.request.method", "POST".into()),
        ("gcp.client.service", "storage".into()),
        ("gcp.client.version", version.clone()),
        ("gcp.client.repo", "googleapis/google-cloud-rust".into()),
        ("gcp.client.artifact", "google-cloud-storage".into()),
        ("gcp.client.language", "rust".into()),
        ("otel.name", "POST /upload/storage/v1/b/{bucket}/o".into()),
        ("otel.kind", "Client".into()),
        ("otel.status_code", "UNSET".into()),
        ("rpc.system", "http".into()),
        ("server.address", server.addr().ip().to_string().into()),
        ("server.port", (server.addr().port() as i64).into()),
        ("url.domain", "storage.googleapis.com".into()),
        (
            "url.full",
            format!(
                "http://{}/upload/storage/v1/b/test-bucket/o?{}",
                server.addr(),
                "uploadType=multipart&name=test-object&ifGenerationMatch=0",
            )
            .into(),
        ),
        ("url.scheme", "http".into()),
        ("url.template", "/upload/storage/v1/b/{bucket}/o".into()),
        (
            "gcp.resource.name",
            "//storage.googleapis.com/projects/_/buckets/test-bucket".into(),
        ),
    ];

    let want = join_want(
        common.clone(),
        [
            ("http.response.status_code", 429_i64.into()),
            ("http.response.body.size", 0_i64.into()),
        ],
    );
    let unexpected = find_unexpected(&retry.attributes, &want);
    let missing = find_missing(&retry.attributes, &want);
    assert_eq!(
        BTreeMap::from_iter(retry.attributes.clone().into_iter()),
        want,
        "\nmissing={missing:?}\nunexpected={unexpected:?}"
    );

    let want = join_want(
        common.clone(),
        [
            ("http.response.status_code", 200_i64.into()),
            ("http.response.body.size", ("{}".len() as i64).into()),
            ("http.request.resend_count", 1_i64.into()),
        ],
    );
    let unexpected = find_unexpected(&success.attributes, &want);
    let missing = find_missing(&success.attributes, &want);
    assert_eq!(
        BTreeMap::from_iter(success.attributes.clone().into_iter()),
        want,
        "\nmissing={missing:?}\nunexpected={unexpected:?}"
    );
    Ok(())
}

async fn write_object_resumable(
    guard: &TestLayerGuard,
    server: &Server,
    client: Storage,
) -> anyhow::Result<()> {
    let upload_url = server.url("/upload/test-upload-id-001");
    server.expect(
        Expectation::matching(all_of![request::method_path(
            "POST",
            "/upload/storage/v1/b/test-bucket/o"
        ),])
        .times(2)
        .respond_with(cycle![
            status_code(429),
            status_code(200).append_header("location", upload_url.to_string()),
        ]),
    );
    server.expect(
        Expectation::matching(all_of![request::method_path(
            "PUT",
            upload_url.path().to_string()
        ),])
        .respond_with(cycle![
            status_code(200)
                .body("{}")
                .append_header("content-type", "application/json")
        ]),
    );

    let object = client
        .write_object("projects/_/buckets/test-bucket", "test-object", CONTENTS)
        .with_resumable_upload_threshold(0_usize)
        .set_if_generation_match(0)
        .send_unbuffered()
        .await;
    assert!(object.is_ok(), "{object:?}");

    let spans = TestLayer::capture(guard);

    const EXPECTED_NAME: &str = "http_request";
    // 5. Find the span for the underlying HTTP requests.
    let found = spans
        .iter()
        .filter(|s| s.name == EXPECTED_NAME)
        .collect::<Vec<_>>();
    let (retry, success, put) = match found[..] {
        [retry, success, put] => (retry, success, put),
        _ => panic!("expected two http_request spans, got={spans:?}"),
    };

    // The actual version changes as the library changes, we don't want to hard-code its value.
    let version = retry.attributes.get("gcp.client.version").unwrap();
    let post_url = format!(
        "http://{}/upload/storage/v1/b/test-bucket/o?{}",
        server.addr(),
        "uploadType=resumable&name=test-object&ifGenerationMatch=0",
    );
    let common = [
        ("gcp.client.service", "storage".into()),
        ("gcp.client.version", version.clone()),
        ("gcp.client.repo", "googleapis/google-cloud-rust".into()),
        ("gcp.client.artifact", "google-cloud-storage".into()),
        ("gcp.client.language", "rust".into()),
        ("otel.kind", "Client".into()),
        ("otel.status_code", "UNSET".into()),
        ("rpc.system", "http".into()),
        ("server.address", server.addr().ip().to_string().into()),
        ("server.port", (server.addr().port() as i64).into()),
        ("url.domain", "storage.googleapis.com".into()),
        ("url.scheme", "http".into()),
        ("url.template", "/upload/storage/v1/b/{bucket}/o".into()),
        (
            "gcp.resource.name",
            "//storage.googleapis.com/projects/_/buckets/test-bucket".into(),
        ),
    ];

    let want = join_want(
        common.clone(),
        [
            ("http.request.method", "POST".into()),
            ("otel.name", "POST /upload/storage/v1/b/{bucket}/o".into()),
            ("url.full", post_url.clone().into()),
            ("http.response.status_code", 429_i64.into()),
            ("http.response.body.size", 0_i64.into()),
        ],
    );
    let unexpected = find_unexpected(&retry.attributes, &want);
    let missing = find_missing(&retry.attributes, &want);
    assert_eq!(
        BTreeMap::from_iter(retry.attributes.clone().into_iter()),
        want,
        "\nmissing={missing:?}\nunexpected={unexpected:?}"
    );

    let want = join_want(
        common.clone(),
        [
            ("http.request.method", "POST".into()),
            ("otel.name", "POST /upload/storage/v1/b/{bucket}/o".into()),
            ("url.full", post_url.clone().into()),
            ("http.response.status_code", 200_i64.into()),
            ("http.response.body.size", 0_i64.into()),
            ("http.request.resend_count", 1_i64.into()),
        ],
    );
    let unexpected = find_unexpected(&success.attributes, &want);
    let missing = find_missing(&success.attributes, &want);
    assert_eq!(
        BTreeMap::from_iter(success.attributes.clone().into_iter()),
        want,
        "\nmissing={missing:?}\nunexpected={unexpected:?}"
    );

    let want = join_want(
        common.clone(),
        [
            ("http.request.method", "PUT".into()),
            ("otel.name", "PUT /upload/storage/v1/b/{bucket}/o".into()),
            ("url.full", upload_url.to_string().into()),
            ("http.response.status_code", 200_i64.into()),
            ("http.response.body.size", ("{}".len() as i64).into()),
        ],
    );
    let unexpected = find_unexpected(&put.attributes, &want);
    let missing = find_missing(&put.attributes, &want);
    assert_eq!(
        BTreeMap::from_iter(put.attributes.clone().into_iter()),
        want,
        "\nmissing={missing:?}\nunexpected={unexpected:?}"
    );
    Ok(())
}

fn join_want<C, E>(common: C, extra: E) -> BTreeMap<String, AttributeValue>
where
    C: IntoIterator<Item = (&'static str, AttributeValue)>,
    E: IntoIterator<Item = (&'static str, AttributeValue)>,
{
    common
        .into_iter()
        .chain(extra)
        .map(|(k, v)| (k.to_string(), v))
        .collect()
}

fn find_unexpected<'a>(
    got: &'a HashMap<String, AttributeValue>,
    want: &'a BTreeMap<String, AttributeValue>,
) -> Vec<(&'a String, &'a AttributeValue)> {
    got.iter()
        .filter(|(k, v)| want.get(*k) != Some(v))
        .collect()
}

fn find_missing<'a>(
    got: &'a HashMap<String, AttributeValue>,
    want: &'a BTreeMap<String, AttributeValue>,
) -> Vec<(&'a String, &'a AttributeValue)> {
    want.iter()
        .filter(|(k, v)| got.get(*k) != Some(v))
        .collect()
}

async fn setup_fake_storage() -> (TestLayerGuard, Server, Storage) {
    let guard = TestLayer::initialize();
    let server = Server::run();
    let endpoint = server.url("/").to_string();
    let endpoint = endpoint.trim_end_matches('/');
    let client = Storage::builder()
        .with_endpoint(endpoint)
        .with_credentials(Anonymous::new().build())
        .with_tracing()
        .build()
        .await
        .expect("failed to build client");

    (guard, server, client)
}
