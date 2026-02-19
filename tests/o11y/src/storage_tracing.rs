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
use httptest::{Expectation, Server, matchers::*, responders::status_code};
use std::collections::BTreeMap;

pub async fn success_testlayer() -> anyhow::Result<()> {
    // 1. Create a fake server and a client pointing to it.
    let (guard, server, client) = setup_fake_storage().await;

    // 2. Configure the fake server to expect a `read_object()` request.
    const CONTENTS: &str = "the quick brown fox jumps over the lazy dog";
    server.expect(
        Expectation::matching(all_of![
            request::method_path("GET", "/storage/v1/b/test-bucket/o/test-object"),
            request::query(url_decoded(contains(("alt", "media")))),
        ])
        .respond_with(
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
        ),
    );

    // 3. Make the read_object() request and get all the data.
    let mut reader = client
        .read_object("projects/_/buckets/test-bucket", "test-object")
        .send()
        .await?;
    while reader.next().await.transpose()?.is_some() {}

    // 4. Capture all the spans.
    let spans = TestLayer::capture(&guard);

    const EXPECTED_NAME: &str = "http_request";
    // 5. Find the span for the underlying HTTP request.
    let t4_span = spans.iter().find(|s| s.name == EXPECTED_NAME);
    let t4_span = t4_span.unwrap_or_else(|| panic!("missing http_request span, spans={spans:?}"));

    // The actual version changes as the library changes, we don't want to hard-code its value.
    let version = t4_span.attributes.get("gcp.client.version").unwrap();
    // Use a BTreeMap<> to simplify comparisons when there is a mismatch.
    let expected_attributes: BTreeMap<String, AttributeValue> = [
        ("otel.name", "GET /storage/v1/b/{bucket}/o/{object}".into()),
        ("otel.kind", "Client".into()),
        ("rpc.system", "http".into()),
        ("gcp.client.service", "storage".into()),
        ("gcp.client.version", version.clone()),
        ("gcp.client.repo", "googleapis/google-cloud-rust".into()),
        ("gcp.client.artifact", "google-cloud-storage".into()),
        ("gcp.client.language", "rust".into()),
        ("otel.status_code", "UNSET".into()),
        ("http.response.status_code", 200_i64.into()),
        ("http.request.method", "GET".into()),
        ("http.response.body.size", (CONTENTS.len() as i64).into()),
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
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v))
    .collect();

    let unexpected = t4_span
        .attributes
        .iter()
        .filter(|(k, v)| expected_attributes.get(*k) != Some(v))
        .collect::<Vec<_>>();
    let missing = expected_attributes
        .iter()
        .filter(|(k, v)| t4_span.attributes.get(*k) != Some(v))
        .collect::<Vec<_>>();
    assert_eq!(
        BTreeMap::from_iter(t4_span.attributes.clone().into_iter()),
        expected_attributes,
        "\nmissing={missing:?}\nunexpected={unexpected:?}"
    );

    Ok(())
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
