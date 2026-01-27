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
use firestore::client::Firestore;
use firestore::model;
use gax::paginator::ItemPaginator as _;
use google_cloud_test_utils::runtime_config::project_id;
use rand::{Rng, distr::Alphanumeric};

pub const COLLECTION_ID_LENGTH: usize = 32;

fn new_collection_id() -> String {
    let prefix = "doc-";
    let collection_id: String = rand::rng()
        .sample_iter(&Alphanumeric)
        .take(COLLECTION_ID_LENGTH - prefix.len())
        .map(char::from)
        .collect();
    format!("{prefix}{collection_id}")
}

pub async fn basic() -> Result<()> {
    let client = Firestore::builder().with_tracing().build().await?;
    cleanup_stale_documents(&client).await?;

    let project_id = project_id()?;
    let collection_id = new_collection_id();
    let response = client
        .create_document()
        .set_parent(format!(
            "projects/{project_id}/databases/(default)/documents"
        ))
        .set_collection_id(&collection_id)
        .set_document(model::Document::new().set_fields([
            (
                "greeting",
                model::Value::new().set_string_value("Hello World!"),
            ),
            (
                "integration-test",
                model::Value::new().set_boolean_value(true),
            ),
        ]))
        .send()
        .await?;
    println!("SUCCESS on create_document: {response:?}");

    let document_name = response.name;
    let response = client
        .get_document()
        .set_name(&document_name)
        .send()
        .await?;
    println!("SUCCESS on get_document: {response:?}");

    let mut documents = client
        .list_documents()
        .set_parent(format!(
            "projects/{project_id}/databases/(default)/documents"
        ))
        .set_collection_id(&collection_id)
        .by_item();
    while let Some(doc) = documents.next().await {
        let doc = doc?;
        println!("  ITEM = {doc:?}");
    }
    println!("SUCCESS on list_documents:");

    let response = client
        .update_document()
        .set_document(
            model::Document::new()
                .set_name(&document_name)
                .set_fields([("greeting", model::Value::new().set_string_value("Goodbye."))]),
        )
        .set_update_mask(model::DocumentMask::new().set_field_paths(["greeting"]))
        .send()
        .await?;
    println!("SUCCESS on update_document: {response:?}");

    client
        .delete_document()
        .set_name(&document_name)
        .send()
        .await?;
    println!("SUCCESS on delete_document");

    Ok(())
}

async fn cleanup_stale_documents(client: &Firestore) -> Result<()> {
    use gax::options::RequestOptionsBuilder;
    use gax::retry_policy::RetryPolicyExt;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    let stale_deadline = SystemTime::now().duration_since(UNIX_EPOCH)?;
    let stale_deadline = stale_deadline - Duration::from_secs(48 * 60 * 60);
    let stale_deadline = wkt::Timestamp::clamp(stale_deadline.as_secs() as i64, 0);

    let mut stale_documents = Vec::new();

    let project_id = project_id()?;
    let mut documents = client
        .list_documents()
        .set_parent(format!(
            "projects/{project_id}/databases/(default)/documents"
        ))
        .by_item();
    while let Some(doc) = documents.next().await {
        let doc = doc?;
        if let Some(true) = doc.create_time.map(|v| v >= stale_deadline) {
            continue;
        }
        if doc
            .fields
            .get("integration-test")
            .and_then(|v| v.boolean_value())
            .is_some_and(|v| *v)
        {
            stale_documents.push(doc.name);
        }
    }
    let stale_documents = stale_documents;
    let pending = stale_documents
        .iter()
        .map(|name| {
            client
                .delete_document()
                .set_name(name)
                .with_retry_policy(gax::retry_policy::AlwaysRetry.with_attempt_limit(3))
                .send()
        })
        .collect::<Vec<_>>();

    futures::future::join_all(pending)
        .await
        .into_iter()
        .zip(stale_documents)
        .for_each(|(r, name)| println!("Deleting stale document {name} = {r:?}"));

    Ok(())
}
