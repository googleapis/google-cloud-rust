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
use bigquery::client::ClientBuilder;
use rand::{Rng, distr::Alphanumeric};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use wkt::Value;

pub async fn run_query(builder: ClientBuilder) -> Result<()> {
    let project_id = crate::project_id()?;
    let client = builder.with_project_id(project_id).build().await?;

    // Simple query
    let query = client
        .query("SELECT 17 as num, CURRENT_TIMESTAMP() as ts, SESSION_USER() as bar".to_string())
        .await?;
    let mut iter = query.read().await?;
    let mut rows = vec![];
    while let Some(row) = iter.next().await {
        rows.push(row?);
    }

    assert_eq!(rows.len(), 1);
    let first_row = rows[0].clone();

    // Read each field as a native type
    let num: Option<i64> = first_row.get("num")?;
    assert_eq!(num, Some(17));
    let ts: Option<chrono::DateTime<chrono::Utc>> = first_row.get("ts")?;
    assert!(ts.is_some());
    let bar: Option<String> = first_row.get("bar")?;
    assert!(bar.is_some());
    println!("row: {num:?}, {ts:?}, {bar:?}");

    // Parse as user defined struct
    #[derive(serde::Deserialize, Debug)]
    struct MyStruct {
        num: i64,
        #[serde(with = "bigquery::value")]
        ts: chrono::DateTime<chrono::Utc>,
        bar: String,
    }

    let my_struct: MyStruct =
        serde_json::from_value(first_row.to_value()).expect("Should parse as user defined struct");
    assert_eq!(my_struct.num, 17);
    assert_eq!(my_struct.ts, ts.unwrap());
    assert_eq!(my_struct.bar, bar.unwrap());
    println!("struct row: {my_struct:?}");

    Ok(())
}

pub async fn run_query_nested_data(builder: ClientBuilder) -> Result<()> {
    let project_id = crate::project_id()?;
    let client = builder.with_project_id(project_id).build().await?;

    // deeply nested query result
    let query = client
        .query("SELECT [STRUCT(STRUCT('1' as a, '2' as b) as object)] as nested".to_string())
        .await?;
    let mut iter = query.read().await?;
    let mut rows = vec![];
    while let Some(row) = iter.next().await {
        rows.push(row?);
    }

    assert_eq!(rows.len(), 1);
    let first_row = rows[0].clone();

    // Read nested field
    let nested_array: Option<Vec<Value>> = first_row.get("nested")?;
    assert!(nested_array.is_some());
    let nested_array = nested_array.unwrap();
    assert_eq!(nested_array.len(), 1);
    let object = nested_array[0].as_object().expect("nested item should be an object");
    let nested_object = object.get("object")
        .expect("msg object should have `object` field")
        .as_object()
        .expect("msg object should have `object` object field");
    assert_eq!(nested_object.get("a")
        .expect("msg object should have `a` field")
        .as_str()
        .expect("msg object should have `a` string field"), "1");
    assert_eq!(nested_object.get("b")
        .expect("msg object should have `b` field")
        .as_str()
        .expect("msg object should have `b` string field"), "2");

    // Parse as user defined struct
    #[derive(serde::Deserialize, Debug)]
    struct MyStruct {
        nested: Vec<NestedArrayObject>,
    }

    #[derive(serde::Deserialize, Debug)]
    struct NestedArrayObject{
        object: NestedObject
    }

    #[derive(serde::Deserialize, Debug)]
    struct NestedObject {        
        a: String,
        b: String
    }    

    let my_struct: MyStruct =
        serde_json::from_value(first_row.to_value()).expect("Should parse as user defined struct");
    assert_eq!(my_struct.nested[0].object.a, "1");
    assert_eq!(my_struct.nested[0].object.b, "2");
    println!("struct row: {my_struct:?}");

    Ok(())
}

pub async fn run_dml_query(
    builder: ClientBuilder,
    dataset_builder: bigquery_admin::builder::dataset_service::ClientBuilder,
) -> Result<()> {
    let project_id = crate::project_id()?;
    let client = builder.with_project_id(project_id.clone()).build().await?;

    let dataset_client = dataset_builder.build().await?;
    cleanup_stale_datasets(&dataset_client, &project_id.clone()).await?;

    let dataset_id = create_test_dataset(&dataset_client, project_id).await?;
    println!("CREATED DATASET = {dataset_id:?}");

    // DML query
    let table_id = "table_dml";
    let create_table_query =
        format!("CREATE OR REPLACE TABLE {dataset_id}.{table_id} (x INT64)").to_string();
    let mut create_query = client.query(create_table_query).await?;
    create_query.wait().await?;

    let insert_query =
        format!("INSERT INTO {dataset_id}.{table_id} (x) VALUES (1), (2)").to_string();
    let mut dml_query = client.query(insert_query).await?;
    dml_query.wait().await?;

    assert_eq!(dml_query.num_dml_affected_rows(), 2);

    Ok(())
}

pub async fn dataset_admin(
    builder: bigquery_admin::builder::dataset_service::ClientBuilder,
) -> Result<()> {
    // Enable a basic subscriber. Useful to troubleshoot problems and visually
    // verify tracing is doing something.
    #[cfg(feature = "log-integration-tests")]
    let _guard = {
        use tracing_subscriber::fmt::format::FmtSpan;
        let subscriber = tracing_subscriber::fmt()
            .with_level(true)
            .with_thread_ids(true)
            .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
            .finish();

        tracing::subscriber::set_default(subscriber)
    };

    let project_id = crate::project_id()?;
    let client = builder.build().await?;
    cleanup_stale_datasets(&client, &project_id).await?;

    let dataset_id = create_test_dataset(&client, project_id.clone()).await?;
    println!("CREATED DATASET = {dataset_id:?}");

    let list = client
        .list_datasets()
        .set_project_id(&project_id)
        .send()
        .await?;
    println!("LIST DATASET = {} entries", list.datasets.len());

    assert!(list.datasets.iter().any(|v| v.id.contains(&dataset_id)));

    client
        .delete_dataset()
        .set_project_id(&project_id)
        .set_dataset_id(&dataset_id)
        .set_delete_contents(true)
        .send()
        .await?;
    println!("DELETE DATASET");

    Ok(())
}

async fn cleanup_stale_datasets(
    client: &bigquery_admin::client::DatasetService,
    project_id: &str,
) -> Result<()> {
    let stale_deadline = SystemTime::now().duration_since(UNIX_EPOCH)?;
    let stale_deadline = stale_deadline - Duration::from_secs(48 * 60 * 60);
    let stale_deadline = stale_deadline.as_millis() as i64;

    let list = client
        .list_datasets()
        .set_project_id(project_id)
        .set_filter("labels.integration-test:true")
        .send()
        .await?;
    let pending_all_datasets = list
        .datasets
        .into_iter()
        .filter_map(|v| {
            if let Some(dataset_id) = extract_dataset_id(project_id, v.id) {
                return Some(
                    client
                        .get_dataset()
                        .set_project_id(project_id)
                        .set_dataset_id(dataset_id)
                        .send(),
                );
            }
            None
        })
        .collect::<Vec<_>>();

    let stale_datasets = futures::future::join_all(pending_all_datasets)
        .await
        .into_iter()
        .filter_map(|r| {
            let dataset = r.unwrap();
            if let Some("true") = dataset.labels.get("integration-test").map(String::as_str) {
                if dataset.creation_time < stale_deadline {
                    return Some(dataset);
                }
            }
            None
        })
        .collect::<Vec<_>>();

    println!("found {} stale datasets", stale_datasets.len());

    let pending_deletion: Vec<_> = stale_datasets
        .into_iter()
        .filter_map(|ds| {
            if let Some(dataset_id) = extract_dataset_id(project_id, ds.id) {
                return Some(
                    client
                        .delete_dataset()
                        .set_project_id(project_id)
                        .set_dataset_id(dataset_id)
                        .set_delete_contents(true)
                        .send(),
                );
            }
            None
        })
        .collect();

    futures::future::join_all(pending_deletion).await;

    Ok(())
}

async fn create_test_dataset(
    client: &bigquery_admin::client::DatasetService,
    project_id: String,
) -> Result<String> {
    let dataset_id = random_dataset_id();

    println!("CREATING DATASET WITH ID: {dataset_id}");

    let create = client
        .insert_dataset()
        .set_project_id(project_id.as_str())
        .set_dataset(
            bigquery_admin::model::Dataset::new()
                .set_dataset_reference(
                    bigquery_admin::model::DatasetReference::new().set_dataset_id(&dataset_id),
                )
                .set_labels([("integration-test", "true")]),
        )
        .send()
        .await?;

    assert!(create.dataset_reference.is_some());

    Ok(dataset_id)
}

fn random_dataset_id() -> String {
    let rand_suffix: String = rand::rng()
        .sample_iter(&Alphanumeric)
        .take(8)
        .map(char::from)
        .collect();

    format!("rust_bq_test_dataset_{rand_suffix}")
}

fn extract_dataset_id(project_id: &str, id: String) -> Option<String> {
    id.strip_prefix(&format!("projects/{project_id}/datasets/"))
        .map(|v| v.to_string())
}
