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

use anyhow::Result;
use bigquery_samples::{
    INSTANCE_LABEL, cleanup_stale_datasets, create_dataset, delete_dataset, random_dataset_id,
};
use futures::stream::TryStreamExt;
use google_cloud_bigquery_v2::client::DatasetService;
use google_cloud_gax::paginator::ItemPaginator;
use google_cloud_test_utils::runtime_config::project_id;

pub async fn dataset_admin() -> Result<()> {
    let project_id = project_id()?;
    let client = DatasetService::builder().with_tracing().build().await?;
    cleanup_stale_datasets(&client, &project_id).await?;

    let dataset_id = random_dataset_id();

    let create = create_dataset(&client, &project_id, &dataset_id).await?;
    println!("CREATE DATASET = {create:?}");
    assert!(create.dataset_reference.is_some(), "{create:?}");

    let list = client
        .list_datasets()
        .set_project_id(&project_id)
        .set_filter(format!("labels.{INSTANCE_LABEL}"))
        .by_item()
        .into_stream();
    let items: Vec<_> = list.try_collect().await?;
    println!("LIST DATASET = {} entries", items.len());

    assert!(items.iter().any(|v| v.id.contains(&dataset_id)));

    delete_dataset(&client, &project_id, &dataset_id).await?;
    println!("DELETE DATASET");

    Ok(())
}
