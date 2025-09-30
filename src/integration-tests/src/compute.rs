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
use compute::client::{Images, MachineTypes, Zones};
use gax::paginator::ItemPaginator as _;

pub async fn zones() -> Result<()> {
    let client = Zones::builder().with_tracing().build().await?;
    let project_id = crate::project_id()?;

    tracing::info!("Testing Zones::list()");
    let mut items = client.list().set_project(&project_id).by_item();
    while let Some(item) = items.next().await.transpose()? {
        tracing::info!("item = {item:?}");
    }
    tracing::info!("DONE with Zones::list()");

    tracing::info!("Testing Zones::get()");
    // us-central1-a is well-known, and if it goes away fixing this test is the
    // least of our problems.
    let response = client
        .get()
        .set_project(&project_id)
        .set_zone("us-central1-a")
        .send()
        .await?;
    assert_eq!(
        response.status,
        Some(compute::model::zone::Status::Up),
        "response={response:?}"
    );
    tracing::info!("Zones::get() = {response:?}");

    Ok(())
}

pub async fn machine_types() -> Result<()> {
    let client = MachineTypes::builder().with_tracing().build().await?;
    let project_id = crate::project_id()?;

    tracing::info!("Testing MachineTypes::list()");
    let mut items = client
        .list()
        .set_project(&project_id)
        .set_zone("us-central1-a")
        .by_item();
    while let Some(item) = items.next().await.transpose()? {
        tracing::info!("item = {item:?}");
    }
    tracing::info!("DONE with MachineTypes::list()");

    tracing::info!("Testing MachineTypes::aggregated_list()");
    let mut token = String::new();
    loop {
        let response = client
            .aggregated_list()
            .set_project(&project_id)
            .set_page_token(&token)
            .send()
            .await?;
        response
            .items
            .iter()
            .filter(|(_k, v)| !v.machine_types.is_empty())
            .for_each(|(k, v)| {
                tracing::info!("item[{k}] has {} machine types", v.machine_types.len());
            });
        tracing::info!("MachineTypes::aggregated_list = {response:?}");
        token = response.next_page_token.unwrap_or_default();
        if token.is_empty() {
            break;
        }
    }
    tracing::info!("DONE with MachineTypes::aggregated_list()");

    tracing::info!("Testing MachineTypes::get()");
    // us-central1-a is well-known, and if it goes away fixing this test is the
    // least of our problems.
    let response = client
        .get()
        .set_project(&project_id)
        .set_zone("us-central1-a")
        .set_machine_type("f1-micro")
        .send()
        .await?;
    assert_eq!(response.is_shared_cpu, Some(true), "response={response:?}");
    tracing::info!("MachineTypes::get() = {response:?}");

    Ok(())
}

pub async fn images() -> Result<()> {
    use compute::model::image::Architecture;

    let client = Images::builder().with_tracing().build().await?;

    tracing::info!("Testing Images::list()");
    let mut latest = None;
    let mut items = client.list().set_project("cos-cloud").by_item();
    while let Some(item) = items.next().await.transpose()? {
        tracing::info!("item = {item:?}");
        if item.architecture != Some(Architecture::X8664)
            || item
                .family
                .as_ref()
                .is_some_and(|v| v.strip_prefix("cos-").is_some())
        {
            continue;
        }
        latest = match &latest {
            None => Some(item),
            Some(i) if item.family > i.family => Some(item),
            Some(i)
                if item.family == i.family && item.creation_timestamp > i.creation_timestamp =>
            {
                Some(item)
            }
            _ => latest,
        };
    }
    tracing::info!("DONE with Images::list()");
    tracing::info!("LATEST cos-cloud image is {latest:?}");

    Ok(())
}
