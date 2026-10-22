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

mod instance;
mod table;

use anyhow::Result;
use google_cloud_bigtable_admin_v2::client::{BigtableInstanceAdmin, BigtableTableAdmin};
use google_cloud_test_utils::runtime_config::{project_id, zone_id};

pub async fn table_admin() -> Result<()> {
    let project = project_id()?;
    let zone = zone_id();

    let instance_admin = BigtableInstanceAdmin::builder()
        .with_tracing()
        .build()
        .await?;
    let table_admin = BigtableTableAdmin::builder().with_tracing().build().await?;

    if let Err(e) = instance::cleanup_stale_instances(&instance_admin, &project).await {
        tracing::warn!("error cleaning up stale instances: {e:?}");
    }

    let instance = instance::create_test_instance(&instance_admin, &project, &zone).await?;
    let result = table::check_consistency(&table_admin, &instance.name).await;
    instance::delete_instance(&instance_admin, &instance.name).await;

    result
}
