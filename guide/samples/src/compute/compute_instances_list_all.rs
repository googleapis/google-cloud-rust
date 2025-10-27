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

// [START compute_instances_list_all]
use google_cloud_compute_v1::client::Instances;
use google_cloud_gax::paginator::ItemPaginator;

pub async fn sample(client: &Instances, project_id: &str) -> anyhow::Result<()> {
    let mut items = client.aggregated_list().set_project(project_id).by_item();
    while let Some((zone, scoped_list)) = items.next().await.transpose()? {
        for instance in scoped_list.instances {
            println!(
                "Instance {} found in zone: {zone}",
                instance.name.expect("name should be Some()")
            );
        }
    }

    Ok(())
}
// [END compute_instances_list_all]
