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

// [START compute_instances_list] ANCHOR: all
pub async fn quickstart(project_id: &str) -> anyhow::Result<()> {
    use google_cloud_compute_v1::client::Instances;
    use google_cloud_gax::paginator::ItemPaginator;

    let client = Instances::builder().build().await?;
    println!("Listing instances for project {project_id}");
    let mut instances = client.list().set_project(project_id).by_item();
    while let Some(item) = instances.next().await.transpose()? {
        println!("  {:?}", item.name);
    }
    println!("DONE");
    Ok(())
}
// [END compute_instances_list] ANCHOR_END: all
