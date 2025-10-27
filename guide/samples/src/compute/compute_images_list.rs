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

// [START compute_images_list]
pub use google_cloud_compute_v1::client::Images;
pub use google_cloud_gax::paginator::ItemPaginator;

pub async fn sample(client: &Images, project_id: &str) -> anyhow::Result<()> {
    let mut items = client
        .list()
        .set_project(project_id)
        // Maximum number of results per page. Higher values reduce the number
        // of RPCs, but increase memory usage.
        .set_max_results(100_u32)
        // Skip deprecated images.
        .set_filter("deprecated.state != DEPRECATED")
        .by_item();
    println!("iterating by item:");
    while let Some(image) = items.next().await.transpose()? {
        println!("item = {:?}", image);
    }
    Ok(())
}
// [END compute_images_list]
