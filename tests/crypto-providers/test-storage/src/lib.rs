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

use google_cloud_storage::client::{Storage, StorageControl};
use google_cloud_storage::model_ext::ReadRange;
use google_cloud_gax::paginator::ItemPaginator as _;

/// Use a public dataset to verify read operations work.
const LANDSAT_DATASET: &str = "projects/_/buckets/gcp-public-data-landsat";
const LANDSAT_INDEX: &str = "index.csv.gz";

/// Verify the google_cloud_storage crate is minimally functional.
///
/// The caller may need to configure the default crypto provider.
pub async fn run() -> anyhow::Result<()> {
    let project_id = std::env::var("GOOGLE_CLOUD_PROJECT")?;
    // Verify gRPC requests work as expected:
    let client = StorageControl::builder().build().await?;
    let mut buckets = client
        .list_buckets()
        .set_parent(format!("projects/{project_id}"))
        .by_item();
    let _ = buckets.next().await.expect("expected at least one bucket")?;

    // Verify JSON requests work as expected:
    let storage = Storage::builder().build().await?;
    let mut reader = storage
        .read_object(LANDSAT_DATASET, LANDSAT_INDEX)
        .set_read_range(ReadRange::head(128))
        .send()
        .await?;
    let mut contents = Vec::new();
    while let Some(chunk) = reader.next().await.transpose()? {
        contents.extend_from_slice(&chunk);
    }
    assert_eq!(contents.len(), 128);
    Ok(())
}
