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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use google_cloud_secretmanager_v1::client::SecretManagerService;

    let project_id = std::env::args().nth(1).unwrap();
    let secret_id = std::env::args().nth(2).unwrap();
    let client = SecretManagerService::builder().build().await?;

    let response = client
        .get_secret()
        .set_name(format!("projects/{project_id}/secrets/{secret_id}"))
        .send()
        .await?;

    println!("{response:?}");

    Ok(())
}
