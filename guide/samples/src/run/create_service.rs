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
    use google_cloud_run_v2::client::Services;
    use google_cloud_run_v2::model::{Container, RevisionTemplate, Service};

    let project_id = std::env::args().nth(1).unwrap();
    let location = std::env::args().nth(2).unwrap();
    let service_id = std::env::args().nth(3).unwrap();
    let client = Services::builder().build().await?;

    let mut service = Service::new();
    let mut revision_template = RevisionTemplate::new();
    let mut container = Container::new();
    container.image = "us-docker.pkg.dev/cloudrun/container/hello:latest".to_string();
    revision_template.containers.push(container);
    service.template = Some(revision_template);

    let response = client
        .create_service()
        .set_parent(format!("projects/{project_id}/locations/{location}"))
        .set_service(service)
        .set_service_id(service_id)
        .send()
        .await?;

    println!("{response:?}");

    Ok(())
}
