// Copyright 2024 Google LLC
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

use integration_tests::Result;
use rand::{distributions::Alphanumeric, Rng};

pub async fn run() -> Result<()> {
    let project_id = integration_tests::project_id()?;
    let secret_id: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(integration_tests::SECRET_ID_LENGTH)
        .map(char::from)
        .collect();

    let client = smo::Client::new()
        .await?
        .google_cloud_secretmanager_v_1_secret_manager_service();

    let create_response = client
        .create_secret(
            smo::model::CreateSecretRequest::default()
                .set_project(&project_id)
                .set_secret_id(&secret_id)
                .set_request_body(
                    smo::model::Secret::default()
                        .set_replication(
                            smo::model::Replication::default()
                                .set_automatic(smo::model::Automatic::default()),
                        )
                        .set_labels(
                            [("integration-test", "true")]
                                .map(|(k, v)| (k.to_string(), v.to_string())),
                        ),
                ),
        )
        .await?;
    println!("CREATE = {create_response:?}");

    let project_name = create_response
        .name
        .as_ref()
        .and_then(|s| s.strip_suffix(format!("/secrets/{secret_id}").as_str()));
    assert!(project_name.is_some());

    let get_response = client
        .get_secret(
            smo::model::GetSecretRequest::default()
                .set_project(&project_id)
                .set_secret(&secret_id),
        )
        .await?;
    println!("GET = {get_response:?}");
    assert_eq!(get_response, create_response);

    let response = client
        .delete_secret(
            smo::model::DeleteSecretRequest::default()
                .set_project(&project_id)
                .set_secret(&secret_id),
        )
        .await?;
    println!("DELETE = {response:?}");
    Ok(())
}
