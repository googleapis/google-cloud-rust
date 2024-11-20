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

use crate::Result;
use rand::{distributions::Alphanumeric, Rng};

pub async fn run() -> Result<()> {
    let project_id = crate::project_id()?;
    let secret_id: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(crate::SECRET_ID_LENGTH)
        .map(char::from)
        .collect();

    let client = sm::Client::new().await?.secret_manager_service();

    cleanup_stale_secrets(&client, &project_id, &secret_id).await?;

    println!("\nTesting create_secret()");
    let create = client
        .create_secret(
            sm::model::CreateSecretRequest::default()
                .set_parent(format!("projects/{project_id}"))
                .set_secret_id(&secret_id)
                .set_secret(
                    sm::model::Secret::default()
                        .set_replication(sm::model::Replication::default().set_replication(
                            sm::model::replication::Replication::Automatic(
                                sm::model::replication::Automatic::default(),
                            ),
                        ))
                        .set_labels(
                            [("integration-test", "true")]
                                .map(|(k, v)| (k.to_string(), v.to_string())),
                        ),
                ),
        )
        .await?;
    println!("CREATE = {create:?}");

    let project_name = create
        .name
        .strip_suffix(format!("/secrets/{secret_id}").as_str());
    assert!(project_name.is_some());

    println!("\nTesting get_secret()");
    let get = client
        .get_secret(sm::model::GetSecretRequest::default().set_name(&create.name))
        .await?;
    println!("GET = {get:?}");
    assert_eq!(get, create);

    println!("\nTesting update_secret()");
    let mut new_labels = get.labels.clone();
    new_labels.insert("updated".to_string(), "true".to_string());
    let update = client
        .update_secret(
            sm::model::UpdateSecretRequest::default()
                .set_update_mask(
                    wkt::FieldMask::default().set_paths(["labels"].map(str::to_string).to_vec()),
                )
                .set_secret(
                    sm::model::Secret::default()
                        .set_name(&get.name)
                        .set_labels(new_labels),
                ),
        )
        .await?;
    println!("UPDATE = {update:?}");

    println!("\nTesting list_secrets()");
    let list = get_all_secret_names(&client, &project_id).await?;
    assert!(
        list.iter().any(|name| name == &get.name),
        "missing secret {} in {list:?}",
        &get.name
    );

    run_secret_versions(&client, &create.name).await?;
    run_iam(&client, &create.name).await?;

    println!("\nTesting delete_secret()");
    let delete = client
        .delete_secret(sm::model::DeleteSecretRequest::default().set_name(get.name))
        .await?;
    println!("DELETE = {delete:?}");

    Ok(())
}

async fn run_iam(client: &sm::SecretManagerService, secret_name: &str) -> Result<()> {
    let service_account = crate::service_account_for_iam_tests()?;

    println!("\nTesting get_iam_policy()");
    let policy = client
        .get_iam_policy(iam_v1::model::GetIamPolicyRequest::default().set_resource(secret_name))
        .await?;
    println!("POLICY = {policy:?}");

    println!("\nTesting test_iam_permissions()");
    let response = client
        .test_iam_permissions(
            iam_v1::model::TestIamPermissionsRequest::default()
                .set_resource(secret_name)
                .set_permissions(
                    ["secretmanager.versions.access"]
                        .map(str::to_string)
                        .to_vec(),
                ),
        )
        .await?;
    println!("RESPONSE = {response:?}");

    // This really could use an OCC loop.
    println!("\nTesting set_iam_policy()");
    let mut new_policy = policy.clone();
    const ROLE: &str = "roles/secretmanager.secretVersionAdder";
    let mut found = false;
    for binding in &mut new_policy.bindings {
        if binding.role != ROLE {
            continue;
        }
        found = true;
        binding
            .members
            .push(format!("serviceAccount:{service_account}"));
    }
    if !found {
        new_policy.bindings.push(
            iam_v1::model::Binding::default()
                .set_role(ROLE)
                .set_members([format!("serviceAccount:{service_account}")].to_vec()),
        );
    }
    let response = client
        .set_iam_policy(
            iam_v1::model::SetIamPolicyRequest::default()
                .set_resource(secret_name)
                .set_update_mask(
                    wkt::FieldMask::default().set_paths(["bindings"].map(str::to_string).to_vec()),
                )
                .set_policy(new_policy),
        )
        .await?;
    println!("RESPONSE = {response:?}");

    Ok(())
}

async fn run_secret_versions(client: &sm::SecretManagerService, secret_name: &str) -> Result<()> {
    println!("\nTesting create_secret_version()");
    let data = "The quick brown fox jumps over the lazy dog".as_bytes();
    let checksum = crc32c::crc32c(data);
    let create_secret_version = client
        .add_secret_version(
            sm::model::AddSecretVersionRequest::default()
                .set_parent(secret_name)
                .set_payload(
                    sm::model::SecretPayload::default()
                        .set_data(bytes::Bytes::from(data))
                        .set_data_crc32c(checksum as i64),
                ),
        )
        .await?;
    println!("CREATE_SECRET_VERSION = {create_secret_version:?}");

    println!("\nTesting get_secret_version()");
    let get_secret_version = client
        .get_secret_version(
            sm::model::GetSecretVersionRequest::default().set_name(&create_secret_version.name),
        )
        .await?;
    println!("GET_SECRET_VERSION = {create_secret_version:?}");
    assert_eq!(get_secret_version, create_secret_version);

    println!("\nTesting list_secret_versions()");
    let secret_versions_list = get_all_secret_version_names(client, secret_name).await?;
    assert!(
        secret_versions_list
            .iter()
            .any(|name| name == &get_secret_version.name),
        "missing secret version {} in {secret_versions_list:?}",
        &get_secret_version.name
    );

    println!("\nTesting access_secret_version()");
    let access_secret_version = client
        .access_secret_version(
            sm::model::AccessSecretVersionRequest::default().set_name(&create_secret_version.name),
        )
        .await?;
    println!("ACCESS_SECRET_VERSION = {access_secret_version:?}");
    assert_eq!(
        access_secret_version.payload.map(|p| p.data),
        Some(bytes::Bytes::from(data))
    );

    println!("\nTesting disable_secret_version()");
    let disable = client
        .disable_secret_version(
            sm::model::DisableSecretVersionRequest::default().set_name(&create_secret_version.name),
        )
        .await?;
    println!("DISABLE_SECRET_VERSION = {disable:?}");

    println!("\nTesting disable_secret_version()");
    let enable = client
        .enable_secret_version(
            sm::model::EnableSecretVersionRequest::default().set_name(&create_secret_version.name),
        )
        .await?;
    println!("ENABLE_SECRET_VERSION = {enable:?}");

    println!("\nTesting destroy_secret_version()");
    let delete = client
        .destroy_secret_version(
            sm::model::DestroySecretVersionRequest::default().set_name(&get_secret_version.name),
        )
        .await?;
    println!("RESPONSE = {delete:?}");

    Ok(())
}

async fn get_all_secret_version_names(
    client: &sm::SecretManagerService,
    secret_name: &str,
) -> Result<Vec<String>> {
    let mut names = Vec::new();
    let mut page_token = String::new();
    loop {
        let response = client
            .list_secret_versions(
                sm::model::ListSecretVersionsRequest::default()
                    .set_parent(secret_name)
                    .set_page_token(&page_token),
            )
            .await?;
        response
            .versions
            .into_iter()
            .for_each(|s| names.push(s.name));
        if response.next_page_token.is_empty() {
            break;
        }
        page_token = response.next_page_token;
    }
    Ok(names)
}

async fn get_all_secret_names(
    client: &sm::SecretManagerService,
    project_id: &str,
) -> Result<Vec<String>> {
    let mut names = Vec::new();
    let mut page_token = String::new();
    loop {
        let response = client
            .list_secrets(
                sm::model::ListSecretsRequest::default()
                    .set_parent(format!("projects/{project_id}"))
                    .set_page_token(&page_token),
            )
            .await?;
        response
            .secrets
            .into_iter()
            .for_each(|s| names.push(s.name));
        if response.next_page_token.is_empty() {
            break;
        }
        page_token = response.next_page_token;
    }
    Ok(names)
}

async fn cleanup_stale_secrets(
    client: &sm::SecretManagerService,
    project_id: &str,
    secret_id: &str,
) -> Result<()> {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    let stale_deadline = SystemTime::now().duration_since(UNIX_EPOCH)?;
    let stale_deadline = stale_deadline - Duration::from_secs(48 * 60 * 60);
    let stale_deadline = stale_deadline.as_secs() as i64;

    let mut stale_secrets = Vec::new();
    let mut list_request =
        sm::model::ListSecretsRequest::default().set_parent(format!("projects/{project_id}"));
    loop {
        let response = client.list_secrets(list_request.clone()).await?;
        for secret in response.secrets {
            if secret
                .name
                .ends_with(format!("/secrets/{secret_id}").as_str())
            {
                return Err("randomly generated secret id already exists {secret_id}".into());
            }

            if let Some("true") = secret.labels.get("integration-test").map(String::as_str) {
                if let Some(true) = secret.create_time.map(|v| v.seconds < stale_deadline) {
                    stale_secrets.push(secret.name);
                }
            }
        }
        if response.next_page_token.is_empty() {
            break;
        }
        list_request.page_token = response.next_page_token;
    }

    let pending = stale_secrets
        .iter()
        .map(|v| client.delete_secret(sm::model::DeleteSecretRequest::default().set_name(v)))
        .collect::<Vec<_>>();

    // Print the errors, but otherwise ignore them.
    futures::future::join_all(pending)
        .await
        .into_iter()
        .zip(stale_secrets)
        .for_each(|(r, name)| println!("{name} = {r:?}"));

    Ok(())
}
