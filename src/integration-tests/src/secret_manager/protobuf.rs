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
use gax::paginator::{ItemPaginator, Paginator};
use rand::{Rng, distr::Alphanumeric};

pub async fn run(builder: sm::builder::secret_manager_service::ClientBuilder) -> Result<()> {
    let project_id = crate::project_id()?;
    let secret_id: String = rand::rng()
        .sample_iter(&Alphanumeric)
        .take(crate::SECRET_ID_LENGTH)
        .map(char::from)
        .collect();

    let client = builder.build().await?;
    cleanup_stale_secrets(&client, &project_id, &secret_id).await?;

    println!("\nTesting create_secret()");
    use gax::options::RequestOptionsBuilder;
    let create = client
        .create_secret()
        .set_parent(format!("projects/{project_id}"))
        .with_user_agent("test/1.2.3")
        .set_secret_id(&secret_id)
        .set_secret(
            sm::model::Secret::new()
                .set_replication(
                    sm::model::Replication::new()
                        .set_automatic(sm::model::replication::Automatic::new()),
                )
                .set_labels([("integration-test", "true")]),
        )
        .send()
        .await?;
    println!("CREATE = {create:?}");

    let project_name = create
        .name
        .strip_suffix(format!("/secrets/{secret_id}").as_str());
    assert!(project_name.is_some());

    println!("\nTesting get_secret()");
    let get = client.get_secret().set_name(&create.name).send().await?;
    println!("GET = {get:?}");
    assert_eq!(get, create);

    // We need to verify that FieldMask as query parameters are sent correctly
    // by the client library. This involves:
    // - Setting the mask does not result in a RPC error
    // - The mask has the desired effect, only the fields in the mask are set
    // This test assumes the service works correctly, we are not trying to write
    // service tests.
    println!("\nTesting update_secret() [1]");
    let tag = |mut map: std::collections::HashMap<String, String>, msg: &str| {
        map.insert("updated".to_string(), msg.to_string());
        map
    };
    use gax::retry_policy::RetryPolicyExt;
    let update = client
        .update_secret()
        .set_secret(
            sm::model::Secret::new()
                .set_name(&get.name)
                .set_etag(get.etag)
                .set_labels(tag(get.labels.clone(), "test-1"))
                .set_annotations(tag(get.annotations.clone(), "test-1")),
        )
        .set_update_mask(wkt::FieldMask::default().set_paths(["annotations", "labels"]))
        // Avoid flakes, safe to retry because of the etag.
        .with_retry_policy(gax::retry_policy::AlwaysRetry.with_attempt_limit(3))
        .send()
        .await?;
    println!("UPDATE = {update:?}");
    assert_eq!(
        update.labels.get("updated").map(String::as_str),
        Some("test-1")
    );
    assert_eq!(
        update.annotations.get("updated").map(String::as_str),
        Some("test-1")
    );

    println!("\nTesting update_secret() [2]");
    let update = client
        .update_secret()
        .set_secret(
            sm::model::Secret::new()
                .set_name(&get.name)
                .set_etag(update.etag.clone())
                .set_labels(tag(get.labels.clone(), "test-2"))
                .set_annotations(tag(get.annotations.clone(), "test-2")),
        )
        .set_update_mask(wkt::FieldMask::default().set_paths(["annotations"]))
        // Avoid flakes, safe to retry because of the etag.
        .with_retry_policy(gax::retry_policy::AlwaysRetry.with_attempt_limit(3))
        .send()
        .await?;
    println!("UPDATE = {update:?}");
    // Should not change, it is not in the field mask
    assert_eq!(
        update.labels.get("updated").map(String::as_str),
        Some("test-1")
    );
    assert_eq!(
        update.annotations.get("updated").map(String::as_str),
        Some("test-2")
    );

    println!("\nTesting list_secrets()");
    let list = get_all_secret_names(&client, &project_id).await?;
    assert!(
        list.iter().any(|name| name == &get.name),
        "missing secret {} in {list:?}",
        &get.name
    );

    run_secret_versions(&client, &create.name).await?;
    run_many_secret_versions(&client, &create.name).await?;
    run_iam(&client, &create.name).await?;
    run_locations(&client, &project_id).await?;

    println!("\nTesting delete_secret()");
    client.delete_secret().set_name(get.name).send().await?;
    println!("DELETE finished");

    Ok(())
}

async fn run_locations(client: &sm::client::SecretManagerService, project_id: &str) -> Result<()> {
    println!("\nTesting list_locations()");
    let locations = client
        .list_locations()
        .set_name(format!("projects/{project_id}"))
        .send()
        .await?;
    println!("LOCATIONS = {locations:?}");

    assert!(
        !locations.locations.is_empty(),
        "got empty locations field for {locations:?}"
    );
    let first = locations.locations[0].clone();
    assert!(
        !first.location_id.is_empty(),
        "expected some location field to be set"
    );

    println!("\nTesting get_location()");
    let get = client
        .get_location()
        .set_name(format!(
            "projects/{project_id}/locations/{}",
            first.location_id
        ))
        .send()
        .await?;
    println!("GET = {get:?}");

    assert_eq!(get, first);

    Ok(())
}

async fn run_iam(client: &sm::client::SecretManagerService, secret_name: &str) -> Result<()> {
    let service_account = crate::test_service_account()?;

    println!("\nTesting get_iam_policy()");
    let policy = client
        .get_iam_policy()
        .set_resource(secret_name)
        .send()
        .await?;
    println!("POLICY = {policy:?}");

    println!("\nTesting test_iam_permissions()");
    let response = client
        .test_iam_permissions()
        .set_resource(secret_name)
        .set_permissions(["secretmanager.versions.access"])
        .send()
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
            iam_v1::model::Binding::new()
                .set_role(ROLE)
                .set_members([format!("serviceAccount:{service_account}")]),
        );
    }
    let response = client
        .set_iam_policy()
        .set_resource(secret_name)
        .set_update_mask(wkt::FieldMask::default().set_paths(["bindings"]))
        .set_policy(new_policy)
        .send()
        .await?;
    println!("RESPONSE = {response:?}");

    Ok(())
}

async fn run_secret_versions(
    client: &sm::client::SecretManagerService,
    secret_name: &str,
) -> Result<()> {
    println!("\nTesting create_secret_version()");
    let data = "The quick brown fox jumps over the lazy dog".as_bytes();
    let checksum = crc32c::crc32c(data);
    let create_secret_version = client
        .add_secret_version()
        .set_parent(secret_name)
        .set_payload(
            sm::model::SecretPayload::new()
                .set_data(bytes::Bytes::from(data))
                .set_data_crc32c(checksum as i64),
        )
        .send()
        .await?;
    println!("CREATE_SECRET_VERSION = {create_secret_version:?}");

    println!("\nTesting get_secret_version()");
    let get_secret_version = client
        .get_secret_version()
        .set_name(&create_secret_version.name)
        .send()
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
        .access_secret_version()
        .set_name(&create_secret_version.name)
        .send()
        .await?;
    println!("ACCESS_SECRET_VERSION = {access_secret_version:?}");
    assert_eq!(
        access_secret_version.payload.map(|p| p.data),
        Some(bytes::Bytes::from(data))
    );

    println!("\nTesting disable_secret_version()");
    let disable = client
        .disable_secret_version()
        .set_name(&create_secret_version.name)
        .send()
        .await?;
    println!("DISABLE_SECRET_VERSION = {disable:?}");

    println!("\nTesting enable_secret_version()");
    let enable = client
        .enable_secret_version()
        .set_name(&create_secret_version.name)
        .send()
        .await?;
    println!("ENABLE_SECRET_VERSION = {enable:?}");

    println!("\nTesting destroy_secret_version()");
    let delete = client
        .destroy_secret_version()
        .set_name(&get_secret_version.name)
        .send()
        .await?;
    println!("RESPONSE = {delete:?}");

    Ok(())
}

async fn run_many_secret_versions(
    client: &sm::client::SecretManagerService,
    secret_name: &str,
) -> Result<()> {
    use std::collections::BTreeSet;

    println!("\nTesting list_secret_versions() with multiple pages");
    let mut want = BTreeSet::new();
    for i in 0..5 {
        println!("\nTesting create_secret_version() with i = {i}");
        let data = "The quick brown fox jumps over the lazy dog".as_bytes();
        let checksum = crc32c::crc32c(data);
        let create_secret_version = client
            .add_secret_version()
            .set_parent(secret_name)
            .set_payload(
                sm::model::SecretPayload::new()
                    .set_data(bytes::Bytes::from(data))
                    .set_data_crc32c(checksum as i64),
            )
            .send()
            .await?;
        want.insert(create_secret_version.name);
    }
    let want = want;

    const PAGE_SIZE: i32 = 2;
    let mut paginator = client
        .list_secret_versions()
        .set_parent(secret_name)
        .set_page_size(PAGE_SIZE)
        .by_page();
    let mut got = BTreeSet::new();
    while let Some(page) = paginator.next().await {
        let page = page?;
        assert!(page.versions.len() <= PAGE_SIZE as usize, "{page:?}");
        page.versions.into_iter().for_each(|v| {
            got.insert(v.name);
        });
    }
    let got = got;

    assert!(want.is_subset(&got), "want={want:?}, got={got:?}");

    let pending: Vec<_> = want
        .iter()
        .map(|name| client.destroy_secret_version().set_name(name).send())
        .collect();
    // Print the errors, but otherwise ignore them.
    futures::future::join_all(pending)
        .await
        .into_iter()
        .zip(want)
        .for_each(|(r, name)| println!("    {name} = {r:?}"));

    Ok(())
}

async fn get_all_secret_version_names(
    client: &sm::client::SecretManagerService,
    secret_name: &str,
) -> Result<Vec<String>> {
    let mut names = Vec::new();
    let mut page_token = String::new();
    loop {
        let response = client
            .list_secret_versions()
            .set_parent(secret_name)
            .set_page_token(&page_token)
            .send()
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
    client: &sm::client::SecretManagerService,
    project_id: &str,
) -> Result<Vec<String>> {
    let mut names = Vec::new();
    let mut paginator = client
        .list_secrets()
        .set_parent(format!("projects/{project_id}"))
        .by_item();
    while let Some(response) = paginator.next().await {
        let item = response?;
        names.push(item.name);
    }
    Ok(names)
}

async fn cleanup_stale_secrets(
    client: &sm::client::SecretManagerService,
    project_id: &str,
    secret_id: &str,
) -> Result<()> {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    let stale_deadline = SystemTime::now().duration_since(UNIX_EPOCH)?;
    let stale_deadline = stale_deadline - Duration::from_secs(48 * 60 * 60);
    let stale_deadline = wkt::Timestamp::clamp(stale_deadline.as_secs() as i64, 0);

    let mut stale_secrets = Vec::new();
    let mut paginator = client
        .list_secrets()
        .set_parent(format!("projects/{project_id}"))
        .by_item();
    while let Some(secret) = paginator.next().await {
        let secret = secret?;
        if secret
            .name
            .ends_with(format!("/secrets/{secret_id}").as_str())
        {
            return Err(anyhow::Error::msg(
                "randomly generated secret id already exists {secret_id}",
            ));
        }

        if secret
            .labels
            .get("integration-test")
            .is_some_and(|v| v == "true")
            && secret.create_time.is_some_and(|v| v < stale_deadline)
        {
            stale_secrets.push(secret.name);
        }
    }

    let pending = stale_secrets
        .iter()
        .map(|v| client.delete_secret().set_name(v).send())
        .collect::<Vec<_>>();

    // Print the errors, but otherwise ignore them.
    futures::future::join_all(pending)
        .await
        .into_iter()
        .zip(stale_secrets)
        .for_each(|(r, name)| println!("{name} = {r:?}"));

    Ok(())
}
