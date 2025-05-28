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

use crate::Result;
use showcase::model::*;

pub async fn run() -> Result<()> {
    let client = showcase::client::Identity::builder()
        .with_endpoint("http://localhost:7469")
        .with_credentials(auth::credentials::testing::test_credentials())
        .with_retry_policy(gax::retry_policy::NeverRetry)
        .with_tracing()
        .build()
        .await?;

    let user = create_user(&client).await?;
    get_user(&client, &user).await?;
    update_user(&client, &user).await?;
    list_users(&client, &user).await?;
    delete_user(&client, &user).await?;

    Ok(())
}

async fn create_user(client: &showcase::client::Identity) -> Result<User> {
    let response = client
        .create_user()
        .set_user(
            User::new()
                .set_name("test-001")
                .set_display_name("Test 001")
                .set_email("test-001@example.com"),
        )
        .send()
        .await?;
    Ok(response)
}

async fn get_user(client: &showcase::client::Identity, user: &User) -> Result<()> {
    let response = client.get_user().set_name(&user.name).send().await?;
    assert_eq!(&response, user);
    Ok(())
}

async fn update_user(client: &showcase::client::Identity, user: &User) -> Result<()> {
    let response = client
        .update_user()
        .set_user(
            user.clone()
                .set_display_name("should not change")
                .set_or_clear_age(user.age.map(|x| x + 1)),
        )
        .set_update_mask(wkt::FieldMask::default().set_paths(["age"]))
        .send()
        .await?;
    assert_ne!(response.update_time, user.update_time);
    assert_ne!(response.age, user.age);
    assert_eq!(response.display_name, user.display_name);
    Ok(())
}

async fn list_users(client: &showcase::client::Identity, user: &User) -> Result<()> {
    use gax::paginator::ItemPaginator;
    let mut items = client.list_users().by_item();
    while let Some(u) = items.next().await {
        let u = u?;
        if user.name == u.name {
            return Ok(());
        }
    }
    Err(anyhow::Error::msg(format!(
        "missing user {user:?} in list results"
    )))
}

async fn delete_user(client: &showcase::client::Identity, user: &User) -> Result<()> {
    client.delete_user().set_name(&user.name).send().await?;
    Ok(())
}
