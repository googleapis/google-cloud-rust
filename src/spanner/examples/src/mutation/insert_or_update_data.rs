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

// [START spanner_insert_or_update_data]
use google_cloud_spanner::client::DatabaseClient;
use google_cloud_spanner::mutation::Mutation;

pub async fn sample(client: &DatabaseClient) -> anyhow::Result<()> {
    let mutations = vec![
        Mutation::new_insert_or_update_builder("Singers")
            .set("SingerId")
            .to(1)
            .set("FirstName")
            .to("Marc")
            .set("LastName")
            .to("Richards")
            .build(),
        Mutation::new_insert_or_update_builder("Singers")
            .set("SingerId")
            .to(2)
            .set("FirstName")
            .to("Catalina")
            .set("LastName")
            .to("Smith")
            .build(),
        Mutation::new_insert_or_update_builder("Singers")
            .set("SingerId")
            .to(3)
            .set("FirstName")
            .to("Alice")
            .set("LastName")
            .to("Trentor")
            .build(),
        Mutation::new_insert_or_update_builder("Singers")
            .set("SingerId")
            .to(4)
            .set("FirstName")
            .to("Lea")
            .set("LastName")
            .to("Martin")
            .build(),
        Mutation::new_insert_or_update_builder("Singers")
            .set("SingerId")
            .to(5)
            .set("FirstName")
            .to("David")
            .set("LastName")
            .to("Lomond")
            .build(),
    ];

    println!("Upserting records into Singers using mutations...");
    let write_transaction = client.write_only_transaction().build();
    write_transaction.write(mutations).await?;
    println!("Upserted records successfully.");

    Ok(())
}
// [END spanner_insert_or_update_data]
