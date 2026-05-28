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

// [START spanner_insert_data]
use google_cloud_spanner::client::{DatabaseClient, Mutation};

pub async fn sample(client: &DatabaseClient) -> anyhow::Result<()> {
    let mutations = vec![
        Mutation::new_insert_builder("Singers")
            .set("SingerId")
            .to(&1)
            .set("FirstName")
            .to(&"Marc")
            .set("LastName")
            .to(&"Richards")
            .build(),
        Mutation::new_insert_builder("Singers")
            .set("SingerId")
            .to(&2)
            .set("FirstName")
            .to(&"Catalina")
            .set("LastName")
            .to(&"Smith")
            .build(),
        Mutation::new_insert_builder("Singers")
            .set("SingerId")
            .to(&3)
            .set("FirstName")
            .to(&"Alice")
            .set("LastName")
            .to(&"Trentor")
            .build(),
        Mutation::new_insert_builder("Singers")
            .set("SingerId")
            .to(&4)
            .set("FirstName")
            .to(&"Lea")
            .set("LastName")
            .to(&"Martin")
            .build(),
        Mutation::new_insert_builder("Singers")
            .set("SingerId")
            .to(&5)
            .set("FirstName")
            .to(&"David")
            .set("LastName")
            .to(&"Lomond")
            .build(),
        Mutation::new_insert_builder("Albums")
            .set("SingerId")
            .to(&1)
            .set("AlbumId")
            .to(&1)
            .set("AlbumTitle")
            .to(&"Total Junk")
            .build(),
        Mutation::new_insert_builder("Albums")
            .set("SingerId")
            .to(&1)
            .set("AlbumId")
            .to(&2)
            .set("AlbumTitle")
            .to(&"Go, Go, Go")
            .build(),
        Mutation::new_insert_builder("Albums")
            .set("SingerId")
            .to(&2)
            .set("AlbumId")
            .to(&1)
            .set("AlbumTitle")
            .to(&"Green")
            .build(),
        Mutation::new_insert_builder("Albums")
            .set("SingerId")
            .to(&2)
            .set("AlbumId")
            .to(&2)
            .set("AlbumTitle")
            .to(&"Forever Hold Your Peace")
            .build(),
        Mutation::new_insert_builder("Albums")
            .set("SingerId")
            .to(&2)
            .set("AlbumId")
            .to(&3)
            .set("AlbumTitle")
            .to(&"Terrified")
            .build(),
    ];

    println!("Inserting initial data into Singers & Albums...");
    let write_transaction = client.write_only_transaction().build();
    write_transaction.write(mutations).await?;
    println!("Inserted data successfully.");

    Ok(())
}
// [END spanner_insert_data]
