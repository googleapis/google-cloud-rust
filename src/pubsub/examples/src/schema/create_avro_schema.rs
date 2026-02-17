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

// [START pubsub_create_avro_schema]
use google_cloud_pubsub::client::SchemaService;
use google_cloud_pubsub::model::{Schema, schema::Type};

pub async fn sample(client: &SchemaService, project: &str, schema_id: &str) -> anyhow::Result<()> {
    const AVRO_SCHEMA: &str =
        r#"{"type": "record", "name": "User", "fields": [{"name": "name", "type": "string"}]}"#;
    let schema = client
        .create_schema()
        .set_parent(format!("projects/{project}"))
        .set_schema_id(schema_id)
        .set_schema(
            Schema::new()
                .set_type(Type::Avro)
                .set_definition(AVRO_SCHEMA),
        )
        .send()
        .await?;

    println!("successfully created schema {schema:?}");
    Ok(())
}
// [END pubsub_create_avro_schema]
