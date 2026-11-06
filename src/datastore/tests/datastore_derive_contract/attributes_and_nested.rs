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

//! Contract: `#[datastore(...)]` attributes (`rename_all`, `rename`, `exclude_from_indexes`, `key`, `default`, `flatten`).

use google_cloud_datastore::entity::{Entity, FromEntity, IntoEntity};
use google_cloud_datastore::key::Key;
use google_cloud_datastore::{FromEntity, FromValue, IntoEntity, IntoValue};

#[derive(Clone, Debug, Default, PartialEq, FromEntity, IntoEntity, FromValue, IntoValue)]
struct AuditMetadata {
    created_by: String,
    #[datastore(default)]
    revision: i64,
}

#[derive(Clone, Debug, PartialEq, FromEntity, IntoEntity)]
#[datastore(rename_all = "camelCase")]
struct UserProfile {
    #[datastore(key)]
    key: Option<Key>,
    display_name: String,
    #[datastore(rename = "email_address")]
    email: String,
    #[datastore(exclude_from_indexes)]
    raw_bio: String,
    #[datastore(default)]
    login_count: i64,
    #[datastore(flatten)]
    audit: AuditMetadata,
    embedded_audit: AuditMetadata,
}

#[test]
fn contract_and_behavior() -> anyhow::Result<()> {
    super::assert_from_entity::<UserProfile>();
    super::assert_into_entity::<UserProfile>();
    super::assert_from_value::<AuditMetadata>();
    super::assert_into_value::<AuditMetadata>();

    let key = Key::with_name("UserProfile", "u-123")?;
    let profile = UserProfile {
        key: Some(key.clone()),
        display_name: "Ada Lovelace".to_string(),
        email: "ada@example.com".to_string(),
        raw_bio: "long unindexed bio".to_string(),
        login_count: 7,
        audit: AuditMetadata {
            created_by: "system".to_string(),
            revision: 3,
        },
        embedded_audit: AuditMetadata {
            created_by: "admin".to_string(),
            revision: 1,
        },
    };

    let mut entity: Entity = profile.clone().into_entity();
    assert_eq!(entity.key(), Some(&key));
    assert_eq!(entity.get::<String>("displayName")?, "Ada Lovelace");
    assert_eq!(entity.get::<String>("email_address")?, "ada@example.com");
    assert!(
        entity
            .properties()
            .get("rawBio")
            .unwrap()
            .exclude_from_indexes()
    );
    assert_eq!(entity.get::<String>("created_by")?, "system");

    let decoded = UserProfile::from_entity(&entity)?;
    assert_eq!(decoded, profile);

    // Verify `#[datastore(default)]` when property is missing
    entity.remove("loginCount");
    let decoded_default = UserProfile::from_entity(&entity)?;
    assert_eq!(decoded_default.login_count, 0);
    Ok(())
}
