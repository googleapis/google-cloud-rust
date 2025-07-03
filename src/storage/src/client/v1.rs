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

use base64::Engine as _;

#[serde_with::serde_as]
#[derive(Debug, Default, serde::Deserialize, PartialEq, Clone)]
#[serde(default, rename_all = "camelCase")]
// See http://cloud.google.com/storage/docs/json_api/v1/objects#resource for API reference.
pub struct Object {
    id: String,
    name: String,
    bucket: String,
    #[serde_as(as = "serde_with::DisplayFromStr")]
    generation: i64,
    #[serde_as(as = "serde_with::DisplayFromStr")]
    metageneration: i64,
    content_type: String,
    storage_class: String,
    #[serde_as(as = "serde_with::DisplayFromStr")]
    size: u64,
    component_count: i32,
    kms_key_name: String,
    etag: String,
    restore_token: Option<String>,
    content_encoding: String,
    content_disposition: String,
    content_language: String,
    cache_control: String,
    temporary_hold: bool,
    event_based_hold: Option<bool>,
    soft_delete_time: Option<wkt::Timestamp>,
    hard_delete_time: Option<wkt::Timestamp>,
    retention_expiration_time: Option<wkt::Timestamp>,
    time_created: wkt::Timestamp,
    time_finalized: wkt::Timestamp,
    time_deleted: Option<wkt::Timestamp>,
    time_storage_class_updated: wkt::Timestamp,
    updated: wkt::Timestamp,
    custom_time: Option<wkt::Timestamp>,
    acl: Vec<ObjectAccessControl>,
    owner: Option<Owner>,
    customer_encryption: Option<CustomerEncryption>,
    metadata: std::collections::HashMap<String, String>,
    #[serde_as(as = "Option<Crc32c>")]
    crc32c: Option<u32>,
    #[serde_as(as = "serde_with::base64::Base64")]
    md5_hash: bytes::Bytes,
    // The following are excluded from the protos, so we don't really need to parse them.
    media_link: String,
    self_link: String,
    // ObjectRetention cannot be configured or reported through the gRPC API.
    retention: Retention,
}

#[derive(Debug, Default, serde::Deserialize, PartialEq, Clone)]
#[serde(default, rename_all = "camelCase")]
struct Retention {
    retain_until_time: wkt::Timestamp,
    mode: String,
}

// CRC32c checksum is a unsigned 32-bit int encoded using base64 in big-endian byte order.
struct Crc32c;

impl<'de> serde_with::DeserializeAs<'de, u32> for Crc32c {
    fn deserialize_as<D>(deserializer: D) -> Result<u32, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        struct Crc32cVisitor;

        impl serde::de::Visitor<'_> for Crc32cVisitor {
            type Value = u32;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a base64 encoded string")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                let bytes = base64::prelude::BASE64_STANDARD
                    .decode(value)
                    .map_err(serde::de::Error::custom)?;

                let length = bytes.len();
                if bytes.len() != 4 {
                    return Err(serde::de::Error::invalid_length(
                        length,
                        &"a Byte Vector of length 4.",
                    ));
                }
                Ok(((bytes[0] as u32) << 24)
                    + ((bytes[1] as u32) << 16)
                    + ((bytes[2] as u32) << 8)
                    + (bytes[3] as u32))
            }
        }

        deserializer.deserialize_str(Crc32cVisitor)
    }
}

fn new_object_checksums(
    crc32c: Option<u32>,
    md5_hash: bytes::Bytes,
) -> Option<control::model::ObjectChecksums> {
    if crc32c.is_none() && md5_hash.is_empty() {
        return None;
    }
    Some(
        control::model::ObjectChecksums::new()
            .set_or_clear_crc32c(crc32c)
            .set_md5_hash(md5_hash),
    )
}

#[serde_with::serde_as]
#[derive(Debug, Default, serde::Deserialize, PartialEq, Clone)]
#[serde(default, rename_all = "camelCase")]
struct ObjectAccessControl {
    id: String,
    entity: String,
    role: String,
    email: String,
    domain: String,
    entity_id: String,
    etag: String,
    project_team: Option<ProjectTeam>,
}

#[serde_with::serde_as]
#[derive(Debug, Default, serde::Deserialize, PartialEq, Clone)]
#[serde(default, rename_all = "camelCase")]
struct ProjectTeam {
    project_number: String,
    team: String,
}

impl From<ObjectAccessControl> for control::model::ObjectAccessControl {
    fn from(value: ObjectAccessControl) -> Self {
        Self::new()
            .set_id(value.id)
            .set_entity(value.entity)
            .set_role(value.role)
            .set_email(value.email)
            .set_domain(value.domain)
            .set_entity_id(value.entity_id)
            .set_etag(value.etag)
            .set_or_clear_project_team::<control::model::ProjectTeam>(
                value.project_team.map(|x| x.into()),
            )
    }
}

impl From<ProjectTeam> for control::model::ProjectTeam {
    fn from(p: ProjectTeam) -> Self {
        control::model::ProjectTeam::new()
            .set_project_number(p.project_number)
            .set_team(p.team)
    }
}

#[serde_with::serde_as]
#[derive(Debug, Default, serde::Deserialize, PartialEq, Clone)]
#[serde(default, rename_all = "camelCase")]
struct Owner {
    entity: String,
    entity_id: String,
}

impl From<Owner> for control::model::Owner {
    fn from(value: Owner) -> Self {
        Self::new()
            .set_entity(value.entity)
            .set_entity_id(value.entity_id)
    }
}

#[serde_with::serde_as]
#[derive(Debug, Default, serde::Deserialize, PartialEq, Clone)]
#[serde(default, rename_all = "camelCase")]
struct CustomerEncryption {
    encryption_algorithm: String,
    #[serde_as(as = "serde_with::base64::Base64")]
    key_sha256: bytes::Bytes,
}

impl From<CustomerEncryption> for control::model::CustomerEncryption {
    fn from(value: CustomerEncryption) -> Self {
        Self::new()
            .set_encryption_algorithm(value.encryption_algorithm)
            .set_key_sha256_bytes(value.key_sha256)
    }
}

impl From<Object> for control::model::Object {
    fn from(value: Object) -> Self {
        Self::new()
            .set_name(value.name)
            .set_bucket(format!("projects/_/buckets/{}", value.bucket))
            .set_generation(value.generation)
            .set_metageneration(value.metageneration)
            .set_content_type(value.content_type)
            .set_storage_class(value.storage_class)
            .set_size(value.size as i64)
            .set_kms_key(value.kms_key_name)
            .set_etag(value.etag)
            .set_or_clear_restore_token(value.restore_token)
            .set_content_encoding(value.content_encoding)
            .set_content_disposition(value.content_disposition)
            .set_content_language(value.content_language)
            .set_cache_control(value.cache_control)
            .set_temporary_hold(value.temporary_hold)
            .set_or_clear_event_based_hold(value.event_based_hold)
            .set_component_count(value.component_count)
            .set_or_clear_soft_delete_time(value.soft_delete_time)
            .set_or_clear_hard_delete_time(value.hard_delete_time)
            .set_or_clear_retention_expire_time(value.retention_expiration_time)
            .set_create_time(value.time_created)
            .set_finalize_time(value.time_finalized)
            .set_or_clear_delete_time(value.time_deleted)
            .set_update_storage_class_time(value.time_storage_class_updated)
            .set_or_clear_custom_time(value.custom_time)
            .set_update_time(value.updated)
            .set_acl(value.acl)
            .set_or_clear_owner(value.owner)
            .set_metadata(value.metadata)
            .set_or_clear_customer_encryption(value.customer_encryption)
            .set_or_clear_checksums(new_object_checksums(value.crc32c, value.md5_hash))
    }
}

/// Create a JSON object for the [control::model::Object] fields used in uploads.
///
/// When uploading (aka inserting) an object number of metadata fields can be
/// sent via the POST body.
#[allow(dead_code)]
pub(crate) fn insert_body(resource: &control::model::Object) -> serde_json::Value {
    use serde_json::*;

    let mut fields = Vec::new();
    if !resource.acl.is_empty() {
        let list: Vec<Value> = resource
            .acl
            .iter()
            .map(|v| {
                json!({
                    "entity": v.entity,
                    "role": v.role,
                })
            })
            .collect();
        fields.push(("acl", serde_json::Value::Array(list)));
    }
    [
        ("cacheControl", &resource.cache_control),
        ("contentDisposition", &resource.content_disposition),
        ("contentEncoding", &resource.content_encoding),
        ("contentLanguage", &resource.content_language),
        ("contentType", &resource.content_type),
        ("storageClass", &resource.storage_class),
    ]
    .into_iter()
    .for_each(|(name, value)| {
        if value.is_empty() {
            return;
        }
        fields.push((name, Value::String(value.clone())));
    });

    [
        (
            "eventBasedHold",
            resource.event_based_hold.as_ref().unwrap_or(&false),
        ),
        ("temporaryHold", &resource.temporary_hold),
    ]
    .into_iter()
    .for_each(|(name, value)| {
        if !value {
            return;
        }
        fields.push((name, Value::Bool(*value)));
    });

    if let Some(ts) = resource.custom_time {
        fields.push(("customTime", Value::String(String::from(ts))));
    }
    if let Some(cs) = &resource.checksums {
        use base64::prelude::BASE64_STANDARD;
        if let Some(u) = cs.crc32c {
            let bytes = [
                (u >> 24 & 0xFF) as u8,
                (u >> 16 & 0xFF) as u8,
                (u >> 8 & 0xFF) as u8,
                (u & 0xFF) as u8,
            ];
            let value = BASE64_STANDARD.encode(bytes);
            fields.push(("crc32c", Value::String(value)));
        }
        if !cs.md5_hash.is_empty() {
            let value = BASE64_STANDARD.encode(&cs.md5_hash);
            fields.push(("md5Hash", Value::String(value)));
        }
    }
    if !resource.metadata.is_empty() {
        let map: Map<_, _> = resource
            .metadata
            .iter()
            .map(|(k, v)| (k.clone(), Value::String(v.clone())))
            .collect();
        fields.push(("metadata", Value::Object(map)));
    }
    if let Some(r) = resource.retention.as_ref() {
        let mut value = Map::new();
        value.insert(
            "mode".to_string(),
            Value::String(r.mode.name().unwrap_or_default().to_string()),
        );
        if let Some(u) = r.retain_until_time {
            value.insert(
                "retainUntilTime".to_string(),
                Value::String(String::from(u)),
            );
        }
        fields.push(("retention", Value::Object(value)));
    }

    serde_json::Value::Object(
        fields
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use serde_with::DeserializeAs;
    use test_case::test_case;

    #[test]
    fn test_deserialize_object() {
        let json = serde_json::json!({
            // string fields:
            "id": "obj1",
            "name": "test-object.txt",
            "bucket": "my-bucket",
            "contentType": "text/plain",
            "storageClass": "STANDARD",
            // i64 and u64 fields:
            "generation": "123",
            "metageneration": "456",
            "size": "789",
            // boolean fields:
            "temporaryHold": true,
            // number fields:
            "componentCount": 5,
            // datetime fields:
            "timeCreated": "2025-05-13T10:30:00Z",
            // list fields:
            "acl": [{"id": "acl-id","unknownField": 5, "projectTeam": {"projectNumber": "123456", "team": "myteam"}}],
            // map fields:
            "metadata": {"key1": "val1", "key2": "val2", "key3": "val3"},
            // base64 fields:
            "customerEncryption": {"encryptionAlgorithm": "algorithm", "keySha256": "dGhlIHF1aWNrIGJyb3duIGZveCBqdW1wcyBvdmVyIHRoZSBsYXp5IGRvZw"},
            // checksum fields:
            // $ echo 'The quick brown fox jumps over the lazy dog' > quick.txt
            //
            // $ gcloud storage hash quick.txt
            // ---
            // crc32c_hash: /ieOcg==
            // digest_format: base64
            // md5_hash: N8S4ft/8XRmP9aGFzufuCQ==
            // url: quick.txt
            "md5Hash": "N8S4ft/8XRmP9aGFzufuCQ==",
            // base64 encoded uint32 in BigEndian order field:
            "crc32c": "/ieOcg==",
            // unused fields:
            "mediaLink": "my-link",
            "retention": { "mode": "my-mode", "retainUntilTime": "2026-05-13T10:30:00Z"}
        });
        let object: Object = serde_json::from_value(json)
            .expect("json value in object test should be deserializable");

        let want = Object {
            // string fields:
            id: "obj1".to_string(),
            name: "test-object.txt".to_string(),
            bucket: "my-bucket".to_string(),
            content_type: "text/plain".to_string(),
            storage_class: "STANDARD".to_string(),
            // i64 and u64 fields:
            generation: 123,
            metageneration: 456,
            size: 789,
            // boolean fields:
            temporary_hold: true,
            // number fields:
            component_count: 5,
            // datetime fields:
            time_created: wkt::Timestamp::clamp(1747132200, 0),
            // list fields:
            acl: vec![ObjectAccessControl {
                id: "acl-id".to_string(),
                project_team: Some(ProjectTeam {
                    project_number: "123456".to_string(),
                    team: "myteam".to_string(),
                }),
                ..Default::default()
            }],
            // map fields:
            metadata: std::collections::HashMap::from([
                ("key1".to_string(), "val1".to_string()),
                ("key2".to_string(), "val2".to_string()),
                ("key3".to_string(), "val3".to_string()),
            ]),
            // base64 encoded fields:
            customer_encryption: Some(CustomerEncryption {
                encryption_algorithm: "algorithm".to_string(),
                key_sha256: bytes::Bytes::from("the quick brown fox jumps over the lazy dog"),
            }),
            md5_hash: vec![
                55, 196, 184, 126, 223, 252, 93, 25, 143, 245, 161, 133, 206, 231, 238, 9,
            ]
            .into(),
            // base64 encoded uint32 in BigEndian order field:
            crc32c: Some(4264005234),
            // unused in control::model::Object:
            media_link: "my-link".to_string(),
            retention: Retention {
                retain_until_time: wkt::Timestamp::clamp(1778668200, 0),
                mode: "my-mode".to_string(),
            },
            ..Default::default()
        };

        assert_eq!(object, want);
    }

    #[test_case(Object::default(); "default fields")]
    #[test_case(Object {
        // string fields:
        id: "obj1".to_string(),
        name: "test-object.txt".to_string(),
        bucket: "my-bucket".to_string(),
        content_type: "text/plain".to_string(),
        storage_class: "STANDARD".to_string(),
        // i64 and u64 fields:
        generation: 123,
        metageneration: 456,
        size: 789,
        // boolean fields:
        temporary_hold: true,
        // number fields:
        component_count: 5,
        // datetime fields:
        time_created: wkt::Timestamp::clamp(1747132200, 0),
        // list fields:
        acl: vec![
            ObjectAccessControl {
                id: "acl1".to_string(),
                ..Default::default()
            },
            ObjectAccessControl {
                id: "acl2".to_string(),
                ..Default::default()
            },
        ],
        // map fields:
        metadata: std::collections::HashMap::from([
            ("key1".to_string(), "val1".to_string()),
            ("key2".to_string(), "val2".to_string()),
            ("key3".to_string(), "val3".to_string()),
        ]),
        // unused in control::model
        media_link: "my-media-link".to_string(),
        ..Default::default()
    }; "some fields set")]
    #[test_case(Object {
        id: "obj1".to_string(),
        name: "test-object.txt".to_string(),
        bucket: "my-bucket".to_string(),
        generation: 123,
        metageneration: 456,
        content_type: "text/plain".to_string(),
        storage_class: "STANDARD".to_string(),
        size: 789,
        component_count: 101112,
        kms_key_name: "my-kms-key".to_string(),
        etag: "etag1".to_string(),
        restore_token: Some("restore-token1".to_string()),
        content_encoding: "content-encoding".to_string(),
        content_disposition: "content-disposition1".to_string(),
        content_language: "content-language1".to_string(),
        cache_control: "cache-control1".to_string(),
        temporary_hold: true,
        event_based_hold: Some(false),
        soft_delete_time: Some(wkt::Timestamp::clamp(1747132200, 1)),
        hard_delete_time: Some(wkt::Timestamp::clamp(1747132200, 2)),
        retention_expiration_time: Some(wkt::Timestamp::clamp(1747132200, 3)),
        time_created: wkt::Timestamp::clamp(1747132200, 4),
        time_finalized: wkt::Timestamp::clamp(1747132200, 5),
        time_deleted: Some(wkt::Timestamp::clamp(1747132200, 6)),
        time_storage_class_updated: wkt::Timestamp::clamp(1747132200, 7),
        updated: wkt::Timestamp::clamp(1747132200, 8),
        custom_time: Some(wkt::Timestamp::clamp(1747132200, 9)),
        acl: vec![
            ObjectAccessControl {
                id: "acl1".to_string(),
                ..Default::default()
            },
            ObjectAccessControl {
                id: "acl2".to_string(),
                ..Default::default()
            },
        ],
        owner: Some(Owner{
            entity: "user-emailAddress".to_string(),
            entity_id: "entity-id".to_string(),
        }),
        metadata: std::collections::HashMap::from([
            ("key1".to_string(), "val1".to_string()),
            ("key2".to_string(), "val2".to_string()),
            ("key3".to_string(), "val3".to_string()),
        ]),
        customer_encryption: Some(CustomerEncryption{
            encryption_algorithm: "my-encryption-alg".to_string(),
            key_sha256: "hash-of-encryption-key".into(),
        }),
        md5_hash: "md5Hash".into(),
        crc32c: Some(4321),
        // unused in control::model
        media_link: "my-media-link".to_string(),
        self_link: "my-self-link".to_string(),
        retention: Retention { retain_until_time: wkt::Timestamp::clamp(1747132200, 10), mode: "mode".to_string() }
    }; "all fields set")]
    // Tests for acl values.
    #[test_case(Object { acl: Vec::new(), ..Default::default()}; "empty acl")]
    #[test_case(Object {acl: vec![ObjectAccessControl::default(), object_acl_with_some_fields(), object_acl_with_all_fields()], ..Default::default()}; "acls with different fields")]
    fn test_convert_object_to_control_model(object: Object) {
        let got = control::model::Object::from(object.clone());
        assert_eq_object(object, got);
    }

    fn assert_eq_object(object: Object, got: control::model::Object) {
        assert_eq!(got.name, object.name);
        assert_eq!(got.bucket, format!("projects/_/buckets/{}", object.bucket));
        assert_eq!(got.etag, object.etag);
        assert_eq!(got.generation, object.generation);
        assert_eq!(got.restore_token, object.restore_token);
        assert_eq!(got.metageneration, object.metageneration);
        assert_eq!(got.storage_class, object.storage_class);
        assert_eq!(got.size, object.size as i64);
        assert_eq!(got.content_encoding, object.content_encoding);
        assert_eq!(got.content_disposition, object.content_disposition);
        assert_eq!(got.cache_control, object.cache_control);
        got.acl
            .iter()
            .zip(object.acl)
            .for_each(|a| assert_eq_object_access_control(a.0, &a.1));
        assert_eq!(got.content_language, object.content_language);
        assert_eq!(got.delete_time, object.time_deleted);
        assert_eq!(
            got.finalize_time.expect("finalize time is set"),
            object.time_finalized
        );
        assert_eq!(got.content_type, object.content_type);
        assert_eq!(
            got.create_time.expect("create time is set"),
            object.time_created
        );
        assert_eq!(got.component_count, object.component_count);
        assert_eq!(got.update_time.expect("update time is set"), object.updated);
        assert_eq!(got.kms_key, object.kms_key_name);
        assert_eq!(
            got.update_storage_class_time
                .expect("update storage class time is set"),
            object.time_storage_class_updated
        );
        assert_eq!(got.temporary_hold, object.temporary_hold);
        assert_eq!(got.retention_expire_time, object.retention_expiration_time);
        assert_eq!(got.event_based_hold, object.event_based_hold);
        assert_eq!(got.custom_time, object.custom_time);
        assert_eq!(got.soft_delete_time, object.soft_delete_time);
        assert_eq!(got.hard_delete_time, object.hard_delete_time);
        match (&object.owner, &got.owner) {
            (None, None) => {}
            (Some(from), None) => panic!("expected a value in the owner, {from:?}"),
            (None, Some(got)) => panic!("unexpected value in the owner, {got:?}"),
            (Some(from), Some(got)) => {
                assert_eq!(got.entity, from.entity);
                assert_eq!(got.entity_id, from.entity_id);
            }
        }
        assert_eq!(got.metadata, object.metadata);
        match (&object.customer_encryption, &got.customer_encryption) {
            (None, None) => {}
            (Some(from), None) => {
                panic!("expected a value in the customer_encryption, {from:?}")
            }
            (None, Some(got)) => panic!("unexpected value in the customer_encryption, {got:?}"),
            (Some(from), Some(got)) => {
                assert_eq!(got.encryption_algorithm, from.encryption_algorithm);
                assert_eq!(got.key_sha256_bytes, from.key_sha256);
            }
        }
        match got.checksums {
            Some(checksums) => {
                assert_eq!(object.md5_hash, checksums.md5_hash);
                assert_eq!(object.crc32c, checksums.crc32c)
            }
            None => {
                assert!(object.md5_hash.is_empty());
                assert!(object.crc32c.is_none());
            }
        }
    }

    fn object_acl_with_all_fields() -> ObjectAccessControl {
        ObjectAccessControl {
            id: "acl1".to_string(),
            entity: "entity1".to_string(),
            role: "role1".to_string(),
            email: "email1".to_string(),
            domain: "domain1".to_string(),
            entity_id: "entity1".to_string(),
            etag: "etag1".to_string(),
            project_team: Some(ProjectTeam {
                project_number: "123456".to_string(),
                team: "team1".to_string(),
            }),
        }
    }

    fn object_acl_with_some_fields() -> ObjectAccessControl {
        ObjectAccessControl {
            id: "acl1".to_string(),
            entity: "entity1".to_string(),
            role: "role1".to_string(),
            project_team: Some(ProjectTeam {
                project_number: "123456".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test_case(ObjectAccessControl::default(); "default fields")]
    #[test_case(object_acl_with_all_fields(); "all fields have values")]
    #[test_case(object_acl_with_some_fields(); "some fields have values" )]
    fn test_object_access_control(from: ObjectAccessControl) {
        let got = control::model::ObjectAccessControl::from(from.clone());
        assert_eq_object_access_control(&got, &from);
    }

    fn assert_eq_object_access_control(
        got: &control::model::ObjectAccessControl,
        from: &ObjectAccessControl,
    ) {
        assert_eq!(got.id, from.id);
        assert_eq!(got.entity, from.entity);
        assert_eq!(got.role, from.role);
        assert_eq!(got.email, from.email);
        assert_eq!(got.domain, from.domain);
        assert_eq!(got.entity_id, from.entity_id);
        assert_eq!(got.etag, from.etag);
        match (&from.project_team, &got.project_team) {
            (None, None) => {}
            (Some(from), None) => {
                panic!("expected a value in the project team, {from:?}")
            }
            (None, Some(got)) => panic!("unexpected value in the project team, {got:?}"),
            (Some(from), Some(got)) => {
                assert_eq!(got.project_number, from.project_number);
                assert_eq!(got.team, from.team);
            }
        }
    }

    #[test_case(None, bytes::Bytes::new(), None; "unset")]
    #[test_case(Some(5), bytes::Bytes::new(), Some(control::model::ObjectChecksums::new().set_crc32c(5_u32)); "crc32c set")]
    #[test_case(None, "hello".into(), Some(control::model::ObjectChecksums::new().set_md5_hash("hello")); "md5_hash set")]
    #[test_case(Some(5), "hello".into(), Some(control::model::ObjectChecksums::new().set_crc32c(5_u32).set_md5_hash("hello")); "both set")]
    fn test_new_object_checksums(
        crc32c: Option<u32>,
        md5_hash: bytes::Bytes,
        want: Option<control::model::ObjectChecksums>,
    ) {
        assert_eq!(new_object_checksums(crc32c, md5_hash), want)
    }

    #[test_case("AAAAAA==", 0_u32; "zero")]
    #[test_case("SZYC0g==", 1234567890_u32; "number")]
    #[test_case("/////w==", u32::MAX; "max u32")]
    fn test_deserialize_crc32c(s: &str, want: u32) {
        let got =
            Crc32c::deserialize_as(serde_json::json!(s)).expect("deserialization should not error");
        assert_eq!(got, want);
    }

    #[test_case(""; "empty")]
    #[test_case("invalid"; "invalid")]
    #[test_case("AAA="; "too small")]
    #[test_case("AAAAAAAAAAA="; "too large")]
    fn test_deserialize_crc32c_err(input: &str) {
        Crc32c::deserialize_as(serde_json::json!(input))
            .expect_err("expected error deserializing string");
    }

    #[test]
    fn test_deserialize_crc32c_not_string_err() {
        Crc32c::deserialize_as(serde_json::json!(5)).expect_err("expected error deserializing int");
    }

    #[test_case(
        control::model::Object::new().set_acl([
            control::model::ObjectAccessControl::new().set_entity("test-entity").set_role("READER")
        ]),
        json!({"acl": [{"entity": "test-entity", "role": "READER"}]})
    )]
    #[test_case(
        control::model::Object::new().set_cache_control("public, max-age=3600"),
        json!({"cacheControl": "public, max-age=3600"})
    )]
    #[test_case(
        control::model::Object::new().set_content_disposition("inline"),
        json!({"contentDisposition": "inline"})
    )]
    #[test_case(
        control::model::Object::new().set_content_encoding("gzip"),
        json!({"contentEncoding": "gzip"})
    )]
    #[test_case(
        control::model::Object::new().set_content_language("en"),
        json!({"contentLanguage": "en"})
    )]
    #[test_case(
        control::model::Object::new().set_content_type("application/octet-stream"),
        json!({"contentType": "application/octet-stream"})
    )]
    #[test_case(
        control::model::Object::new().set_checksums(control::model::ObjectChecksums::new().set_crc32c(0x01020304_u32)),
        json!({"crc32c": "AQIDBA=="})
    )]
    #[test_case(
        control::model::Object::new().set_custom_time(wkt::Timestamp::try_from("2025-07-03T16:22:00Z").unwrap()),
        json!({"customTime": "2025-07-03T16:22:00Z"})
    )]
    #[test_case(
        control::model::Object::new().set_event_based_hold(true),
        json!({"eventBasedHold": true})
    )]
    #[test_case(
        control::model::Object::new().set_checksums(
            control::model::ObjectChecksums::new().set_md5_hash(
                vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            )),
        json!({"md5Hash": "AQIDBAUGBwgJCgsMDQ4PEA=="})
    )]
    #[test_case(
        control::model::Object::new().set_metadata([("k0", "v0"), ("k1", "v1")]),
        json!({"metadata": {"k0": "v0", "k1": "v1"}})
    )]
    #[test_case(
        control::model::Object::new().set_retention(
            control::model::object::Retention::new().set_mode(control::model::object::retention::Mode::Locked)
                .set_retain_until_time(wkt::Timestamp::try_from("2035-07-03T15:03:00Z").unwrap()),
        ),
        json!({"retention": {"mode": "LOCKED", "retainUntilTime": "2035-07-03T15:03:00Z"}})
    )]
    #[test_case(
        control::model::Object::new().set_storage_class("ARCHIVE"),
        json!({"storageClass": "ARCHIVE"})
    )]
    #[test_case(
        control::model::Object::new().set_temporary_hold(false),
        json!({})
    )]
    #[test_case(
        control::model::Object::new().set_temporary_hold(true),
        json!({"temporaryHold": true})
    )]
    fn insert_body(input: control::model::Object, want: serde_json::Value) {
        let got = super::insert_body(&input);
        assert_eq!(got, want);
    }
}
