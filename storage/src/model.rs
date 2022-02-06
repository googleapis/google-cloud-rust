// Copyright 2022 Google LLC
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

#![allow(dead_code)]

use serde::{Deserialize, Serialize};

/// A bucket.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct Bucket {
    /// Access controls on the bucket.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub acl: Option<Vec<BucketAccessControl>>,
    /// The bucket's Autoclass configuration.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub autoclass: Option<BucketAutoclass>,
    /// The bucket's billing configuration.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub billing: Option<BucketBilling>,
    /// The bucket's Cross-Origin Resource Sharing (CORS) configuration.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cors: Option<Vec<BucketCors>>,
    /// The bucket's custom placement configuration for Custom Dual Regions.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub custom_placement_config: Option<BucketCustomPlacementConfig>,
    /// The default value for event-based hold on newly created objects in
    /// this bucket. Event-based hold is a way to retain objects indefinitely
    /// until an event occurs, signified by the hold's release. After being
    /// released, such objects will be subject to bucket-level retention (if
    /// any). One sample use case of this flag is for banks to hold loan
    /// documents for at least 3 years after loan is paid in full. Here,
    /// bucket-level retention is 3 years and the event is loan being paid in
    /// full. In this example, these objects will be held intact for any
    /// number of years until the event has occurred (event-based hold on the
    /// object is released) and then 3 more years after that. That means
    /// retention duration of the objects begins from the moment event-based
    /// hold transitioned from true to false. Objects under event-based hold
    /// cannot be deleted, overwritten or archived until the hold is removed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_event_based_hold: Option<bool>,
    /// Default access controls to apply to new objects when no ACL is
    /// provided.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_object_acl: Option<Vec<ObjectAccessControl>>,
    /// Encryption configuration for a bucket.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encryption: Option<BucketEncryption>,
    /// HTTP 1.1 Entity tag for the bucket.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub etag: Option<String>,
    /// The bucket's IAM configuration.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub iam_configuration: Option<BucketIamConfiguration>,
    /// The ID of the bucket. For buckets, the id and name properties are the
    /// same.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// The kind of item this is. For buckets, this is always storage#bucket.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// User-provided labels, in key/value pairs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub labels: Option<std::collections::HashMap<String, String>>,
    /// The bucket's lifecycle configuration. See lifecycle management for
    /// more information.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lifecycle: Option<BucketLifecycle>,
    /// The location of the bucket. Object data for objects in the bucket
    /// resides in physical storage within this region. Defaults to US. See
    /// the developer's guide for the authoritative list.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
    /// The type of the bucket location.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location_type: Option<String>,
    /// The bucket's logging configuration, which defines the destination
    /// bucket and optional name prefix for the current bucket's logs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logging: Option<BucketLogging>,
    /// The metadata generation of this bucket.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metageneration: Option<String>,
    /// The name of the bucket.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// The owner of the bucket. This is always the project team's owner
    /// group.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner: Option<BucketOwner>,
    /// The project number of the project the bucket belongs to.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_number: Option<String>,
    /// The bucket's retention policy. The retention policy enforces a
    /// minimum retention time for all objects contained in the bucket, based
    /// on their creation time. Any attempt to overwrite or delete objects
    /// younger than the retention period will result in a PERMISSION_DENIED
    /// error. An unlocked retention policy can be modified or removed from
    /// the bucket via a storage.buckets.update operation. A locked retention
    /// policy cannot be removed or shortened in duration for the lifetime of
    /// the bucket. Attempting to remove or decrease period of a locked
    /// retention policy will result in a PERMISSION_DENIED error.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retention_policy: Option<BucketRetentionPolicy>,
    /// The Recovery Point Objective (RPO) of this bucket. Set to ASYNC_TURBO
    /// to turn on Turbo Replication on a bucket.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rpo: Option<String>,
    /// Reserved for future use.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub satisfies_p_z_s: Option<bool>,
    /// The URI of this bucket.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub self_link: Option<String>,
    /// The bucket's default storage class, used whenever no storageClass is
    /// specified for a newly-created object. This defines how objects in the
    /// bucket are stored and determines the SLA and the cost of storage.
    /// Values include MULTI_REGIONAL, REGIONAL, STANDARD, NEARLINE,
    /// COLDLINE, ARCHIVE, and DURABLE_REDUCED_AVAILABILITY. If this value is
    /// not specified when the bucket is created, it will default to
    /// STANDARD. For more information, see storage classes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_class: Option<String>,
    /// The creation time of the bucket in RFC 3339 format.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_created: Option<String>,
    /// The modification time of the bucket in RFC 3339 format.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated: Option<String>,
    /// The bucket's versioning configuration.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub versioning: Option<BucketVersioning>,
    /// The bucket's website configuration, controlling how the service
    /// behaves when accessing bucket contents as a web site. See the Static
    /// Website Examples for more information.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub website: Option<BucketWebsite>,
}

impl Bucket {
    /// Creates a builder to more easily construct the [Bucket] struct.
    pub fn builder() -> BucketBuilder {
        Default::default()
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, Default)]
/// A builder used to more easily construct the [Bucket] struct.
pub struct BucketBuilder {
    acl: Option<Vec<BucketAccessControl>>,
    autoclass: Option<BucketAutoclass>,
    billing: Option<BucketBilling>,
    cors: Option<Vec<BucketCors>>,
    custom_placement_config: Option<BucketCustomPlacementConfig>,
    default_event_based_hold: Option<bool>,
    default_object_acl: Option<Vec<ObjectAccessControl>>,
    encryption: Option<BucketEncryption>,
    etag: Option<String>,
    iam_configuration: Option<BucketIamConfiguration>,
    id: Option<String>,
    kind: Option<String>,
    labels: Option<std::collections::HashMap<String, String>>,
    lifecycle: Option<BucketLifecycle>,
    location: Option<String>,
    location_type: Option<String>,
    logging: Option<BucketLogging>,
    metageneration: Option<String>,
    name: Option<String>,
    owner: Option<BucketOwner>,
    project_number: Option<String>,
    retention_policy: Option<BucketRetentionPolicy>,
    rpo: Option<String>,
    satisfies_p_z_s: Option<bool>,
    self_link: Option<String>,
    storage_class: Option<String>,
    time_created: Option<String>,
    updated: Option<String>,
    versioning: Option<BucketVersioning>,
    website: Option<BucketWebsite>,
}

impl BucketBuilder {
    /// Access controls on the bucket.
    pub fn acl(mut self, value: Vec<BucketAccessControl>) -> Self {
        self.acl = Some(value);
        self
    }
    /// The bucket's Autoclass configuration.
    pub fn autoclass(mut self, value: BucketAutoclass) -> Self {
        self.autoclass = Some(value);
        self
    }
    /// The bucket's billing configuration.
    pub fn billing(mut self, value: BucketBilling) -> Self {
        self.billing = Some(value);
        self
    }
    /// The bucket's Cross-Origin Resource Sharing (CORS) configuration.
    pub fn cors(mut self, value: Vec<BucketCors>) -> Self {
        self.cors = Some(value);
        self
    }
    /// The bucket's custom placement configuration for Custom Dual Regions.
    pub fn custom_placement_config(mut self, value: BucketCustomPlacementConfig) -> Self {
        self.custom_placement_config = Some(value);
        self
    }
    /// The default value for event-based hold on newly created objects in
    /// this bucket. Event-based hold is a way to retain objects indefinitely
    /// until an event occurs, signified by the hold's release. After being
    /// released, such objects will be subject to bucket-level retention (if
    /// any). One sample use case of this flag is for banks to hold loan
    /// documents for at least 3 years after loan is paid in full. Here,
    /// bucket-level retention is 3 years and the event is loan being paid in
    /// full. In this example, these objects will be held intact for any
    /// number of years until the event has occurred (event-based hold on the
    /// object is released) and then 3 more years after that. That means
    /// retention duration of the objects begins from the moment event-based
    /// hold transitioned from true to false. Objects under event-based hold
    /// cannot be deleted, overwritten or archived until the hold is removed.
    pub fn default_event_based_hold(mut self, value: bool) -> Self {
        self.default_event_based_hold = Some(value);
        self
    }
    /// Default access controls to apply to new objects when no ACL is
    /// provided.
    pub fn default_object_acl(mut self, value: Vec<ObjectAccessControl>) -> Self {
        self.default_object_acl = Some(value);
        self
    }
    /// Encryption configuration for a bucket.
    pub fn encryption(mut self, value: BucketEncryption) -> Self {
        self.encryption = Some(value);
        self
    }
    /// HTTP 1.1 Entity tag for the bucket.
    pub fn etag(mut self, value: impl Into<String>) -> Self {
        self.etag = Some(value.into());
        self
    }
    /// The bucket's IAM configuration.
    pub fn iam_configuration(mut self, value: BucketIamConfiguration) -> Self {
        self.iam_configuration = Some(value);
        self
    }
    /// The ID of the bucket. For buckets, the id and name properties are the
    /// same.
    pub fn id(mut self, value: impl Into<String>) -> Self {
        self.id = Some(value.into());
        self
    }
    /// The kind of item this is. For buckets, this is always storage#bucket.
    pub fn kind(mut self, value: impl Into<String>) -> Self {
        self.kind = Some(value.into());
        self
    }
    /// User-provided labels, in key/value pairs.
    pub fn labels(mut self, value: std::collections::HashMap<String, String>) -> Self {
        self.labels = Some(value);
        self
    }
    /// The bucket's lifecycle configuration. See lifecycle management for
    /// more information.
    pub fn lifecycle(mut self, value: BucketLifecycle) -> Self {
        self.lifecycle = Some(value);
        self
    }
    /// The location of the bucket. Object data for objects in the bucket
    /// resides in physical storage within this region. Defaults to US. See
    /// the developer's guide for the authoritative list.
    pub fn location(mut self, value: impl Into<String>) -> Self {
        self.location = Some(value.into());
        self
    }
    /// The type of the bucket location.
    pub fn location_type(mut self, value: impl Into<String>) -> Self {
        self.location_type = Some(value.into());
        self
    }
    /// The bucket's logging configuration, which defines the destination
    /// bucket and optional name prefix for the current bucket's logs.
    pub fn logging(mut self, value: BucketLogging) -> Self {
        self.logging = Some(value);
        self
    }
    /// The metadata generation of this bucket.
    pub fn metageneration(mut self, value: impl Into<String>) -> Self {
        self.metageneration = Some(value.into());
        self
    }
    /// The name of the bucket.
    pub fn name(mut self, value: impl Into<String>) -> Self {
        self.name = Some(value.into());
        self
    }
    /// The owner of the bucket. This is always the project team's owner
    /// group.
    pub fn owner(mut self, value: BucketOwner) -> Self {
        self.owner = Some(value);
        self
    }
    /// The project number of the project the bucket belongs to.
    pub fn project_number(mut self, value: impl Into<String>) -> Self {
        self.project_number = Some(value.into());
        self
    }
    /// The bucket's retention policy. The retention policy enforces a
    /// minimum retention time for all objects contained in the bucket, based
    /// on their creation time. Any attempt to overwrite or delete objects
    /// younger than the retention period will result in a PERMISSION_DENIED
    /// error. An unlocked retention policy can be modified or removed from
    /// the bucket via a storage.buckets.update operation. A locked retention
    /// policy cannot be removed or shortened in duration for the lifetime of
    /// the bucket. Attempting to remove or decrease period of a locked
    /// retention policy will result in a PERMISSION_DENIED error.
    pub fn retention_policy(mut self, value: BucketRetentionPolicy) -> Self {
        self.retention_policy = Some(value);
        self
    }
    /// The Recovery Point Objective (RPO) of this bucket. Set to ASYNC_TURBO
    /// to turn on Turbo Replication on a bucket.
    pub fn rpo(mut self, value: impl Into<String>) -> Self {
        self.rpo = Some(value.into());
        self
    }
    /// Reserved for future use.
    pub fn satisfies_p_z_s(mut self, value: bool) -> Self {
        self.satisfies_p_z_s = Some(value);
        self
    }
    /// The URI of this bucket.
    pub fn self_link(mut self, value: impl Into<String>) -> Self {
        self.self_link = Some(value.into());
        self
    }
    /// The bucket's default storage class, used whenever no storageClass is
    /// specified for a newly-created object. This defines how objects in the
    /// bucket are stored and determines the SLA and the cost of storage.
    /// Values include MULTI_REGIONAL, REGIONAL, STANDARD, NEARLINE,
    /// COLDLINE, ARCHIVE, and DURABLE_REDUCED_AVAILABILITY. If this value is
    /// not specified when the bucket is created, it will default to
    /// STANDARD. For more information, see storage classes.
    pub fn storage_class(mut self, value: impl Into<String>) -> Self {
        self.storage_class = Some(value.into());
        self
    }
    /// The creation time of the bucket in RFC 3339 format.
    pub fn time_created(mut self, value: impl Into<String>) -> Self {
        self.time_created = Some(value.into());
        self
    }
    /// The modification time of the bucket in RFC 3339 format.
    pub fn updated(mut self, value: impl Into<String>) -> Self {
        self.updated = Some(value.into());
        self
    }
    /// The bucket's versioning configuration.
    pub fn versioning(mut self, value: BucketVersioning) -> Self {
        self.versioning = Some(value);
        self
    }
    /// The bucket's website configuration, controlling how the service
    /// behaves when accessing bucket contents as a web site. See the Static
    /// Website Examples for more information.
    pub fn website(mut self, value: BucketWebsite) -> Self {
        self.website = Some(value);
        self
    }
    /// Builds [Bucket].
    pub fn build(self) -> Bucket {
        Bucket {
            acl: self.acl,
            autoclass: self.autoclass,
            billing: self.billing,
            cors: self.cors,
            custom_placement_config: self.custom_placement_config,
            default_event_based_hold: self.default_event_based_hold,
            default_object_acl: self.default_object_acl,
            encryption: self.encryption,
            etag: self.etag,
            iam_configuration: self.iam_configuration,
            id: self.id,
            kind: self.kind,
            labels: self.labels,
            lifecycle: self.lifecycle,
            location: self.location,
            location_type: self.location_type,
            logging: self.logging,
            metageneration: self.metageneration,
            name: self.name,
            owner: self.owner,
            project_number: self.project_number,
            retention_policy: self.retention_policy,
            rpo: self.rpo,
            satisfies_p_z_s: self.satisfies_p_z_s,
            self_link: self.self_link,
            storage_class: self.storage_class,
            time_created: self.time_created,
            updated: self.updated,
            versioning: self.versioning,
            website: self.website,
        }
    }
}

/// An access-control entry.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct BucketAccessControl {
    /// The name of the bucket.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bucket: Option<String>,
    /// The domain associated with the entity, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub domain: Option<String>,
    /// The email address associated with the entity, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    /// The entity holding the permission, in one of the following forms:
    /// - user-userId
    /// - user-email
    /// - group-groupId
    /// - group-email
    /// - domain-domain
    /// - project-team-projectId
    /// - allUsers
    /// - allAuthenticatedUsers Examples:
    /// - The user liz@example.com would be user-liz@example.com.
    /// - The group example@googlegroups.com would be
    /// group-example@googlegroups.com.
    /// - To refer to all members of the Google Apps for Business domain
    /// example.com, the entity would be domain-example.com.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entity: Option<String>,
    /// The ID for the entity, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entity_id: Option<String>,
    /// HTTP 1.1 Entity tag for the access-control entry.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub etag: Option<String>,
    /// The ID of the access-control entry.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// The kind of item this is. For bucket access control entries, this is
    /// always storage#bucketAccessControl.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// The project team associated with the entity, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_team: Option<BucketAccessControlProjectTeam>,
    /// The access permission for the entity.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    /// The link to this access-control entry.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub self_link: Option<String>,
}

impl BucketAccessControl {
    /// Creates a builder to more easily construct the [BucketAccessControl] struct.
    pub fn builder() -> BucketAccessControlBuilder {
        Default::default()
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, Default)]
/// A builder used to more easily construct the [BucketAccessControl] struct.
pub struct BucketAccessControlBuilder {
    bucket: Option<String>,
    domain: Option<String>,
    email: Option<String>,
    entity: Option<String>,
    entity_id: Option<String>,
    etag: Option<String>,
    id: Option<String>,
    kind: Option<String>,
    project_team: Option<BucketAccessControlProjectTeam>,
    role: Option<String>,
    self_link: Option<String>,
}

impl BucketAccessControlBuilder {
    /// The name of the bucket.
    pub fn bucket(mut self, value: impl Into<String>) -> Self {
        self.bucket = Some(value.into());
        self
    }
    /// The domain associated with the entity, if any.
    pub fn domain(mut self, value: impl Into<String>) -> Self {
        self.domain = Some(value.into());
        self
    }
    /// The email address associated with the entity, if any.
    pub fn email(mut self, value: impl Into<String>) -> Self {
        self.email = Some(value.into());
        self
    }
    /// The entity holding the permission, in one of the following forms:
    /// - user-userId
    /// - user-email
    /// - group-groupId
    /// - group-email
    /// - domain-domain
    /// - project-team-projectId
    /// - allUsers
    /// - allAuthenticatedUsers Examples:
    /// - The user liz@example.com would be user-liz@example.com.
    /// - The group example@googlegroups.com would be
    /// group-example@googlegroups.com.
    /// - To refer to all members of the Google Apps for Business domain
    /// example.com, the entity would be domain-example.com.
    pub fn entity(mut self, value: impl Into<String>) -> Self {
        self.entity = Some(value.into());
        self
    }
    /// The ID for the entity, if any.
    pub fn entity_id(mut self, value: impl Into<String>) -> Self {
        self.entity_id = Some(value.into());
        self
    }
    /// HTTP 1.1 Entity tag for the access-control entry.
    pub fn etag(mut self, value: impl Into<String>) -> Self {
        self.etag = Some(value.into());
        self
    }
    /// The ID of the access-control entry.
    pub fn id(mut self, value: impl Into<String>) -> Self {
        self.id = Some(value.into());
        self
    }
    /// The kind of item this is. For bucket access control entries, this is
    /// always storage#bucketAccessControl.
    pub fn kind(mut self, value: impl Into<String>) -> Self {
        self.kind = Some(value.into());
        self
    }
    /// The project team associated with the entity, if any.
    pub fn project_team(mut self, value: BucketAccessControlProjectTeam) -> Self {
        self.project_team = Some(value);
        self
    }
    /// The access permission for the entity.
    pub fn role(mut self, value: impl Into<String>) -> Self {
        self.role = Some(value.into());
        self
    }
    /// The link to this access-control entry.
    pub fn self_link(mut self, value: impl Into<String>) -> Self {
        self.self_link = Some(value.into());
        self
    }
    /// Builds [BucketAccessControl].
    pub fn build(self) -> BucketAccessControl {
        BucketAccessControl {
            bucket: self.bucket,
            domain: self.domain,
            email: self.email,
            entity: self.entity,
            entity_id: self.entity_id,
            etag: self.etag,
            id: self.id,
            kind: self.kind,
            project_team: self.project_team,
            role: self.role,
            self_link: self.self_link,
        }
    }
}

/// The project team associated with the entity, if any.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct BucketAccessControlProjectTeam {
    /// The project number.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_number: Option<String>,
    /// The team.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub team: Option<String>,
}

impl BucketAccessControlProjectTeam {
    /// Creates a builder to more easily construct the [BucketAccessControlProjectTeam] struct.
    pub fn builder() -> BucketAccessControlProjectTeamBuilder {
        Default::default()
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, Default)]
/// A builder used to more easily construct the [BucketAccessControlProjectTeam] struct.
pub struct BucketAccessControlProjectTeamBuilder {
    project_number: Option<String>,
    team: Option<String>,
}

impl BucketAccessControlProjectTeamBuilder {
    /// The project number.
    pub fn project_number(mut self, value: impl Into<String>) -> Self {
        self.project_number = Some(value.into());
        self
    }
    /// The team.
    pub fn team(mut self, value: impl Into<String>) -> Self {
        self.team = Some(value.into());
        self
    }
    /// Builds [BucketAccessControlProjectTeam].
    pub fn build(self) -> BucketAccessControlProjectTeam {
        BucketAccessControlProjectTeam {
            project_number: self.project_number,
            team: self.team,
        }
    }
}

/// An access-control list.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct BucketAccessControls {
    /// The list of items.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub items: Option<Vec<BucketAccessControl>>,
    /// The kind of item this is. For lists of bucket access control entries,
    /// this is always storage#bucketAccessControls.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
}

impl BucketAccessControls {
    /// Creates a builder to more easily construct the [BucketAccessControls] struct.
    pub fn builder() -> BucketAccessControlsBuilder {
        Default::default()
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, Default)]
/// A builder used to more easily construct the [BucketAccessControls] struct.
pub struct BucketAccessControlsBuilder {
    items: Option<Vec<BucketAccessControl>>,
    kind: Option<String>,
}

impl BucketAccessControlsBuilder {
    /// The list of items.
    pub fn items(mut self, value: Vec<BucketAccessControl>) -> Self {
        self.items = Some(value);
        self
    }
    /// The kind of item this is. For lists of bucket access control entries,
    /// this is always storage#bucketAccessControls.
    pub fn kind(mut self, value: impl Into<String>) -> Self {
        self.kind = Some(value.into());
        self
    }
    /// Builds [BucketAccessControls].
    pub fn build(self) -> BucketAccessControls {
        BucketAccessControls {
            items: self.items,
            kind: self.kind,
        }
    }
}

/// The bucket's Autoclass configuration.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct BucketAutoclass {
    /// Whether or not Autoclass is enabled on this bucket
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    /// A date and time in RFC 3339 format representing the instant at which
    /// "enabled" was last toggled.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub toggle_time: Option<String>,
}

impl BucketAutoclass {
    /// Creates a builder to more easily construct the [BucketAutoclass] struct.
    pub fn builder() -> BucketAutoclassBuilder {
        Default::default()
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, Default)]
/// A builder used to more easily construct the [BucketAutoclass] struct.
pub struct BucketAutoclassBuilder {
    enabled: Option<bool>,
    toggle_time: Option<String>,
}

impl BucketAutoclassBuilder {
    /// Whether or not Autoclass is enabled on this bucket
    pub fn enabled(mut self, value: bool) -> Self {
        self.enabled = Some(value);
        self
    }
    /// A date and time in RFC 3339 format representing the instant at which
    /// "enabled" was last toggled.
    pub fn toggle_time(mut self, value: impl Into<String>) -> Self {
        self.toggle_time = Some(value.into());
        self
    }
    /// Builds [BucketAutoclass].
    pub fn build(self) -> BucketAutoclass {
        BucketAutoclass {
            enabled: self.enabled,
            toggle_time: self.toggle_time,
        }
    }
}

/// The bucket's billing configuration.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct BucketBilling {
    /// When set to true, Requester Pays is enabled for this bucket.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requester_pays: Option<bool>,
}

impl BucketBilling {
    /// Creates a builder to more easily construct the [BucketBilling] struct.
    pub fn builder() -> BucketBillingBuilder {
        Default::default()
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, Default)]
/// A builder used to more easily construct the [BucketBilling] struct.
pub struct BucketBillingBuilder {
    requester_pays: Option<bool>,
}

impl BucketBillingBuilder {
    /// When set to true, Requester Pays is enabled for this bucket.
    pub fn requester_pays(mut self, value: bool) -> Self {
        self.requester_pays = Some(value);
        self
    }
    /// Builds [BucketBilling].
    pub fn build(self) -> BucketBilling {
        BucketBilling {
            requester_pays: self.requester_pays,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct BucketCors {
    /// The value, in seconds, to return in the  Access-Control-Max-Age
    /// header used in preflight responses.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_age_seconds: Option<i64>,
    /// The list of HTTP methods on which to include CORS response headers,
    /// (GET, OPTIONS, POST, etc) Note: "*" is permitted in the list of
    /// methods, and means "any method".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub method: Option<Vec<String>>,
    /// The list of Origins eligible to receive CORS response headers. Note:
    /// "*" is permitted in the list of origins, and means "any Origin".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub origin: Option<Vec<String>>,
    /// The list of HTTP headers other than the simple response headers to
    /// give permission for the user-agent to share across domains.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_header: Option<Vec<String>>,
}

impl BucketCors {
    /// Creates a builder to more easily construct the [BucketCors] struct.
    pub fn builder() -> BucketCorsBuilder {
        Default::default()
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, Default)]
/// A builder used to more easily construct the [BucketCors] struct.
pub struct BucketCorsBuilder {
    max_age_seconds: Option<i64>,
    method: Option<Vec<String>>,
    origin: Option<Vec<String>>,
    response_header: Option<Vec<String>>,
}

impl BucketCorsBuilder {
    /// The value, in seconds, to return in the  Access-Control-Max-Age
    /// header used in preflight responses.
    pub fn max_age_seconds(mut self, value: impl Into<i64>) -> Self {
        self.max_age_seconds = Some(value.into());
        self
    }
    /// The list of HTTP methods on which to include CORS response headers,
    /// (GET, OPTIONS, POST, etc) Note: "*" is permitted in the list of
    /// methods, and means "any method".
    pub fn method(mut self, value: Vec<String>) -> Self {
        self.method = Some(value);
        self
    }
    /// The list of Origins eligible to receive CORS response headers. Note:
    /// "*" is permitted in the list of origins, and means "any Origin".
    pub fn origin(mut self, value: Vec<String>) -> Self {
        self.origin = Some(value);
        self
    }
    /// The list of HTTP headers other than the simple response headers to
    /// give permission for the user-agent to share across domains.
    pub fn response_header(mut self, value: Vec<String>) -> Self {
        self.response_header = Some(value);
        self
    }
    /// Builds [BucketCors].
    pub fn build(self) -> BucketCors {
        BucketCors {
            max_age_seconds: self.max_age_seconds,
            method: self.method,
            origin: self.origin,
            response_header: self.response_header,
        }
    }
}

/// The bucket's custom placement configuration for Custom Dual Regions.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct BucketCustomPlacementConfig {
    /// The list of regional locations in which data is placed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_locations: Option<Vec<String>>,
}

impl BucketCustomPlacementConfig {
    /// Creates a builder to more easily construct the [BucketCustomPlacementConfig] struct.
    pub fn builder() -> BucketCustomPlacementConfigBuilder {
        Default::default()
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, Default)]
/// A builder used to more easily construct the [BucketCustomPlacementConfig] struct.
pub struct BucketCustomPlacementConfigBuilder {
    data_locations: Option<Vec<String>>,
}

impl BucketCustomPlacementConfigBuilder {
    /// The list of regional locations in which data is placed.
    pub fn data_locations(mut self, value: Vec<String>) -> Self {
        self.data_locations = Some(value);
        self
    }
    /// Builds [BucketCustomPlacementConfig].
    pub fn build(self) -> BucketCustomPlacementConfig {
        BucketCustomPlacementConfig {
            data_locations: self.data_locations,
        }
    }
}

/// Encryption configuration for a bucket.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct BucketEncryption {
    /// A Cloud KMS key that will be used to encrypt objects inserted into
    /// this bucket, if no encryption method is specified.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_kms_key_name: Option<String>,
}

impl BucketEncryption {
    /// Creates a builder to more easily construct the [BucketEncryption] struct.
    pub fn builder() -> BucketEncryptionBuilder {
        Default::default()
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, Default)]
/// A builder used to more easily construct the [BucketEncryption] struct.
pub struct BucketEncryptionBuilder {
    default_kms_key_name: Option<String>,
}

impl BucketEncryptionBuilder {
    /// A Cloud KMS key that will be used to encrypt objects inserted into
    /// this bucket, if no encryption method is specified.
    pub fn default_kms_key_name(mut self, value: impl Into<String>) -> Self {
        self.default_kms_key_name = Some(value.into());
        self
    }
    /// Builds [BucketEncryption].
    pub fn build(self) -> BucketEncryption {
        BucketEncryption {
            default_kms_key_name: self.default_kms_key_name,
        }
    }
}

/// The bucket's IAM configuration.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct BucketIamConfiguration {
    /// The bucket's uniform bucket-level access configuration. The feature
    /// was formerly known as Bucket Policy Only. For backward compatibility,
    /// this field will be populated with identical information as the
    /// uniformBucketLevelAccess field. We recommend using the
    /// uniformBucketLevelAccess field to enable and disable the feature.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bucket_policy_only: Option<BucketIamConfigurationBucketPolicyOnly>,
    /// The bucket's Public Access Prevention configuration. Currently,
    /// 'unspecified' and 'enforced' are supported.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub public_access_prevention: Option<String>,
    /// The bucket's uniform bucket-level access configuration.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uniform_bucket_level_access: Option<BucketIamConfigurationUniformBucketLevelAccess>,
}

impl BucketIamConfiguration {
    /// Creates a builder to more easily construct the [BucketIamConfiguration] struct.
    pub fn builder() -> BucketIamConfigurationBuilder {
        Default::default()
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, Default)]
/// A builder used to more easily construct the [BucketIamConfiguration] struct.
pub struct BucketIamConfigurationBuilder {
    bucket_policy_only: Option<BucketIamConfigurationBucketPolicyOnly>,
    public_access_prevention: Option<String>,
    uniform_bucket_level_access: Option<BucketIamConfigurationUniformBucketLevelAccess>,
}

impl BucketIamConfigurationBuilder {
    /// The bucket's uniform bucket-level access configuration. The feature
    /// was formerly known as Bucket Policy Only. For backward compatibility,
    /// this field will be populated with identical information as the
    /// uniformBucketLevelAccess field. We recommend using the
    /// uniformBucketLevelAccess field to enable and disable the feature.
    pub fn bucket_policy_only(mut self, value: BucketIamConfigurationBucketPolicyOnly) -> Self {
        self.bucket_policy_only = Some(value);
        self
    }
    /// The bucket's Public Access Prevention configuration. Currently,
    /// 'unspecified' and 'enforced' are supported.
    pub fn public_access_prevention(mut self, value: impl Into<String>) -> Self {
        self.public_access_prevention = Some(value.into());
        self
    }
    /// The bucket's uniform bucket-level access configuration.
    pub fn uniform_bucket_level_access(
        mut self,
        value: BucketIamConfigurationUniformBucketLevelAccess,
    ) -> Self {
        self.uniform_bucket_level_access = Some(value);
        self
    }
    /// Builds [BucketIamConfiguration].
    pub fn build(self) -> BucketIamConfiguration {
        BucketIamConfiguration {
            bucket_policy_only: self.bucket_policy_only,
            public_access_prevention: self.public_access_prevention,
            uniform_bucket_level_access: self.uniform_bucket_level_access,
        }
    }
}

/// The bucket's uniform bucket-level access configuration. The feature
/// was formerly known as Bucket Policy Only. For backward compatibility,
/// this field will be populated with identical information as the
/// uniformBucketLevelAccess field. We recommend using the
/// uniformBucketLevelAccess field to enable and disable the feature.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct BucketIamConfigurationBucketPolicyOnly {
    /// If set, access is controlled only by bucket-level or above IAM
    /// policies.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    /// The deadline for changing iamConfiguration.bucketPolicyOnly.enabled
    /// from true to false in RFC 3339 format.
    /// iamConfiguration.bucketPolicyOnly.enabled may be changed from true to
    /// false until the locked time, after which the field is immutable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub locked_time: Option<String>,
}

impl BucketIamConfigurationBucketPolicyOnly {
    /// Creates a builder to more easily construct the [BucketIamConfigurationBucketPolicyOnly] struct.
    pub fn builder() -> BucketIamConfigurationBucketPolicyOnlyBuilder {
        Default::default()
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, Default)]
/// A builder used to more easily construct the [BucketIamConfigurationBucketPolicyOnly] struct.
pub struct BucketIamConfigurationBucketPolicyOnlyBuilder {
    enabled: Option<bool>,
    locked_time: Option<String>,
}

impl BucketIamConfigurationBucketPolicyOnlyBuilder {
    /// If set, access is controlled only by bucket-level or above IAM
    /// policies.
    pub fn enabled(mut self, value: bool) -> Self {
        self.enabled = Some(value);
        self
    }
    /// The deadline for changing iamConfiguration.bucketPolicyOnly.enabled
    /// from true to false in RFC 3339 format.
    /// iamConfiguration.bucketPolicyOnly.enabled may be changed from true to
    /// false until the locked time, after which the field is immutable.
    pub fn locked_time(mut self, value: impl Into<String>) -> Self {
        self.locked_time = Some(value.into());
        self
    }
    /// Builds [BucketIamConfigurationBucketPolicyOnly].
    pub fn build(self) -> BucketIamConfigurationBucketPolicyOnly {
        BucketIamConfigurationBucketPolicyOnly {
            enabled: self.enabled,
            locked_time: self.locked_time,
        }
    }
}

/// The bucket's uniform bucket-level access configuration.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct BucketIamConfigurationUniformBucketLevelAccess {
    /// If set, access is controlled only by bucket-level or above IAM
    /// policies.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    /// The deadline for changing
    /// iamConfiguration.uniformBucketLevelAccess.enabled from true to false
    /// in RFC 3339  format.
    /// iamConfiguration.uniformBucketLevelAccess.enabled may be changed from
    /// true to false until the locked time, after which the field is
    /// immutable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub locked_time: Option<String>,
}

impl BucketIamConfigurationUniformBucketLevelAccess {
    /// Creates a builder to more easily construct the [BucketIamConfigurationUniformBucketLevelAccess] struct.
    pub fn builder() -> BucketIamConfigurationUniformBucketLevelAccessBuilder {
        Default::default()
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, Default)]
/// A builder used to more easily construct the [BucketIamConfigurationUniformBucketLevelAccess] struct.
pub struct BucketIamConfigurationUniformBucketLevelAccessBuilder {
    enabled: Option<bool>,
    locked_time: Option<String>,
}

impl BucketIamConfigurationUniformBucketLevelAccessBuilder {
    /// If set, access is controlled only by bucket-level or above IAM
    /// policies.
    pub fn enabled(mut self, value: bool) -> Self {
        self.enabled = Some(value);
        self
    }
    /// The deadline for changing
    /// iamConfiguration.uniformBucketLevelAccess.enabled from true to false
    /// in RFC 3339  format.
    /// iamConfiguration.uniformBucketLevelAccess.enabled may be changed from
    /// true to false until the locked time, after which the field is
    /// immutable.
    pub fn locked_time(mut self, value: impl Into<String>) -> Self {
        self.locked_time = Some(value.into());
        self
    }
    /// Builds [BucketIamConfigurationUniformBucketLevelAccess].
    pub fn build(self) -> BucketIamConfigurationUniformBucketLevelAccess {
        BucketIamConfigurationUniformBucketLevelAccess {
            enabled: self.enabled,
            locked_time: self.locked_time,
        }
    }
}

/// The bucket's lifecycle configuration. See lifecycle management for
/// more information.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct BucketLifecycle {
    /// A lifecycle management rule, which is made of an action to take and
    /// the condition(s) under which the action will be taken.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rule: Option<Vec<BucketLifecycleRule>>,
}

impl BucketLifecycle {
    /// Creates a builder to more easily construct the [BucketLifecycle] struct.
    pub fn builder() -> BucketLifecycleBuilder {
        Default::default()
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, Default)]
/// A builder used to more easily construct the [BucketLifecycle] struct.
pub struct BucketLifecycleBuilder {
    rule: Option<Vec<BucketLifecycleRule>>,
}

impl BucketLifecycleBuilder {
    /// A lifecycle management rule, which is made of an action to take and
    /// the condition(s) under which the action will be taken.
    pub fn rule(mut self, value: Vec<BucketLifecycleRule>) -> Self {
        self.rule = Some(value);
        self
    }
    /// Builds [BucketLifecycle].
    pub fn build(self) -> BucketLifecycle {
        BucketLifecycle { rule: self.rule }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct BucketLifecycleRule {
    /// The action to take.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action: Option<BucketLifecycleRuleAction>,
    /// The condition(s) under which the action will be taken.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub condition: Option<BucketLifecycleRuleCondition>,
}

impl BucketLifecycleRule {
    /// Creates a builder to more easily construct the [BucketLifecycleRule] struct.
    pub fn builder() -> BucketLifecycleRuleBuilder {
        Default::default()
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, Default)]
/// A builder used to more easily construct the [BucketLifecycleRule] struct.
pub struct BucketLifecycleRuleBuilder {
    action: Option<BucketLifecycleRuleAction>,
    condition: Option<BucketLifecycleRuleCondition>,
}

impl BucketLifecycleRuleBuilder {
    /// The action to take.
    pub fn action(mut self, value: BucketLifecycleRuleAction) -> Self {
        self.action = Some(value);
        self
    }
    /// The condition(s) under which the action will be taken.
    pub fn condition(mut self, value: BucketLifecycleRuleCondition) -> Self {
        self.condition = Some(value);
        self
    }
    /// Builds [BucketLifecycleRule].
    pub fn build(self) -> BucketLifecycleRule {
        BucketLifecycleRule {
            action: self.action,
            condition: self.condition,
        }
    }
}

/// The action to take.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct BucketLifecycleRuleAction {
    /// Target storage class. Required iff the type of the action is
    /// SetStorageClass.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_class: Option<String>,
    /// Type of the action. Currently, only Delete and SetStorageClass are
    /// supported.
    #[serde(rename = "type")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub type_: Option<String>,
}

impl BucketLifecycleRuleAction {
    /// Creates a builder to more easily construct the [BucketLifecycleRuleAction] struct.
    pub fn builder() -> BucketLifecycleRuleActionBuilder {
        Default::default()
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, Default)]
/// A builder used to more easily construct the [BucketLifecycleRuleAction] struct.
pub struct BucketLifecycleRuleActionBuilder {
    storage_class: Option<String>,
    type_: Option<String>,
}

impl BucketLifecycleRuleActionBuilder {
    /// Target storage class. Required iff the type of the action is
    /// SetStorageClass.
    pub fn storage_class(mut self, value: impl Into<String>) -> Self {
        self.storage_class = Some(value.into());
        self
    }
    /// Type of the action. Currently, only Delete and SetStorageClass are
    /// supported.
    pub fn type_(mut self, value: impl Into<String>) -> Self {
        self.type_ = Some(value.into());
        self
    }
    /// Builds [BucketLifecycleRuleAction].
    pub fn build(self) -> BucketLifecycleRuleAction {
        BucketLifecycleRuleAction {
            storage_class: self.storage_class,
            type_: self.type_,
        }
    }
}

/// The condition(s) under which the action will be taken.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct BucketLifecycleRuleCondition {
    /// Age of an object (in days). This condition is satisfied when an
    /// object reaches the specified age.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub age: Option<i64>,
    /// A date in RFC 3339 format with only the date part (for instance,
    /// "2013-01-15"). This condition is satisfied when an object is created
    /// before midnight of the specified date in UTC.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_before: Option<String>,
    /// A date in RFC 3339 format with only the date part (for instance,
    /// "2013-01-15"). This condition is satisfied when the custom time on an
    /// object is before this date in UTC.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub custom_time_before: Option<String>,
    /// Number of days elapsed since the user-specified timestamp set on an
    /// object. The condition is satisfied if the days elapsed is at least
    /// this number. If no custom timestamp is specified on an object, the
    /// condition does not apply.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub days_since_custom_time: Option<i64>,
    /// Number of days elapsed since the noncurrent timestamp of an object.
    /// The condition is satisfied if the days elapsed is at least this
    /// number. This condition is relevant only for versioned objects. The
    /// value of the field must be a nonnegative integer. If it's zero, the
    /// object version will become eligible for Lifecycle action as soon as
    /// it becomes noncurrent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub days_since_noncurrent_time: Option<i64>,
    /// Relevant only for versioned objects. If the value is true, this
    /// condition matches live objects; if the value is false, it matches
    /// archived objects.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_live: Option<bool>,
    /// A regular expression that satisfies the RE2 syntax. This condition is
    /// satisfied when the name of the object matches the RE2 pattern. Note:
    /// This feature is currently in the "Early Access" launch stage and is
    /// only available to a whitelisted set of users; that means that this
    /// feature may be changed in backward-incompatible ways and that it is
    /// not guaranteed to be released.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matches_pattern: Option<String>,
    /// Objects having any of the storage classes specified by this condition
    /// will be matched. Values include MULTI_REGIONAL, REGIONAL, NEARLINE,
    /// COLDLINE, ARCHIVE, STANDARD, and DURABLE_REDUCED_AVAILABILITY.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matches_storage_class: Option<Vec<String>>,
    /// A date in RFC 3339 format with only the date part (for instance,
    /// "2013-01-15"). This condition is satisfied when the noncurrent time
    /// on an object is before this date in UTC. This condition is relevant
    /// only for versioned objects.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub noncurrent_time_before: Option<String>,
    /// Relevant only for versioned objects. If the value is N, this
    /// condition is satisfied when there are at least N versions (including
    /// the live version) newer than this version of the object.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub num_newer_versions: Option<i64>,
}

impl BucketLifecycleRuleCondition {
    /// Creates a builder to more easily construct the [BucketLifecycleRuleCondition] struct.
    pub fn builder() -> BucketLifecycleRuleConditionBuilder {
        Default::default()
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, Default)]
/// A builder used to more easily construct the [BucketLifecycleRuleCondition] struct.
pub struct BucketLifecycleRuleConditionBuilder {
    age: Option<i64>,
    created_before: Option<String>,
    custom_time_before: Option<String>,
    days_since_custom_time: Option<i64>,
    days_since_noncurrent_time: Option<i64>,
    is_live: Option<bool>,
    matches_pattern: Option<String>,
    matches_storage_class: Option<Vec<String>>,
    noncurrent_time_before: Option<String>,
    num_newer_versions: Option<i64>,
}

impl BucketLifecycleRuleConditionBuilder {
    /// Age of an object (in days). This condition is satisfied when an
    /// object reaches the specified age.
    pub fn age(mut self, value: impl Into<i64>) -> Self {
        self.age = Some(value.into());
        self
    }
    /// A date in RFC 3339 format with only the date part (for instance,
    /// "2013-01-15"). This condition is satisfied when an object is created
    /// before midnight of the specified date in UTC.
    pub fn created_before(mut self, value: impl Into<String>) -> Self {
        self.created_before = Some(value.into());
        self
    }
    /// A date in RFC 3339 format with only the date part (for instance,
    /// "2013-01-15"). This condition is satisfied when the custom time on an
    /// object is before this date in UTC.
    pub fn custom_time_before(mut self, value: impl Into<String>) -> Self {
        self.custom_time_before = Some(value.into());
        self
    }
    /// Number of days elapsed since the user-specified timestamp set on an
    /// object. The condition is satisfied if the days elapsed is at least
    /// this number. If no custom timestamp is specified on an object, the
    /// condition does not apply.
    pub fn days_since_custom_time(mut self, value: impl Into<i64>) -> Self {
        self.days_since_custom_time = Some(value.into());
        self
    }
    /// Number of days elapsed since the noncurrent timestamp of an object.
    /// The condition is satisfied if the days elapsed is at least this
    /// number. This condition is relevant only for versioned objects. The
    /// value of the field must be a nonnegative integer. If it's zero, the
    /// object version will become eligible for Lifecycle action as soon as
    /// it becomes noncurrent.
    pub fn days_since_noncurrent_time(mut self, value: impl Into<i64>) -> Self {
        self.days_since_noncurrent_time = Some(value.into());
        self
    }
    /// Relevant only for versioned objects. If the value is true, this
    /// condition matches live objects; if the value is false, it matches
    /// archived objects.
    pub fn is_live(mut self, value: bool) -> Self {
        self.is_live = Some(value);
        self
    }
    /// A regular expression that satisfies the RE2 syntax. This condition is
    /// satisfied when the name of the object matches the RE2 pattern. Note:
    /// This feature is currently in the "Early Access" launch stage and is
    /// only available to a whitelisted set of users; that means that this
    /// feature may be changed in backward-incompatible ways and that it is
    /// not guaranteed to be released.
    pub fn matches_pattern(mut self, value: impl Into<String>) -> Self {
        self.matches_pattern = Some(value.into());
        self
    }
    /// Objects having any of the storage classes specified by this condition
    /// will be matched. Values include MULTI_REGIONAL, REGIONAL, NEARLINE,
    /// COLDLINE, ARCHIVE, STANDARD, and DURABLE_REDUCED_AVAILABILITY.
    pub fn matches_storage_class(mut self, value: Vec<String>) -> Self {
        self.matches_storage_class = Some(value);
        self
    }
    /// A date in RFC 3339 format with only the date part (for instance,
    /// "2013-01-15"). This condition is satisfied when the noncurrent time
    /// on an object is before this date in UTC. This condition is relevant
    /// only for versioned objects.
    pub fn noncurrent_time_before(mut self, value: impl Into<String>) -> Self {
        self.noncurrent_time_before = Some(value.into());
        self
    }
    /// Relevant only for versioned objects. If the value is N, this
    /// condition is satisfied when there are at least N versions (including
    /// the live version) newer than this version of the object.
    pub fn num_newer_versions(mut self, value: impl Into<i64>) -> Self {
        self.num_newer_versions = Some(value.into());
        self
    }
    /// Builds [BucketLifecycleRuleCondition].
    pub fn build(self) -> BucketLifecycleRuleCondition {
        BucketLifecycleRuleCondition {
            age: self.age,
            created_before: self.created_before,
            custom_time_before: self.custom_time_before,
            days_since_custom_time: self.days_since_custom_time,
            days_since_noncurrent_time: self.days_since_noncurrent_time,
            is_live: self.is_live,
            matches_pattern: self.matches_pattern,
            matches_storage_class: self.matches_storage_class,
            noncurrent_time_before: self.noncurrent_time_before,
            num_newer_versions: self.num_newer_versions,
        }
    }
}

/// The bucket's logging configuration, which defines the destination
/// bucket and optional name prefix for the current bucket's logs.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct BucketLogging {
    /// The destination bucket where the current bucket's logs should be
    /// placed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log_bucket: Option<String>,
    /// A prefix for log object names.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log_object_prefix: Option<String>,
}

impl BucketLogging {
    /// Creates a builder to more easily construct the [BucketLogging] struct.
    pub fn builder() -> BucketLoggingBuilder {
        Default::default()
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, Default)]
/// A builder used to more easily construct the [BucketLogging] struct.
pub struct BucketLoggingBuilder {
    log_bucket: Option<String>,
    log_object_prefix: Option<String>,
}

impl BucketLoggingBuilder {
    /// The destination bucket where the current bucket's logs should be
    /// placed.
    pub fn log_bucket(mut self, value: impl Into<String>) -> Self {
        self.log_bucket = Some(value.into());
        self
    }
    /// A prefix for log object names.
    pub fn log_object_prefix(mut self, value: impl Into<String>) -> Self {
        self.log_object_prefix = Some(value.into());
        self
    }
    /// Builds [BucketLogging].
    pub fn build(self) -> BucketLogging {
        BucketLogging {
            log_bucket: self.log_bucket,
            log_object_prefix: self.log_object_prefix,
        }
    }
}

/// The owner of the bucket. This is always the project team's owner
/// group.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct BucketOwner {
    /// The entity, in the form project-owner-projectId.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entity: Option<String>,
    /// The ID for the entity.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entity_id: Option<String>,
}

impl BucketOwner {
    /// Creates a builder to more easily construct the [BucketOwner] struct.
    pub fn builder() -> BucketOwnerBuilder {
        Default::default()
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, Default)]
/// A builder used to more easily construct the [BucketOwner] struct.
pub struct BucketOwnerBuilder {
    entity: Option<String>,
    entity_id: Option<String>,
}

impl BucketOwnerBuilder {
    /// The entity, in the form project-owner-projectId.
    pub fn entity(mut self, value: impl Into<String>) -> Self {
        self.entity = Some(value.into());
        self
    }
    /// The ID for the entity.
    pub fn entity_id(mut self, value: impl Into<String>) -> Self {
        self.entity_id = Some(value.into());
        self
    }
    /// Builds [BucketOwner].
    pub fn build(self) -> BucketOwner {
        BucketOwner {
            entity: self.entity,
            entity_id: self.entity_id,
        }
    }
}

/// The bucket's retention policy. The retention policy enforces a
/// minimum retention time for all objects contained in the bucket, based
/// on their creation time. Any attempt to overwrite or delete objects
/// younger than the retention period will result in a PERMISSION_DENIED
/// error. An unlocked retention policy can be modified or removed from
/// the bucket via a storage.buckets.update operation. A locked retention
/// policy cannot be removed or shortened in duration for the lifetime of
/// the bucket. Attempting to remove or decrease period of a locked
/// retention policy will result in a PERMISSION_DENIED error.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct BucketRetentionPolicy {
    /// Server-determined value that indicates the time from which policy was
    /// enforced and effective. This value is in RFC 3339 format.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub effective_time: Option<String>,
    /// Once locked, an object retention policy cannot be modified.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_locked: Option<bool>,
    /// The duration in seconds that objects need to be retained. Retention
    /// duration must be greater than zero and less than 100 years. Note that
    /// enforcement of retention periods less than a day is not guaranteed.
    /// Such periods should only be used for testing purposes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retention_period: Option<String>,
}

impl BucketRetentionPolicy {
    /// Creates a builder to more easily construct the [BucketRetentionPolicy] struct.
    pub fn builder() -> BucketRetentionPolicyBuilder {
        Default::default()
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, Default)]
/// A builder used to more easily construct the [BucketRetentionPolicy] struct.
pub struct BucketRetentionPolicyBuilder {
    effective_time: Option<String>,
    is_locked: Option<bool>,
    retention_period: Option<String>,
}

impl BucketRetentionPolicyBuilder {
    /// Server-determined value that indicates the time from which policy was
    /// enforced and effective. This value is in RFC 3339 format.
    pub fn effective_time(mut self, value: impl Into<String>) -> Self {
        self.effective_time = Some(value.into());
        self
    }
    /// Once locked, an object retention policy cannot be modified.
    pub fn is_locked(mut self, value: bool) -> Self {
        self.is_locked = Some(value);
        self
    }
    /// The duration in seconds that objects need to be retained. Retention
    /// duration must be greater than zero and less than 100 years. Note that
    /// enforcement of retention periods less than a day is not guaranteed.
    /// Such periods should only be used for testing purposes.
    pub fn retention_period(mut self, value: impl Into<String>) -> Self {
        self.retention_period = Some(value.into());
        self
    }
    /// Builds [BucketRetentionPolicy].
    pub fn build(self) -> BucketRetentionPolicy {
        BucketRetentionPolicy {
            effective_time: self.effective_time,
            is_locked: self.is_locked,
            retention_period: self.retention_period,
        }
    }
}

/// The bucket's versioning configuration.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct BucketVersioning {
    /// While set to true, versioning is fully enabled for this bucket.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
}

impl BucketVersioning {
    /// Creates a builder to more easily construct the [BucketVersioning] struct.
    pub fn builder() -> BucketVersioningBuilder {
        Default::default()
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, Default)]
/// A builder used to more easily construct the [BucketVersioning] struct.
pub struct BucketVersioningBuilder {
    enabled: Option<bool>,
}

impl BucketVersioningBuilder {
    /// While set to true, versioning is fully enabled for this bucket.
    pub fn enabled(mut self, value: bool) -> Self {
        self.enabled = Some(value);
        self
    }
    /// Builds [BucketVersioning].
    pub fn build(self) -> BucketVersioning {
        BucketVersioning {
            enabled: self.enabled,
        }
    }
}

/// The bucket's website configuration, controlling how the service
/// behaves when accessing bucket contents as a web site. See the Static
/// Website Examples for more information.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct BucketWebsite {
    /// If the requested object path is missing, the service will ensure the
    /// path has a trailing '/', append this suffix, and attempt to retrieve
    /// the resulting object. This allows the creation of index.html objects
    /// to represent directory pages.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub main_page_suffix: Option<String>,
    /// If the requested object path is missing, and any mainPageSuffix
    /// object is missing, if applicable, the service will return the named
    /// object from this bucket as the content for a 404 Not Found result.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub not_found_page: Option<String>,
}

impl BucketWebsite {
    /// Creates a builder to more easily construct the [BucketWebsite] struct.
    pub fn builder() -> BucketWebsiteBuilder {
        Default::default()
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, Default)]
/// A builder used to more easily construct the [BucketWebsite] struct.
pub struct BucketWebsiteBuilder {
    main_page_suffix: Option<String>,
    not_found_page: Option<String>,
}

impl BucketWebsiteBuilder {
    /// If the requested object path is missing, the service will ensure the
    /// path has a trailing '/', append this suffix, and attempt to retrieve
    /// the resulting object. This allows the creation of index.html objects
    /// to represent directory pages.
    pub fn main_page_suffix(mut self, value: impl Into<String>) -> Self {
        self.main_page_suffix = Some(value.into());
        self
    }
    /// If the requested object path is missing, and any mainPageSuffix
    /// object is missing, if applicable, the service will return the named
    /// object from this bucket as the content for a 404 Not Found result.
    pub fn not_found_page(mut self, value: impl Into<String>) -> Self {
        self.not_found_page = Some(value.into());
        self
    }
    /// Builds [BucketWebsite].
    pub fn build(self) -> BucketWebsite {
        BucketWebsite {
            main_page_suffix: self.main_page_suffix,
            not_found_page: self.not_found_page,
        }
    }
}

/// A list of buckets.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct Buckets {
    /// The list of items.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub items: Option<Vec<Bucket>>,
    /// The kind of item this is. For lists of buckets, this is always
    /// storage#buckets.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// The continuation token, used to page through large result sets.
    /// Provide this value in a subsequent request to return the next page of
    /// results.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
}

impl Buckets {
    /// Creates a builder to more easily construct the [Buckets] struct.
    pub fn builder() -> BucketsBuilder {
        Default::default()
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, Default)]
/// A builder used to more easily construct the [Buckets] struct.
pub struct BucketsBuilder {
    items: Option<Vec<Bucket>>,
    kind: Option<String>,
    next_page_token: Option<String>,
}

impl BucketsBuilder {
    /// The list of items.
    pub fn items(mut self, value: Vec<Bucket>) -> Self {
        self.items = Some(value);
        self
    }
    /// The kind of item this is. For lists of buckets, this is always
    /// storage#buckets.
    pub fn kind(mut self, value: impl Into<String>) -> Self {
        self.kind = Some(value.into());
        self
    }
    /// The continuation token, used to page through large result sets.
    /// Provide this value in a subsequent request to return the next page of
    /// results.
    pub fn next_page_token(mut self, value: impl Into<String>) -> Self {
        self.next_page_token = Some(value.into());
        self
    }
    /// Builds [Buckets].
    pub fn build(self) -> Buckets {
        Buckets {
            items: self.items,
            kind: self.kind,
            next_page_token: self.next_page_token,
        }
    }
}

/// An notification channel used to watch for resource changes.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct Channel {
    /// The address where notifications are delivered for this channel.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,
    /// Date and time of notification channel expiration, expressed as a Unix
    /// timestamp, in milliseconds. Optional.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expiration: Option<String>,
    /// A UUID or similar unique string that identifies this channel.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// Identifies this as a notification channel used to watch for changes
    /// to a resource, which is "api#channel".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// Additional parameters controlling delivery channel behavior.
    /// Optional.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<std::collections::HashMap<String, String>>,
    /// A Boolean value to indicate whether payload is wanted. Optional.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<bool>,
    /// An opaque ID that identifies the resource being watched on this
    /// channel. Stable across different API versions.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_id: Option<String>,
    /// A version-specific identifier for the watched resource.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_uri: Option<String>,
    /// An arbitrary string delivered to the target address with each
    /// notification delivered over this channel. Optional.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    /// The type of delivery mechanism used for this channel.
    #[serde(rename = "type")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub type_: Option<String>,
}

impl Channel {
    /// Creates a builder to more easily construct the [Channel] struct.
    pub fn builder() -> ChannelBuilder {
        Default::default()
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, Default)]
/// A builder used to more easily construct the [Channel] struct.
pub struct ChannelBuilder {
    address: Option<String>,
    expiration: Option<String>,
    id: Option<String>,
    kind: Option<String>,
    params: Option<std::collections::HashMap<String, String>>,
    payload: Option<bool>,
    resource_id: Option<String>,
    resource_uri: Option<String>,
    token: Option<String>,
    type_: Option<String>,
}

impl ChannelBuilder {
    /// The address where notifications are delivered for this channel.
    pub fn address(mut self, value: impl Into<String>) -> Self {
        self.address = Some(value.into());
        self
    }
    /// Date and time of notification channel expiration, expressed as a Unix
    /// timestamp, in milliseconds. Optional.
    pub fn expiration(mut self, value: impl Into<String>) -> Self {
        self.expiration = Some(value.into());
        self
    }
    /// A UUID or similar unique string that identifies this channel.
    pub fn id(mut self, value: impl Into<String>) -> Self {
        self.id = Some(value.into());
        self
    }
    /// Identifies this as a notification channel used to watch for changes
    /// to a resource, which is "api#channel".
    pub fn kind(mut self, value: impl Into<String>) -> Self {
        self.kind = Some(value.into());
        self
    }
    /// Additional parameters controlling delivery channel behavior.
    /// Optional.
    pub fn params(mut self, value: std::collections::HashMap<String, String>) -> Self {
        self.params = Some(value);
        self
    }
    /// A Boolean value to indicate whether payload is wanted. Optional.
    pub fn payload(mut self, value: bool) -> Self {
        self.payload = Some(value);
        self
    }
    /// An opaque ID that identifies the resource being watched on this
    /// channel. Stable across different API versions.
    pub fn resource_id(mut self, value: impl Into<String>) -> Self {
        self.resource_id = Some(value.into());
        self
    }
    /// A version-specific identifier for the watched resource.
    pub fn resource_uri(mut self, value: impl Into<String>) -> Self {
        self.resource_uri = Some(value.into());
        self
    }
    /// An arbitrary string delivered to the target address with each
    /// notification delivered over this channel. Optional.
    pub fn token(mut self, value: impl Into<String>) -> Self {
        self.token = Some(value.into());
        self
    }
    /// The type of delivery mechanism used for this channel.
    pub fn type_(mut self, value: impl Into<String>) -> Self {
        self.type_ = Some(value.into());
        self
    }
    /// Builds [Channel].
    pub fn build(self) -> Channel {
        Channel {
            address: self.address,
            expiration: self.expiration,
            id: self.id,
            kind: self.kind,
            params: self.params,
            payload: self.payload,
            resource_id: self.resource_id,
            resource_uri: self.resource_uri,
            token: self.token,
            type_: self.type_,
        }
    }
}

/// A Compose request.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct ComposeRequest {
    /// Properties of the resulting object.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub destination: Option<Object>,
    /// The kind of item this is.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// The list of source objects that will be concatenated into a single
    /// object.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_objects: Option<Vec<ComposeRequestSourceObjects>>,
}

impl ComposeRequest {
    /// Creates a builder to more easily construct the [ComposeRequest] struct.
    pub fn builder() -> ComposeRequestBuilder {
        Default::default()
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, Default)]
/// A builder used to more easily construct the [ComposeRequest] struct.
pub struct ComposeRequestBuilder {
    destination: Option<Object>,
    kind: Option<String>,
    source_objects: Option<Vec<ComposeRequestSourceObjects>>,
}

impl ComposeRequestBuilder {
    /// Properties of the resulting object.
    pub fn destination(mut self, value: Object) -> Self {
        self.destination = Some(value);
        self
    }
    /// The kind of item this is.
    pub fn kind(mut self, value: impl Into<String>) -> Self {
        self.kind = Some(value.into());
        self
    }
    /// The list of source objects that will be concatenated into a single
    /// object.
    pub fn source_objects(mut self, value: Vec<ComposeRequestSourceObjects>) -> Self {
        self.source_objects = Some(value);
        self
    }
    /// Builds [ComposeRequest].
    pub fn build(self) -> ComposeRequest {
        ComposeRequest {
            destination: self.destination,
            kind: self.kind,
            source_objects: self.source_objects,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct ComposeRequestSourceObjects {
    /// The generation of this object to use as the source.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub generation: Option<String>,
    /// The source object's name. All source objects must reside in the same
    /// bucket.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Conditions that must be met for this operation to execute.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_preconditions: Option<ComposeRequestSourceObjectsObjectPreconditions>,
}

impl ComposeRequestSourceObjects {
    /// Creates a builder to more easily construct the [ComposeRequestSourceObjects] struct.
    pub fn builder() -> ComposeRequestSourceObjectsBuilder {
        Default::default()
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, Default)]
/// A builder used to more easily construct the [ComposeRequestSourceObjects] struct.
pub struct ComposeRequestSourceObjectsBuilder {
    generation: Option<String>,
    name: Option<String>,
    object_preconditions: Option<ComposeRequestSourceObjectsObjectPreconditions>,
}

impl ComposeRequestSourceObjectsBuilder {
    /// The generation of this object to use as the source.
    pub fn generation(mut self, value: impl Into<String>) -> Self {
        self.generation = Some(value.into());
        self
    }
    /// The source object's name. All source objects must reside in the same
    /// bucket.
    pub fn name(mut self, value: impl Into<String>) -> Self {
        self.name = Some(value.into());
        self
    }
    /// Conditions that must be met for this operation to execute.
    pub fn object_preconditions(
        mut self,
        value: ComposeRequestSourceObjectsObjectPreconditions,
    ) -> Self {
        self.object_preconditions = Some(value);
        self
    }
    /// Builds [ComposeRequestSourceObjects].
    pub fn build(self) -> ComposeRequestSourceObjects {
        ComposeRequestSourceObjects {
            generation: self.generation,
            name: self.name,
            object_preconditions: self.object_preconditions,
        }
    }
}

/// Conditions that must be met for this operation to execute.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct ComposeRequestSourceObjectsObjectPreconditions {
    /// Only perform the composition if the generation of the source object
    /// that would be used matches this value. If this value and a generation
    /// are both specified, they must be the same value or the call will
    /// fail.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub if_generation_match: Option<String>,
}

impl ComposeRequestSourceObjectsObjectPreconditions {
    /// Creates a builder to more easily construct the [ComposeRequestSourceObjectsObjectPreconditions] struct.
    pub fn builder() -> ComposeRequestSourceObjectsObjectPreconditionsBuilder {
        Default::default()
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, Default)]
/// A builder used to more easily construct the [ComposeRequestSourceObjectsObjectPreconditions] struct.
pub struct ComposeRequestSourceObjectsObjectPreconditionsBuilder {
    if_generation_match: Option<String>,
}

impl ComposeRequestSourceObjectsObjectPreconditionsBuilder {
    /// Only perform the composition if the generation of the source object
    /// that would be used matches this value. If this value and a generation
    /// are both specified, they must be the same value or the call will
    /// fail.
    pub fn if_generation_match(mut self, value: impl Into<String>) -> Self {
        self.if_generation_match = Some(value.into());
        self
    }
    /// Builds [ComposeRequestSourceObjectsObjectPreconditions].
    pub fn build(self) -> ComposeRequestSourceObjectsObjectPreconditions {
        ComposeRequestSourceObjectsObjectPreconditions {
            if_generation_match: self.if_generation_match,
        }
    }
}

/// Represents an expression text. Example: title: "User account
/// presence" description: "Determines whether the request has a user
/// account" expression: "size(request.user) > 0"
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct Expr {
    /// An optional description of the expression. This is a longer text
    /// which describes the expression, e.g. when hovered over it in a UI.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Textual representation of an expression in Common Expression Language
    /// syntax. The application context of the containing message determines
    /// which well-known feature set of CEL is supported.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expression: Option<String>,
    /// An optional string indicating the location of the expression for
    /// error reporting, e.g. a file name and a position in the file.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
    /// An optional title for the expression, i.e. a short string describing
    /// its purpose. This can be used e.g. in UIs which allow to enter the
    /// expression.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
}

impl Expr {
    /// Creates a builder to more easily construct the [Expr] struct.
    pub fn builder() -> ExprBuilder {
        Default::default()
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, Default)]
/// A builder used to more easily construct the [Expr] struct.
pub struct ExprBuilder {
    description: Option<String>,
    expression: Option<String>,
    location: Option<String>,
    title: Option<String>,
}

impl ExprBuilder {
    /// An optional description of the expression. This is a longer text
    /// which describes the expression, e.g. when hovered over it in a UI.
    pub fn description(mut self, value: impl Into<String>) -> Self {
        self.description = Some(value.into());
        self
    }
    /// Textual representation of an expression in Common Expression Language
    /// syntax. The application context of the containing message determines
    /// which well-known feature set of CEL is supported.
    pub fn expression(mut self, value: impl Into<String>) -> Self {
        self.expression = Some(value.into());
        self
    }
    /// An optional string indicating the location of the expression for
    /// error reporting, e.g. a file name and a position in the file.
    pub fn location(mut self, value: impl Into<String>) -> Self {
        self.location = Some(value.into());
        self
    }
    /// An optional title for the expression, i.e. a short string describing
    /// its purpose. This can be used e.g. in UIs which allow to enter the
    /// expression.
    pub fn title(mut self, value: impl Into<String>) -> Self {
        self.title = Some(value.into());
        self
    }
    /// Builds [Expr].
    pub fn build(self) -> Expr {
        Expr {
            description: self.description,
            expression: self.expression,
            location: self.location,
            title: self.title,
        }
    }
}

/// JSON template to produce a JSON-style HMAC Key resource for Create
/// responses.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct HmacKey {
    /// The kind of item this is. For HMAC keys, this is always
    /// storage#hmacKey.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// Key metadata.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HmacKeyMetadata>,
    /// HMAC secret key material.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secret: Option<String>,
}

impl HmacKey {
    /// Creates a builder to more easily construct the [HmacKey] struct.
    pub fn builder() -> HmacKeyBuilder {
        Default::default()
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, Default)]
/// A builder used to more easily construct the [HmacKey] struct.
pub struct HmacKeyBuilder {
    kind: Option<String>,
    metadata: Option<HmacKeyMetadata>,
    secret: Option<String>,
}

impl HmacKeyBuilder {
    /// The kind of item this is. For HMAC keys, this is always
    /// storage#hmacKey.
    pub fn kind(mut self, value: impl Into<String>) -> Self {
        self.kind = Some(value.into());
        self
    }
    /// Key metadata.
    pub fn metadata(mut self, value: HmacKeyMetadata) -> Self {
        self.metadata = Some(value);
        self
    }
    /// HMAC secret key material.
    pub fn secret(mut self, value: impl Into<String>) -> Self {
        self.secret = Some(value.into());
        self
    }
    /// Builds [HmacKey].
    pub fn build(self) -> HmacKey {
        HmacKey {
            kind: self.kind,
            metadata: self.metadata,
            secret: self.secret,
        }
    }
}

/// JSON template to produce a JSON-style HMAC Key metadata resource.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct HmacKeyMetadata {
    /// The ID of the HMAC Key.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub access_id: Option<String>,
    /// HTTP 1.1 Entity tag for the HMAC key.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub etag: Option<String>,
    /// The ID of the HMAC key, including the Project ID and the Access ID.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// The kind of item this is. For HMAC Key metadata, this is always
    /// storage#hmacKeyMetadata.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// Project ID owning the service account to which the key authenticates.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    /// The link to this resource.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub self_link: Option<String>,
    /// The email address of the key's associated service account.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_account_email: Option<String>,
    /// The state of the key. Can be one of ACTIVE, INACTIVE, or DELETED.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
    /// The creation time of the HMAC key in RFC 3339 format.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_created: Option<String>,
    /// The last modification time of the HMAC key metadata in RFC 3339
    /// format.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated: Option<String>,
}

impl HmacKeyMetadata {
    /// Creates a builder to more easily construct the [HmacKeyMetadata] struct.
    pub fn builder() -> HmacKeyMetadataBuilder {
        Default::default()
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, Default)]
/// A builder used to more easily construct the [HmacKeyMetadata] struct.
pub struct HmacKeyMetadataBuilder {
    access_id: Option<String>,
    etag: Option<String>,
    id: Option<String>,
    kind: Option<String>,
    project_id: Option<String>,
    self_link: Option<String>,
    service_account_email: Option<String>,
    state: Option<String>,
    time_created: Option<String>,
    updated: Option<String>,
}

impl HmacKeyMetadataBuilder {
    /// The ID of the HMAC Key.
    pub fn access_id(mut self, value: impl Into<String>) -> Self {
        self.access_id = Some(value.into());
        self
    }
    /// HTTP 1.1 Entity tag for the HMAC key.
    pub fn etag(mut self, value: impl Into<String>) -> Self {
        self.etag = Some(value.into());
        self
    }
    /// The ID of the HMAC key, including the Project ID and the Access ID.
    pub fn id(mut self, value: impl Into<String>) -> Self {
        self.id = Some(value.into());
        self
    }
    /// The kind of item this is. For HMAC Key metadata, this is always
    /// storage#hmacKeyMetadata.
    pub fn kind(mut self, value: impl Into<String>) -> Self {
        self.kind = Some(value.into());
        self
    }
    /// Project ID owning the service account to which the key authenticates.
    pub fn project_id(mut self, value: impl Into<String>) -> Self {
        self.project_id = Some(value.into());
        self
    }
    /// The link to this resource.
    pub fn self_link(mut self, value: impl Into<String>) -> Self {
        self.self_link = Some(value.into());
        self
    }
    /// The email address of the key's associated service account.
    pub fn service_account_email(mut self, value: impl Into<String>) -> Self {
        self.service_account_email = Some(value.into());
        self
    }
    /// The state of the key. Can be one of ACTIVE, INACTIVE, or DELETED.
    pub fn state(mut self, value: impl Into<String>) -> Self {
        self.state = Some(value.into());
        self
    }
    /// The creation time of the HMAC key in RFC 3339 format.
    pub fn time_created(mut self, value: impl Into<String>) -> Self {
        self.time_created = Some(value.into());
        self
    }
    /// The last modification time of the HMAC key metadata in RFC 3339
    /// format.
    pub fn updated(mut self, value: impl Into<String>) -> Self {
        self.updated = Some(value.into());
        self
    }
    /// Builds [HmacKeyMetadata].
    pub fn build(self) -> HmacKeyMetadata {
        HmacKeyMetadata {
            access_id: self.access_id,
            etag: self.etag,
            id: self.id,
            kind: self.kind,
            project_id: self.project_id,
            self_link: self.self_link,
            service_account_email: self.service_account_email,
            state: self.state,
            time_created: self.time_created,
            updated: self.updated,
        }
    }
}

/// A list of hmacKeys.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct HmacKeysMetadata {
    /// The list of items.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub items: Option<Vec<HmacKeyMetadata>>,
    /// The kind of item this is. For lists of hmacKeys, this is always
    /// storage#hmacKeysMetadata.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// The continuation token, used to page through large result sets.
    /// Provide this value in a subsequent request to return the next page of
    /// results.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
}

impl HmacKeysMetadata {
    /// Creates a builder to more easily construct the [HmacKeysMetadata] struct.
    pub fn builder() -> HmacKeysMetadataBuilder {
        Default::default()
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, Default)]
/// A builder used to more easily construct the [HmacKeysMetadata] struct.
pub struct HmacKeysMetadataBuilder {
    items: Option<Vec<HmacKeyMetadata>>,
    kind: Option<String>,
    next_page_token: Option<String>,
}

impl HmacKeysMetadataBuilder {
    /// The list of items.
    pub fn items(mut self, value: Vec<HmacKeyMetadata>) -> Self {
        self.items = Some(value);
        self
    }
    /// The kind of item this is. For lists of hmacKeys, this is always
    /// storage#hmacKeysMetadata.
    pub fn kind(mut self, value: impl Into<String>) -> Self {
        self.kind = Some(value.into());
        self
    }
    /// The continuation token, used to page through large result sets.
    /// Provide this value in a subsequent request to return the next page of
    /// results.
    pub fn next_page_token(mut self, value: impl Into<String>) -> Self {
        self.next_page_token = Some(value.into());
        self
    }
    /// Builds [HmacKeysMetadata].
    pub fn build(self) -> HmacKeysMetadata {
        HmacKeysMetadata {
            items: self.items,
            kind: self.kind,
            next_page_token: self.next_page_token,
        }
    }
}

/// A subscription to receive Google PubSub notifications.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct Notification {
    /// An optional list of additional attributes to attach to each Cloud
    /// PubSub message published for this notification subscription.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub custom_attributes: Option<std::collections::HashMap<String, String>>,
    /// HTTP 1.1 Entity tag for this subscription notification.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub etag: Option<String>,
    /// If present, only send notifications about listed event types. If
    /// empty, sent notifications for all event types.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_types: Option<Vec<String>>,
    /// The ID of the notification.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// The kind of item this is. For notifications, this is always
    /// storage#notification.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// If present, only apply this notification configuration to object
    /// names that begin with this prefix.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_name_prefix: Option<String>,
    /// The desired content of the Payload.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload_format: Option<String>,
    /// The canonical URL of this notification.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub self_link: Option<String>,
    /// The Cloud PubSub topic to which this subscription publishes.
    /// Formatted as:
    /// '//pubsub.googleapis.com/projects/{project-identifier}/topics/{my-topi
    /// c}'
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
}

impl Notification {
    /// Creates a builder to more easily construct the [Notification] struct.
    pub fn builder() -> NotificationBuilder {
        Default::default()
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, Default)]
/// A builder used to more easily construct the [Notification] struct.
pub struct NotificationBuilder {
    custom_attributes: Option<std::collections::HashMap<String, String>>,
    etag: Option<String>,
    event_types: Option<Vec<String>>,
    id: Option<String>,
    kind: Option<String>,
    object_name_prefix: Option<String>,
    payload_format: Option<String>,
    self_link: Option<String>,
    topic: Option<String>,
}

impl NotificationBuilder {
    /// An optional list of additional attributes to attach to each Cloud
    /// PubSub message published for this notification subscription.
    pub fn custom_attributes(mut self, value: std::collections::HashMap<String, String>) -> Self {
        self.custom_attributes = Some(value);
        self
    }
    /// HTTP 1.1 Entity tag for this subscription notification.
    pub fn etag(mut self, value: impl Into<String>) -> Self {
        self.etag = Some(value.into());
        self
    }
    /// If present, only send notifications about listed event types. If
    /// empty, sent notifications for all event types.
    pub fn event_types(mut self, value: Vec<String>) -> Self {
        self.event_types = Some(value);
        self
    }
    /// The ID of the notification.
    pub fn id(mut self, value: impl Into<String>) -> Self {
        self.id = Some(value.into());
        self
    }
    /// The kind of item this is. For notifications, this is always
    /// storage#notification.
    pub fn kind(mut self, value: impl Into<String>) -> Self {
        self.kind = Some(value.into());
        self
    }
    /// If present, only apply this notification configuration to object
    /// names that begin with this prefix.
    pub fn object_name_prefix(mut self, value: impl Into<String>) -> Self {
        self.object_name_prefix = Some(value.into());
        self
    }
    /// The desired content of the Payload.
    pub fn payload_format(mut self, value: impl Into<String>) -> Self {
        self.payload_format = Some(value.into());
        self
    }
    /// The canonical URL of this notification.
    pub fn self_link(mut self, value: impl Into<String>) -> Self {
        self.self_link = Some(value.into());
        self
    }
    /// The Cloud PubSub topic to which this subscription publishes.
    /// Formatted as:
    /// '//pubsub.googleapis.com/projects/{project-identifier}/topics/{my-topi
    /// c}'
    pub fn topic(mut self, value: impl Into<String>) -> Self {
        self.topic = Some(value.into());
        self
    }
    /// Builds [Notification].
    pub fn build(self) -> Notification {
        Notification {
            custom_attributes: self.custom_attributes,
            etag: self.etag,
            event_types: self.event_types,
            id: self.id,
            kind: self.kind,
            object_name_prefix: self.object_name_prefix,
            payload_format: self.payload_format,
            self_link: self.self_link,
            topic: self.topic,
        }
    }
}

/// A list of notification subscriptions.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct Notifications {
    /// The list of items.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub items: Option<Vec<Notification>>,
    /// The kind of item this is. For lists of notifications, this is always
    /// storage#notifications.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
}

impl Notifications {
    /// Creates a builder to more easily construct the [Notifications] struct.
    pub fn builder() -> NotificationsBuilder {
        Default::default()
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, Default)]
/// A builder used to more easily construct the [Notifications] struct.
pub struct NotificationsBuilder {
    items: Option<Vec<Notification>>,
    kind: Option<String>,
}

impl NotificationsBuilder {
    /// The list of items.
    pub fn items(mut self, value: Vec<Notification>) -> Self {
        self.items = Some(value);
        self
    }
    /// The kind of item this is. For lists of notifications, this is always
    /// storage#notifications.
    pub fn kind(mut self, value: impl Into<String>) -> Self {
        self.kind = Some(value.into());
        self
    }
    /// Builds [Notifications].
    pub fn build(self) -> Notifications {
        Notifications {
            items: self.items,
            kind: self.kind,
        }
    }
}

/// An object.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct Object {
    /// Access controls on the object.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub acl: Option<Vec<ObjectAccessControl>>,
    /// The name of the bucket containing this object.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bucket: Option<String>,
    /// Cache-Control directive for the object data. If omitted, and the
    /// object is accessible to all anonymous users, the default will be
    /// public, max-age=3600.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_control: Option<String>,
    /// Number of underlying components that make up this object. Components
    /// are accumulated by compose operations.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub component_count: Option<i64>,
    /// Content-Disposition of the object data.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_disposition: Option<String>,
    /// Content-Encoding of the object data.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_encoding: Option<String>,
    /// Content-Language of the object data.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_language: Option<String>,
    /// Content-Type of the object data. If an object is stored without a
    /// Content-Type, it is served as application/octet-stream.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    /// CRC32c checksum, as described in RFC 4960, Appendix B; encoded using
    /// base64 in big-endian byte order. For more information about using the
    /// CRC32c checksum, see Hashes and ETags: Best Practices.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub crc32c: Option<String>,
    /// A timestamp in RFC 3339 format specified by the user for an object.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub custom_time: Option<String>,
    /// Metadata of customer-supplied encryption key, if the object is
    /// encrypted by such a key.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub customer_encryption: Option<ObjectCustomerEncryption>,
    /// HTTP 1.1 Entity tag for the object.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub etag: Option<String>,
    /// Whether an object is under event-based hold. Event-based hold is a
    /// way to retain objects until an event occurs, which is signified by
    /// the hold's release (i.e. this value is set to false). After being
    /// released (set to false), such objects will be subject to bucket-level
    /// retention (if any). One sample use case of this flag is for banks to
    /// hold loan documents for at least 3 years after loan is paid in full.
    /// Here, bucket-level retention is 3 years and the event is the loan
    /// being paid in full. In this example, these objects will be held
    /// intact for any number of years until the event has occurred
    /// (event-based hold on the object is released) and then 3 more years
    /// after that. That means retention duration of the objects begins from
    /// the moment event-based hold transitioned from true to false.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_based_hold: Option<bool>,
    /// The content generation of this object. Used for object versioning.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub generation: Option<String>,
    /// The ID of the object, including the bucket name, object name, and
    /// generation number.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// The kind of item this is. For objects, this is always storage#object.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// Not currently supported. Specifying the parameter causes the request
    /// to fail with status code 400 - Bad Request.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kms_key_name: Option<String>,
    /// MD5 hash of the data; encoded using base64. For more information
    /// about using the MD5 hash, see Hashes and ETags: Best Practices.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub md5_hash: Option<String>,
    /// Media download link.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub media_link: Option<String>,
    /// User-provided metadata, in key/value pairs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<std::collections::HashMap<String, String>>,
    /// The version of the metadata for this object at this generation. Used
    /// for preconditions and for detecting changes in metadata. A
    /// metageneration number is only meaningful in the context of a
    /// particular generation of a particular object.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metageneration: Option<String>,
    /// The name of the object. Required if not specified by URL parameter.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// The owner of the object. This will always be the uploader of the
    /// object.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner: Option<ObjectOwner>,
    /// A server-determined value that specifies the earliest time that the
    /// object's retention period expires. This value is in RFC 3339 format.
    /// Note 1: This field is not provided for objects with an active
    /// event-based hold, since retention expiration is unknown until the
    /// hold is removed. Note 2: This value can be provided even when
    /// temporary hold is set (so that the user can reason about policy
    /// without having to first unset the temporary hold).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retention_expiration_time: Option<String>,
    /// The link to this object.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub self_link: Option<String>,
    /// Content-Length of the data in bytes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<String>,
    /// Storage class of the object.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_class: Option<String>,
    /// Whether an object is under temporary hold. While this flag is set to
    /// true, the object is protected against deletion and overwrites. A
    /// common use case of this flag is regulatory investigations where
    /// objects need to be retained while the investigation is ongoing. Note
    /// that unlike event-based hold, temporary hold does not impact
    /// retention expiration time of an object.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temporary_hold: Option<bool>,
    /// The creation time of the object in RFC 3339 format.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_created: Option<String>,
    /// The deletion time of the object in RFC 3339 format. Will be returned
    /// if and only if this version of the object has been deleted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_deleted: Option<String>,
    /// The time at which the object's storage class was last changed. When
    /// the object is initially created, it will be set to timeCreated.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_storage_class_updated: Option<String>,
    /// The modification time of the object metadata in RFC 3339 format.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated: Option<String>,
}

impl Object {
    /// Creates a builder to more easily construct the [Object] struct.
    pub fn builder() -> ObjectBuilder {
        Default::default()
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, Default)]
/// A builder used to more easily construct the [Object] struct.
pub struct ObjectBuilder {
    acl: Option<Vec<ObjectAccessControl>>,
    bucket: Option<String>,
    cache_control: Option<String>,
    component_count: Option<i64>,
    content_disposition: Option<String>,
    content_encoding: Option<String>,
    content_language: Option<String>,
    content_type: Option<String>,
    crc32c: Option<String>,
    custom_time: Option<String>,
    customer_encryption: Option<ObjectCustomerEncryption>,
    etag: Option<String>,
    event_based_hold: Option<bool>,
    generation: Option<String>,
    id: Option<String>,
    kind: Option<String>,
    kms_key_name: Option<String>,
    md5_hash: Option<String>,
    media_link: Option<String>,
    metadata: Option<std::collections::HashMap<String, String>>,
    metageneration: Option<String>,
    name: Option<String>,
    owner: Option<ObjectOwner>,
    retention_expiration_time: Option<String>,
    self_link: Option<String>,
    size: Option<String>,
    storage_class: Option<String>,
    temporary_hold: Option<bool>,
    time_created: Option<String>,
    time_deleted: Option<String>,
    time_storage_class_updated: Option<String>,
    updated: Option<String>,
}

impl ObjectBuilder {
    /// Access controls on the object.
    pub fn acl(mut self, value: Vec<ObjectAccessControl>) -> Self {
        self.acl = Some(value);
        self
    }
    /// The name of the bucket containing this object.
    pub fn bucket(mut self, value: impl Into<String>) -> Self {
        self.bucket = Some(value.into());
        self
    }
    /// Cache-Control directive for the object data. If omitted, and the
    /// object is accessible to all anonymous users, the default will be
    /// public, max-age=3600.
    pub fn cache_control(mut self, value: impl Into<String>) -> Self {
        self.cache_control = Some(value.into());
        self
    }
    /// Number of underlying components that make up this object. Components
    /// are accumulated by compose operations.
    pub fn component_count(mut self, value: impl Into<i64>) -> Self {
        self.component_count = Some(value.into());
        self
    }
    /// Content-Disposition of the object data.
    pub fn content_disposition(mut self, value: impl Into<String>) -> Self {
        self.content_disposition = Some(value.into());
        self
    }
    /// Content-Encoding of the object data.
    pub fn content_encoding(mut self, value: impl Into<String>) -> Self {
        self.content_encoding = Some(value.into());
        self
    }
    /// Content-Language of the object data.
    pub fn content_language(mut self, value: impl Into<String>) -> Self {
        self.content_language = Some(value.into());
        self
    }
    /// Content-Type of the object data. If an object is stored without a
    /// Content-Type, it is served as application/octet-stream.
    pub fn content_type(mut self, value: impl Into<String>) -> Self {
        self.content_type = Some(value.into());
        self
    }
    /// CRC32c checksum, as described in RFC 4960, Appendix B; encoded using
    /// base64 in big-endian byte order. For more information about using the
    /// CRC32c checksum, see Hashes and ETags: Best Practices.
    pub fn crc32c(mut self, value: impl Into<String>) -> Self {
        self.crc32c = Some(value.into());
        self
    }
    /// A timestamp in RFC 3339 format specified by the user for an object.
    pub fn custom_time(mut self, value: impl Into<String>) -> Self {
        self.custom_time = Some(value.into());
        self
    }
    /// Metadata of customer-supplied encryption key, if the object is
    /// encrypted by such a key.
    pub fn customer_encryption(mut self, value: ObjectCustomerEncryption) -> Self {
        self.customer_encryption = Some(value);
        self
    }
    /// HTTP 1.1 Entity tag for the object.
    pub fn etag(mut self, value: impl Into<String>) -> Self {
        self.etag = Some(value.into());
        self
    }
    /// Whether an object is under event-based hold. Event-based hold is a
    /// way to retain objects until an event occurs, which is signified by
    /// the hold's release (i.e. this value is set to false). After being
    /// released (set to false), such objects will be subject to bucket-level
    /// retention (if any). One sample use case of this flag is for banks to
    /// hold loan documents for at least 3 years after loan is paid in full.
    /// Here, bucket-level retention is 3 years and the event is the loan
    /// being paid in full. In this example, these objects will be held
    /// intact for any number of years until the event has occurred
    /// (event-based hold on the object is released) and then 3 more years
    /// after that. That means retention duration of the objects begins from
    /// the moment event-based hold transitioned from true to false.
    pub fn event_based_hold(mut self, value: bool) -> Self {
        self.event_based_hold = Some(value);
        self
    }
    /// The content generation of this object. Used for object versioning.
    pub fn generation(mut self, value: impl Into<String>) -> Self {
        self.generation = Some(value.into());
        self
    }
    /// The ID of the object, including the bucket name, object name, and
    /// generation number.
    pub fn id(mut self, value: impl Into<String>) -> Self {
        self.id = Some(value.into());
        self
    }
    /// The kind of item this is. For objects, this is always storage#object.
    pub fn kind(mut self, value: impl Into<String>) -> Self {
        self.kind = Some(value.into());
        self
    }
    /// Not currently supported. Specifying the parameter causes the request
    /// to fail with status code 400 - Bad Request.
    pub fn kms_key_name(mut self, value: impl Into<String>) -> Self {
        self.kms_key_name = Some(value.into());
        self
    }
    /// MD5 hash of the data; encoded using base64. For more information
    /// about using the MD5 hash, see Hashes and ETags: Best Practices.
    pub fn md5_hash(mut self, value: impl Into<String>) -> Self {
        self.md5_hash = Some(value.into());
        self
    }
    /// Media download link.
    pub fn media_link(mut self, value: impl Into<String>) -> Self {
        self.media_link = Some(value.into());
        self
    }
    /// User-provided metadata, in key/value pairs.
    pub fn metadata(mut self, value: std::collections::HashMap<String, String>) -> Self {
        self.metadata = Some(value);
        self
    }
    /// The version of the metadata for this object at this generation. Used
    /// for preconditions and for detecting changes in metadata. A
    /// metageneration number is only meaningful in the context of a
    /// particular generation of a particular object.
    pub fn metageneration(mut self, value: impl Into<String>) -> Self {
        self.metageneration = Some(value.into());
        self
    }
    /// The name of the object. Required if not specified by URL parameter.
    pub fn name(mut self, value: impl Into<String>) -> Self {
        self.name = Some(value.into());
        self
    }
    /// The owner of the object. This will always be the uploader of the
    /// object.
    pub fn owner(mut self, value: ObjectOwner) -> Self {
        self.owner = Some(value);
        self
    }
    /// A server-determined value that specifies the earliest time that the
    /// object's retention period expires. This value is in RFC 3339 format.
    /// Note 1: This field is not provided for objects with an active
    /// event-based hold, since retention expiration is unknown until the
    /// hold is removed. Note 2: This value can be provided even when
    /// temporary hold is set (so that the user can reason about policy
    /// without having to first unset the temporary hold).
    pub fn retention_expiration_time(mut self, value: impl Into<String>) -> Self {
        self.retention_expiration_time = Some(value.into());
        self
    }
    /// The link to this object.
    pub fn self_link(mut self, value: impl Into<String>) -> Self {
        self.self_link = Some(value.into());
        self
    }
    /// Content-Length of the data in bytes.
    pub fn size(mut self, value: impl Into<String>) -> Self {
        self.size = Some(value.into());
        self
    }
    /// Storage class of the object.
    pub fn storage_class(mut self, value: impl Into<String>) -> Self {
        self.storage_class = Some(value.into());
        self
    }
    /// Whether an object is under temporary hold. While this flag is set to
    /// true, the object is protected against deletion and overwrites. A
    /// common use case of this flag is regulatory investigations where
    /// objects need to be retained while the investigation is ongoing. Note
    /// that unlike event-based hold, temporary hold does not impact
    /// retention expiration time of an object.
    pub fn temporary_hold(mut self, value: bool) -> Self {
        self.temporary_hold = Some(value);
        self
    }
    /// The creation time of the object in RFC 3339 format.
    pub fn time_created(mut self, value: impl Into<String>) -> Self {
        self.time_created = Some(value.into());
        self
    }
    /// The deletion time of the object in RFC 3339 format. Will be returned
    /// if and only if this version of the object has been deleted.
    pub fn time_deleted(mut self, value: impl Into<String>) -> Self {
        self.time_deleted = Some(value.into());
        self
    }
    /// The time at which the object's storage class was last changed. When
    /// the object is initially created, it will be set to timeCreated.
    pub fn time_storage_class_updated(mut self, value: impl Into<String>) -> Self {
        self.time_storage_class_updated = Some(value.into());
        self
    }
    /// The modification time of the object metadata in RFC 3339 format.
    pub fn updated(mut self, value: impl Into<String>) -> Self {
        self.updated = Some(value.into());
        self
    }
    /// Builds [Object].
    pub fn build(self) -> Object {
        Object {
            acl: self.acl,
            bucket: self.bucket,
            cache_control: self.cache_control,
            component_count: self.component_count,
            content_disposition: self.content_disposition,
            content_encoding: self.content_encoding,
            content_language: self.content_language,
            content_type: self.content_type,
            crc32c: self.crc32c,
            custom_time: self.custom_time,
            customer_encryption: self.customer_encryption,
            etag: self.etag,
            event_based_hold: self.event_based_hold,
            generation: self.generation,
            id: self.id,
            kind: self.kind,
            kms_key_name: self.kms_key_name,
            md5_hash: self.md5_hash,
            media_link: self.media_link,
            metadata: self.metadata,
            metageneration: self.metageneration,
            name: self.name,
            owner: self.owner,
            retention_expiration_time: self.retention_expiration_time,
            self_link: self.self_link,
            size: self.size,
            storage_class: self.storage_class,
            temporary_hold: self.temporary_hold,
            time_created: self.time_created,
            time_deleted: self.time_deleted,
            time_storage_class_updated: self.time_storage_class_updated,
            updated: self.updated,
        }
    }
}

/// An access-control entry.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct ObjectAccessControl {
    /// The name of the bucket.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bucket: Option<String>,
    /// The domain associated with the entity, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub domain: Option<String>,
    /// The email address associated with the entity, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    /// The entity holding the permission, in one of the following forms:
    /// - user-userId
    /// - user-email
    /// - group-groupId
    /// - group-email
    /// - domain-domain
    /// - project-team-projectId
    /// - allUsers
    /// - allAuthenticatedUsers Examples:
    /// - The user liz@example.com would be user-liz@example.com.
    /// - The group example@googlegroups.com would be
    /// group-example@googlegroups.com.
    /// - To refer to all members of the Google Apps for Business domain
    /// example.com, the entity would be domain-example.com.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entity: Option<String>,
    /// The ID for the entity, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entity_id: Option<String>,
    /// HTTP 1.1 Entity tag for the access-control entry.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub etag: Option<String>,
    /// The content generation of the object, if applied to an object.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub generation: Option<String>,
    /// The ID of the access-control entry.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// The kind of item this is. For object access control entries, this is
    /// always storage#objectAccessControl.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// The name of the object, if applied to an object.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object: Option<String>,
    /// The project team associated with the entity, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_team: Option<ObjectAccessControlProjectTeam>,
    /// The access permission for the entity.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    /// The link to this access-control entry.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub self_link: Option<String>,
}

impl ObjectAccessControl {
    /// Creates a builder to more easily construct the [ObjectAccessControl] struct.
    pub fn builder() -> ObjectAccessControlBuilder {
        Default::default()
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, Default)]
/// A builder used to more easily construct the [ObjectAccessControl] struct.
pub struct ObjectAccessControlBuilder {
    bucket: Option<String>,
    domain: Option<String>,
    email: Option<String>,
    entity: Option<String>,
    entity_id: Option<String>,
    etag: Option<String>,
    generation: Option<String>,
    id: Option<String>,
    kind: Option<String>,
    object: Option<String>,
    project_team: Option<ObjectAccessControlProjectTeam>,
    role: Option<String>,
    self_link: Option<String>,
}

impl ObjectAccessControlBuilder {
    /// The name of the bucket.
    pub fn bucket(mut self, value: impl Into<String>) -> Self {
        self.bucket = Some(value.into());
        self
    }
    /// The domain associated with the entity, if any.
    pub fn domain(mut self, value: impl Into<String>) -> Self {
        self.domain = Some(value.into());
        self
    }
    /// The email address associated with the entity, if any.
    pub fn email(mut self, value: impl Into<String>) -> Self {
        self.email = Some(value.into());
        self
    }
    /// The entity holding the permission, in one of the following forms:
    /// - user-userId
    /// - user-email
    /// - group-groupId
    /// - group-email
    /// - domain-domain
    /// - project-team-projectId
    /// - allUsers
    /// - allAuthenticatedUsers Examples:
    /// - The user liz@example.com would be user-liz@example.com.
    /// - The group example@googlegroups.com would be
    /// group-example@googlegroups.com.
    /// - To refer to all members of the Google Apps for Business domain
    /// example.com, the entity would be domain-example.com.
    pub fn entity(mut self, value: impl Into<String>) -> Self {
        self.entity = Some(value.into());
        self
    }
    /// The ID for the entity, if any.
    pub fn entity_id(mut self, value: impl Into<String>) -> Self {
        self.entity_id = Some(value.into());
        self
    }
    /// HTTP 1.1 Entity tag for the access-control entry.
    pub fn etag(mut self, value: impl Into<String>) -> Self {
        self.etag = Some(value.into());
        self
    }
    /// The content generation of the object, if applied to an object.
    pub fn generation(mut self, value: impl Into<String>) -> Self {
        self.generation = Some(value.into());
        self
    }
    /// The ID of the access-control entry.
    pub fn id(mut self, value: impl Into<String>) -> Self {
        self.id = Some(value.into());
        self
    }
    /// The kind of item this is. For object access control entries, this is
    /// always storage#objectAccessControl.
    pub fn kind(mut self, value: impl Into<String>) -> Self {
        self.kind = Some(value.into());
        self
    }
    /// The name of the object, if applied to an object.
    pub fn object(mut self, value: impl Into<String>) -> Self {
        self.object = Some(value.into());
        self
    }
    /// The project team associated with the entity, if any.
    pub fn project_team(mut self, value: ObjectAccessControlProjectTeam) -> Self {
        self.project_team = Some(value);
        self
    }
    /// The access permission for the entity.
    pub fn role(mut self, value: impl Into<String>) -> Self {
        self.role = Some(value.into());
        self
    }
    /// The link to this access-control entry.
    pub fn self_link(mut self, value: impl Into<String>) -> Self {
        self.self_link = Some(value.into());
        self
    }
    /// Builds [ObjectAccessControl].
    pub fn build(self) -> ObjectAccessControl {
        ObjectAccessControl {
            bucket: self.bucket,
            domain: self.domain,
            email: self.email,
            entity: self.entity,
            entity_id: self.entity_id,
            etag: self.etag,
            generation: self.generation,
            id: self.id,
            kind: self.kind,
            object: self.object,
            project_team: self.project_team,
            role: self.role,
            self_link: self.self_link,
        }
    }
}

/// The project team associated with the entity, if any.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct ObjectAccessControlProjectTeam {
    /// The project number.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_number: Option<String>,
    /// The team.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub team: Option<String>,
}

impl ObjectAccessControlProjectTeam {
    /// Creates a builder to more easily construct the [ObjectAccessControlProjectTeam] struct.
    pub fn builder() -> ObjectAccessControlProjectTeamBuilder {
        Default::default()
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, Default)]
/// A builder used to more easily construct the [ObjectAccessControlProjectTeam] struct.
pub struct ObjectAccessControlProjectTeamBuilder {
    project_number: Option<String>,
    team: Option<String>,
}

impl ObjectAccessControlProjectTeamBuilder {
    /// The project number.
    pub fn project_number(mut self, value: impl Into<String>) -> Self {
        self.project_number = Some(value.into());
        self
    }
    /// The team.
    pub fn team(mut self, value: impl Into<String>) -> Self {
        self.team = Some(value.into());
        self
    }
    /// Builds [ObjectAccessControlProjectTeam].
    pub fn build(self) -> ObjectAccessControlProjectTeam {
        ObjectAccessControlProjectTeam {
            project_number: self.project_number,
            team: self.team,
        }
    }
}

/// An access-control list.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct ObjectAccessControls {
    /// The list of items.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub items: Option<Vec<ObjectAccessControl>>,
    /// The kind of item this is. For lists of object access control entries,
    /// this is always storage#objectAccessControls.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
}

impl ObjectAccessControls {
    /// Creates a builder to more easily construct the [ObjectAccessControls] struct.
    pub fn builder() -> ObjectAccessControlsBuilder {
        Default::default()
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, Default)]
/// A builder used to more easily construct the [ObjectAccessControls] struct.
pub struct ObjectAccessControlsBuilder {
    items: Option<Vec<ObjectAccessControl>>,
    kind: Option<String>,
}

impl ObjectAccessControlsBuilder {
    /// The list of items.
    pub fn items(mut self, value: Vec<ObjectAccessControl>) -> Self {
        self.items = Some(value);
        self
    }
    /// The kind of item this is. For lists of object access control entries,
    /// this is always storage#objectAccessControls.
    pub fn kind(mut self, value: impl Into<String>) -> Self {
        self.kind = Some(value.into());
        self
    }
    /// Builds [ObjectAccessControls].
    pub fn build(self) -> ObjectAccessControls {
        ObjectAccessControls {
            items: self.items,
            kind: self.kind,
        }
    }
}

/// Metadata of customer-supplied encryption key, if the object is
/// encrypted by such a key.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct ObjectCustomerEncryption {
    /// The encryption algorithm.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encryption_algorithm: Option<String>,
    /// SHA256 hash value of the encryption key.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_sha256: Option<String>,
}

impl ObjectCustomerEncryption {
    /// Creates a builder to more easily construct the [ObjectCustomerEncryption] struct.
    pub fn builder() -> ObjectCustomerEncryptionBuilder {
        Default::default()
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, Default)]
/// A builder used to more easily construct the [ObjectCustomerEncryption] struct.
pub struct ObjectCustomerEncryptionBuilder {
    encryption_algorithm: Option<String>,
    key_sha256: Option<String>,
}

impl ObjectCustomerEncryptionBuilder {
    /// The encryption algorithm.
    pub fn encryption_algorithm(mut self, value: impl Into<String>) -> Self {
        self.encryption_algorithm = Some(value.into());
        self
    }
    /// SHA256 hash value of the encryption key.
    pub fn key_sha256(mut self, value: impl Into<String>) -> Self {
        self.key_sha256 = Some(value.into());
        self
    }
    /// Builds [ObjectCustomerEncryption].
    pub fn build(self) -> ObjectCustomerEncryption {
        ObjectCustomerEncryption {
            encryption_algorithm: self.encryption_algorithm,
            key_sha256: self.key_sha256,
        }
    }
}

/// The owner of the object. This will always be the uploader of the
/// object.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct ObjectOwner {
    /// The entity, in the form user-userId.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entity: Option<String>,
    /// The ID for the entity.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entity_id: Option<String>,
}

impl ObjectOwner {
    /// Creates a builder to more easily construct the [ObjectOwner] struct.
    pub fn builder() -> ObjectOwnerBuilder {
        Default::default()
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, Default)]
/// A builder used to more easily construct the [ObjectOwner] struct.
pub struct ObjectOwnerBuilder {
    entity: Option<String>,
    entity_id: Option<String>,
}

impl ObjectOwnerBuilder {
    /// The entity, in the form user-userId.
    pub fn entity(mut self, value: impl Into<String>) -> Self {
        self.entity = Some(value.into());
        self
    }
    /// The ID for the entity.
    pub fn entity_id(mut self, value: impl Into<String>) -> Self {
        self.entity_id = Some(value.into());
        self
    }
    /// Builds [ObjectOwner].
    pub fn build(self) -> ObjectOwner {
        ObjectOwner {
            entity: self.entity,
            entity_id: self.entity_id,
        }
    }
}

/// A list of objects.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct Objects {
    /// The list of items.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub items: Option<Vec<Object>>,
    /// The kind of item this is. For lists of objects, this is always
    /// storage#objects.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// The continuation token, used to page through large result sets.
    /// Provide this value in a subsequent request to return the next page of
    /// results.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
    /// The list of prefixes of objects matching-but-not-listed up to and
    /// including the requested delimiter.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefixes: Option<Vec<String>>,
}

impl Objects {
    /// Creates a builder to more easily construct the [Objects] struct.
    pub fn builder() -> ObjectsBuilder {
        Default::default()
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, Default)]
/// A builder used to more easily construct the [Objects] struct.
pub struct ObjectsBuilder {
    items: Option<Vec<Object>>,
    kind: Option<String>,
    next_page_token: Option<String>,
    prefixes: Option<Vec<String>>,
}

impl ObjectsBuilder {
    /// The list of items.
    pub fn items(mut self, value: Vec<Object>) -> Self {
        self.items = Some(value);
        self
    }
    /// The kind of item this is. For lists of objects, this is always
    /// storage#objects.
    pub fn kind(mut self, value: impl Into<String>) -> Self {
        self.kind = Some(value.into());
        self
    }
    /// The continuation token, used to page through large result sets.
    /// Provide this value in a subsequent request to return the next page of
    /// results.
    pub fn next_page_token(mut self, value: impl Into<String>) -> Self {
        self.next_page_token = Some(value.into());
        self
    }
    /// The list of prefixes of objects matching-but-not-listed up to and
    /// including the requested delimiter.
    pub fn prefixes(mut self, value: Vec<String>) -> Self {
        self.prefixes = Some(value);
        self
    }
    /// Builds [Objects].
    pub fn build(self) -> Objects {
        Objects {
            items: self.items,
            kind: self.kind,
            next_page_token: self.next_page_token,
            prefixes: self.prefixes,
        }
    }
}

/// A bucket/object IAM policy.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct Policy {
    /// An association between a role, which comes with a set of permissions,
    /// and members who may assume that role.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bindings: Option<Vec<PolicyBindings>>,
    /// HTTP 1.1  Entity tag for the policy.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub etag: Option<String>,
    /// The kind of item this is. For policies, this is always
    /// storage#policy. This field is ignored on input.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// The ID of the resource to which this policy belongs. Will be of the
    /// form projects/_/buckets/bucket for buckets, and
    /// projects/_/buckets/bucket/objects/object for objects. A specific
    /// generation may be specified by appending #generationNumber to the end
    /// of the object name, e.g.
    /// projects/_/buckets/my-bucket/objects/data.txt#17. The current
    /// generation can be denoted with #0. This field is ignored on input.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_id: Option<String>,
    /// The IAM policy format version.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<i64>,
}

impl Policy {
    /// Creates a builder to more easily construct the [Policy] struct.
    pub fn builder() -> PolicyBuilder {
        Default::default()
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, Default)]
/// A builder used to more easily construct the [Policy] struct.
pub struct PolicyBuilder {
    bindings: Option<Vec<PolicyBindings>>,
    etag: Option<String>,
    kind: Option<String>,
    resource_id: Option<String>,
    version: Option<i64>,
}

impl PolicyBuilder {
    /// An association between a role, which comes with a set of permissions,
    /// and members who may assume that role.
    pub fn bindings(mut self, value: Vec<PolicyBindings>) -> Self {
        self.bindings = Some(value);
        self
    }
    /// HTTP 1.1  Entity tag for the policy.
    pub fn etag(mut self, value: impl Into<String>) -> Self {
        self.etag = Some(value.into());
        self
    }
    /// The kind of item this is. For policies, this is always
    /// storage#policy. This field is ignored on input.
    pub fn kind(mut self, value: impl Into<String>) -> Self {
        self.kind = Some(value.into());
        self
    }
    /// The ID of the resource to which this policy belongs. Will be of the
    /// form projects/_/buckets/bucket for buckets, and
    /// projects/_/buckets/bucket/objects/object for objects. A specific
    /// generation may be specified by appending #generationNumber to the end
    /// of the object name, e.g.
    /// projects/_/buckets/my-bucket/objects/data.txt#17. The current
    /// generation can be denoted with #0. This field is ignored on input.
    pub fn resource_id(mut self, value: impl Into<String>) -> Self {
        self.resource_id = Some(value.into());
        self
    }
    /// The IAM policy format version.
    pub fn version(mut self, value: impl Into<i64>) -> Self {
        self.version = Some(value.into());
        self
    }
    /// Builds [Policy].
    pub fn build(self) -> Policy {
        Policy {
            bindings: self.bindings,
            etag: self.etag,
            kind: self.kind,
            resource_id: self.resource_id,
            version: self.version,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct PolicyBindings {
    /// The condition that is associated with this binding. NOTE: an
    /// unsatisfied condition will not allow user access via current binding.
    /// Different bindings, including their conditions, are examined
    /// independently.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub condition: Option<Expr>,
    /// A collection of identifiers for members who may assume the provided
    /// role. Recognized identifiers are as follows:  
    /// - allUsers — A special identifier that represents anyone on the
    /// internet; with or without a Google account.  
    /// - allAuthenticatedUsers — A special identifier that represents anyone
    /// who is authenticated with a Google account or a service account.  
    /// - user:emailid — An email address that represents a specific account.
    /// For example, user:alice@gmail.com or user:joe@example.com.  
    /// - serviceAccount:emailid — An email address that represents a service
    /// account. For example,
    /// serviceAccount:my-other-app@appspot.gserviceaccount.com .  
    /// - group:emailid — An email address that represents a Google group.
    /// For example, group:admins@example.com.  
    /// - domain:domain — A Google Apps domain name that represents all the
    /// users of that domain. For example, domain:google.com or
    /// domain:example.com.  
    /// - projectOwner:projectid — Owners of the given project. For example,
    /// projectOwner:my-example-project  
    /// - projectEditor:projectid — Editors of the given project. For
    /// example, projectEditor:my-example-project  
    /// - projectViewer:projectid — Viewers of the given project. For
    /// example, projectViewer:my-example-project
    #[serde(skip_serializing_if = "Option::is_none")]
    pub members: Option<Vec<String>>,
    /// The role to which members belong. Two types of roles are supported:
    /// new IAM roles, which grant permissions that do not map directly to
    /// those provided by ACLs, and legacy IAM roles, which do map directly
    /// to ACL permissions. All roles are of the format
    /// roles/storage.specificRole.
    /// The new IAM roles are:  
    /// - roles/storage.admin — Full control of Google Cloud Storage
    /// resources.  
    /// - roles/storage.objectViewer — Read-Only access to Google Cloud
    /// Storage objects.  
    /// - roles/storage.objectCreator — Access to create objects in Google
    /// Cloud Storage.  
    /// - roles/storage.objectAdmin — Full control of Google Cloud Storage
    /// objects.   The legacy IAM roles are:  
    /// - roles/storage.legacyObjectReader — Read-only access to objects
    /// without listing. Equivalent to an ACL entry on an object with the
    /// READER role.  
    /// - roles/storage.legacyObjectOwner — Read/write access to existing
    /// objects without listing. Equivalent to an ACL entry on an object with
    /// the OWNER role.  
    /// - roles/storage.legacyBucketReader — Read access to buckets with
    /// object listing. Equivalent to an ACL entry on a bucket with the
    /// READER role.  
    /// - roles/storage.legacyBucketWriter — Read access to buckets with
    /// object listing/creation/deletion. Equivalent to an ACL entry on a
    /// bucket with the WRITER role.  
    /// - roles/storage.legacyBucketOwner — Read and write access to existing
    /// buckets with object listing/creation/deletion. Equivalent to an ACL
    /// entry on a bucket with the OWNER role.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
}

impl PolicyBindings {
    /// Creates a builder to more easily construct the [PolicyBindings] struct.
    pub fn builder() -> PolicyBindingsBuilder {
        Default::default()
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, Default)]
/// A builder used to more easily construct the [PolicyBindings] struct.
pub struct PolicyBindingsBuilder {
    condition: Option<Expr>,
    members: Option<Vec<String>>,
    role: Option<String>,
}

impl PolicyBindingsBuilder {
    /// The condition that is associated with this binding. NOTE: an
    /// unsatisfied condition will not allow user access via current binding.
    /// Different bindings, including their conditions, are examined
    /// independently.
    pub fn condition(mut self, value: Expr) -> Self {
        self.condition = Some(value);
        self
    }
    /// A collection of identifiers for members who may assume the provided
    /// role. Recognized identifiers are as follows:  
    /// - allUsers — A special identifier that represents anyone on the
    /// internet; with or without a Google account.  
    /// - allAuthenticatedUsers — A special identifier that represents anyone
    /// who is authenticated with a Google account or a service account.  
    /// - user:emailid — An email address that represents a specific account.
    /// For example, user:alice@gmail.com or user:joe@example.com.  
    /// - serviceAccount:emailid — An email address that represents a service
    /// account. For example,
    /// serviceAccount:my-other-app@appspot.gserviceaccount.com .  
    /// - group:emailid — An email address that represents a Google group.
    /// For example, group:admins@example.com.  
    /// - domain:domain — A Google Apps domain name that represents all the
    /// users of that domain. For example, domain:google.com or
    /// domain:example.com.  
    /// - projectOwner:projectid — Owners of the given project. For example,
    /// projectOwner:my-example-project  
    /// - projectEditor:projectid — Editors of the given project. For
    /// example, projectEditor:my-example-project  
    /// - projectViewer:projectid — Viewers of the given project. For
    /// example, projectViewer:my-example-project
    pub fn members(mut self, value: Vec<String>) -> Self {
        self.members = Some(value);
        self
    }
    /// The role to which members belong. Two types of roles are supported:
    /// new IAM roles, which grant permissions that do not map directly to
    /// those provided by ACLs, and legacy IAM roles, which do map directly
    /// to ACL permissions. All roles are of the format
    /// roles/storage.specificRole.
    /// The new IAM roles are:  
    /// - roles/storage.admin — Full control of Google Cloud Storage
    /// resources.  
    /// - roles/storage.objectViewer — Read-Only access to Google Cloud
    /// Storage objects.  
    /// - roles/storage.objectCreator — Access to create objects in Google
    /// Cloud Storage.  
    /// - roles/storage.objectAdmin — Full control of Google Cloud Storage
    /// objects.   The legacy IAM roles are:  
    /// - roles/storage.legacyObjectReader — Read-only access to objects
    /// without listing. Equivalent to an ACL entry on an object with the
    /// READER role.  
    /// - roles/storage.legacyObjectOwner — Read/write access to existing
    /// objects without listing. Equivalent to an ACL entry on an object with
    /// the OWNER role.  
    /// - roles/storage.legacyBucketReader — Read access to buckets with
    /// object listing. Equivalent to an ACL entry on a bucket with the
    /// READER role.  
    /// - roles/storage.legacyBucketWriter — Read access to buckets with
    /// object listing/creation/deletion. Equivalent to an ACL entry on a
    /// bucket with the WRITER role.  
    /// - roles/storage.legacyBucketOwner — Read and write access to existing
    /// buckets with object listing/creation/deletion. Equivalent to an ACL
    /// entry on a bucket with the OWNER role.
    pub fn role(mut self, value: impl Into<String>) -> Self {
        self.role = Some(value.into());
        self
    }
    /// Builds [PolicyBindings].
    pub fn build(self) -> PolicyBindings {
        PolicyBindings {
            condition: self.condition,
            members: self.members,
            role: self.role,
        }
    }
}

/// A rewrite response.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct RewriteResponse {
    /// true if the copy is finished; otherwise, false if the copy is in
    /// progress. This property is always present in the response.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub done: Option<bool>,
    /// The kind of item this is.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// The total size of the object being copied in bytes. This property is
    /// always present in the response.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_size: Option<String>,
    /// A resource containing the metadata for the copied-to object. This
    /// property is present in the response only when copying completes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource: Option<Object>,
    /// A token to use in subsequent requests to continue copying data. This
    /// token is present in the response only when there is more data to
    /// copy.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rewrite_token: Option<String>,
    /// The total bytes written so far, which can be used to provide a
    /// waiting user with a progress indicator. This property is always
    /// present in the response.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_bytes_rewritten: Option<String>,
}

impl RewriteResponse {
    /// Creates a builder to more easily construct the [RewriteResponse] struct.
    pub fn builder() -> RewriteResponseBuilder {
        Default::default()
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, Default)]
/// A builder used to more easily construct the [RewriteResponse] struct.
pub struct RewriteResponseBuilder {
    done: Option<bool>,
    kind: Option<String>,
    object_size: Option<String>,
    resource: Option<Object>,
    rewrite_token: Option<String>,
    total_bytes_rewritten: Option<String>,
}

impl RewriteResponseBuilder {
    /// true if the copy is finished; otherwise, false if the copy is in
    /// progress. This property is always present in the response.
    pub fn done(mut self, value: bool) -> Self {
        self.done = Some(value);
        self
    }
    /// The kind of item this is.
    pub fn kind(mut self, value: impl Into<String>) -> Self {
        self.kind = Some(value.into());
        self
    }
    /// The total size of the object being copied in bytes. This property is
    /// always present in the response.
    pub fn object_size(mut self, value: impl Into<String>) -> Self {
        self.object_size = Some(value.into());
        self
    }
    /// A resource containing the metadata for the copied-to object. This
    /// property is present in the response only when copying completes.
    pub fn resource(mut self, value: Object) -> Self {
        self.resource = Some(value);
        self
    }
    /// A token to use in subsequent requests to continue copying data. This
    /// token is present in the response only when there is more data to
    /// copy.
    pub fn rewrite_token(mut self, value: impl Into<String>) -> Self {
        self.rewrite_token = Some(value.into());
        self
    }
    /// The total bytes written so far, which can be used to provide a
    /// waiting user with a progress indicator. This property is always
    /// present in the response.
    pub fn total_bytes_rewritten(mut self, value: impl Into<String>) -> Self {
        self.total_bytes_rewritten = Some(value.into());
        self
    }
    /// Builds [RewriteResponse].
    pub fn build(self) -> RewriteResponse {
        RewriteResponse {
            done: self.done,
            kind: self.kind,
            object_size: self.object_size,
            resource: self.resource,
            rewrite_token: self.rewrite_token,
            total_bytes_rewritten: self.total_bytes_rewritten,
        }
    }
}

/// A subscription to receive Google PubSub notifications.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct ServiceAccount {
    /// The ID of the notification.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub email_address: Option<String>,
    /// The kind of item this is. For notifications, this is always
    /// storage#notification.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
}

impl ServiceAccount {
    /// Creates a builder to more easily construct the [ServiceAccount] struct.
    pub fn builder() -> ServiceAccountBuilder {
        Default::default()
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, Default)]
/// A builder used to more easily construct the [ServiceAccount] struct.
pub struct ServiceAccountBuilder {
    email_address: Option<String>,
    kind: Option<String>,
}

impl ServiceAccountBuilder {
    /// The ID of the notification.
    pub fn email_address(mut self, value: impl Into<String>) -> Self {
        self.email_address = Some(value.into());
        self
    }
    /// The kind of item this is. For notifications, this is always
    /// storage#notification.
    pub fn kind(mut self, value: impl Into<String>) -> Self {
        self.kind = Some(value.into());
        self
    }
    /// Builds [ServiceAccount].
    pub fn build(self) -> ServiceAccount {
        ServiceAccount {
            email_address: self.email_address,
            kind: self.kind,
        }
    }
}

/// A storage.(buckets|objects).testIamPermissions response.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct TestIamPermissionsResponse {
    /// The kind of item this is.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// The permissions held by the caller. Permissions are always of the
    /// format storage.resource.capability, where resource is one of buckets
    /// or objects. The supported permissions are as follows:  
    /// - storage.buckets.delete — Delete bucket.  
    /// - storage.buckets.get — Read bucket metadata.  
    /// - storage.buckets.getIamPolicy — Read bucket IAM policy.  
    /// - storage.buckets.create — Create bucket.  
    /// - storage.buckets.list — List buckets.  
    /// - storage.buckets.setIamPolicy — Update bucket IAM policy.  
    /// - storage.buckets.update — Update bucket metadata.  
    /// - storage.objects.delete — Delete object.  
    /// - storage.objects.get — Read object data and metadata.  
    /// - storage.objects.getIamPolicy — Read object IAM policy.  
    /// - storage.objects.create — Create object.  
    /// - storage.objects.list — List objects.  
    /// - storage.objects.setIamPolicy — Update object IAM policy.  
    /// - storage.objects.update — Update object metadata.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub permissions: Option<Vec<String>>,
}

impl TestIamPermissionsResponse {
    /// Creates a builder to more easily construct the [TestIamPermissionsResponse] struct.
    pub fn builder() -> TestIamPermissionsResponseBuilder {
        Default::default()
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, Default)]
/// A builder used to more easily construct the [TestIamPermissionsResponse] struct.
pub struct TestIamPermissionsResponseBuilder {
    kind: Option<String>,
    permissions: Option<Vec<String>>,
}

impl TestIamPermissionsResponseBuilder {
    /// The kind of item this is.
    pub fn kind(mut self, value: impl Into<String>) -> Self {
        self.kind = Some(value.into());
        self
    }
    /// The permissions held by the caller. Permissions are always of the
    /// format storage.resource.capability, where resource is one of buckets
    /// or objects. The supported permissions are as follows:  
    /// - storage.buckets.delete — Delete bucket.  
    /// - storage.buckets.get — Read bucket metadata.  
    /// - storage.buckets.getIamPolicy — Read bucket IAM policy.  
    /// - storage.buckets.create — Create bucket.  
    /// - storage.buckets.list — List buckets.  
    /// - storage.buckets.setIamPolicy — Update bucket IAM policy.  
    /// - storage.buckets.update — Update bucket metadata.  
    /// - storage.objects.delete — Delete object.  
    /// - storage.objects.get — Read object data and metadata.  
    /// - storage.objects.getIamPolicy — Read object IAM policy.  
    /// - storage.objects.create — Create object.  
    /// - storage.objects.list — List objects.  
    /// - storage.objects.setIamPolicy — Update object IAM policy.  
    /// - storage.objects.update — Update object metadata.
    pub fn permissions(mut self, value: Vec<String>) -> Self {
        self.permissions = Some(value);
        self
    }
    /// Builds [TestIamPermissionsResponse].
    pub fn build(self) -> TestIamPermissionsResponse {
        TestIamPermissionsResponse {
            kind: self.kind,
            permissions: self.permissions,
        }
    }
}
