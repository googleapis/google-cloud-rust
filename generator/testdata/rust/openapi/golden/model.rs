#![allow(dead_code)]

use serde::{Deserialize, Serialize};


/// The response message for Locations.ListLocations.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct ListLocationsResponse {

    /// A list of locations that matches the specified filter in the request.
    pub locations: ,

    /// The standard List next-page token.
    pub next_page_token: Option<String>,
}

/// A resource that represents a Google Cloud location.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct Location {

    /// Resource name for the location, which may vary between implementations.
    /// For example: `"projects/example-project/locations/us-east1"`
    pub name: Option<String>,

    /// The canonical id for this location. For example: `"us-east1"`.
    pub location_id: Option<String>,

    /// The friendly name for this location, typically a nearby city name.
    /// For example, "Tokyo".
    pub display_name: Option<String>,

    /// Cross-service attributes for the location. For example
    /// 
    ///     {"cloud.googleapis.com/region": "us-east1"}
    pub labels: Option<std::collections::HashMap<String,String>>,

    /// Service-specific metadata. For example the available capacity at the given
    /// location.
    pub metadata: ,
}

/// Response message for SecretManagerService.ListSecrets.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct ListSecretsResponse {

    /// The list of Secrets sorted in reverse by create_time (newest
    /// first).
    pub secrets: ,

    /// A token to retrieve the next page of results. Pass this value in
    /// ListSecretsRequest.page_token to retrieve the next page.
    pub next_page_token: Option<String>,

    /// The total number of Secrets but 0 when the
    /// ListSecretsRequest.filter field is set.
    pub total_size: Option<i32>,
}

/// A Secret is a logical secret whose value and versions can
/// be accessed.
/// 
/// A Secret is made up of zero or more SecretVersions that
/// represent the secret data.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct Secret {

    /// Output only. The resource name of the Secret in the format `projects/_*_/secrets/*`.
    pub name: Option<String>,

    /// Optional. Immutable. The replication policy of the secret data attached to the Secret.
    /// 
    /// The replication policy cannot be changed after the Secret has been created.
    pub replication: Option<crate::Replication>,

    /// Output only. The time at which the Secret was created.
    pub create_time: Option<String> /* TODO(#77) - handle .google.protobuf.Timestamp */,

    /// The labels assigned to this Secret.
    /// 
    /// Label keys must be between 1 and 63 characters long, have a UTF-8 encoding
    /// of maximum 128 bytes, and must conform to the following PCRE regular
    /// expression: `\p{Ll}\p{Lo}{0,62}`
    /// 
    /// Label values must be between 0 and 63 characters long, have a UTF-8
    /// encoding of maximum 128 bytes, and must conform to the following PCRE
    /// regular expression: `[\p{Ll}\p{Lo}\p{N}_-]{0,63}`
    /// 
    /// No more than 64 labels can be assigned to a given resource.
    pub labels: Option<std::collections::HashMap<String,String>>,

    /// Optional. A list of up to 10 Pub/Sub topics to which messages are published when
    /// control plane operations are called on the secret or its versions.
    pub topics: ,

    /// Optional. Timestamp in UTC when the Secret is scheduled to expire. This is
    /// always provided on output, regardless of what was sent on input.
    pub expire_time: Option<String> /* TODO(#77) - handle .google.protobuf.Timestamp */,

    /// Input only. The TTL for the Secret.
    pub ttl: Option<String> /* TODO(#77) - handle .google.protobuf.Duration */,

    /// Optional. Etag of the currently stored Secret.
    pub etag: Option<String>,

    /// Optional. Rotation policy attached to the Secret. May be excluded if there is no
    /// rotation policy.
    pub rotation: Option<crate::Rotation>,

    /// Optional. Mapping from version alias to version name.
    /// 
    /// A version alias is a string with a maximum length of 63 characters and can
    /// contain uppercase and lowercase letters, numerals, and the hyphen (`-`)
    /// and underscore ('_') characters. An alias string must start with a
    /// letter and cannot be the string 'latest' or 'NEW'.
    /// No more than 50 aliases can be assigned to a given secret.
    /// 
    /// Version-Alias pairs will be viewable via GetSecret and modifiable via
    /// UpdateSecret. Access by alias is only be supported on
    /// GetSecretVersion and AccessSecretVersion.
    pub version_aliases: Option<std::collections::HashMap<String,i64>>,

    /// Optional. Custom metadata about the secret.
    /// 
    /// Annotations are distinct from various forms of labels.
    /// Annotations exist to allow client tools to store their own state
    /// information without requiring a database.
    /// 
    /// Annotation keys must be between 1 and 63 characters long, have a UTF-8
    /// encoding of maximum 128 bytes, begin and end with an alphanumeric character
    /// ([a-z0-9A-Z]), and may have dashes (-), underscores (_), dots (.), and
    /// alphanumerics in between these symbols.
    /// 
    /// The total size of annotation keys and values must be less than 16KiB.
    pub annotations: Option<std::collections::HashMap<String,String>>,

    /// Optional. Secret Version TTL after destruction request
    /// 
    /// This is a part of the Delayed secret version destroy feature.
    /// For secret with TTL>0, version destruction doesn't happen immediately
    /// on calling destroy instead the version goes to a disabled state and
    /// destruction happens after the TTL expires.
    pub version_destroy_ttl: Option<String> /* TODO(#77) - handle .google.protobuf.Duration */,

    /// Optional. The customer-managed encryption configuration of the Regionalised Secrets.
    /// If no configuration is provided, Google-managed default encryption is used.
    /// 
    /// Updates to the Secret encryption configuration only apply to
    /// SecretVersions added afterwards. They do not apply
    /// retroactively to existing SecretVersions.
    pub customer_managed_encryption: Option<crate::CustomerManagedEncryption>,
}

/// A policy that defines the replication and encryption configuration of data.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct Replication {

    /// The Secret will automatically be replicated without any restrictions.
    pub automatic: Option<crate::Automatic>,

    /// The Secret will only be replicated into the locations specified.
    pub user_managed: Option<crate::UserManaged>,
}

/// A replication policy that replicates the Secret payload without any
/// restrictions.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct Automatic {

    /// Optional. The customer-managed encryption configuration of the Secret. If no
    /// configuration is provided, Google-managed default encryption is used.
    /// 
    /// Updates to the Secret encryption configuration only apply to
    /// SecretVersions added afterwards. They do not apply
    /// retroactively to existing SecretVersions.
    pub customer_managed_encryption: Option<crate::CustomerManagedEncryption>,
}

/// Configuration for encrypting secret payloads using customer-managed
/// encryption keys (CMEK).
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct CustomerManagedEncryption {

    /// Required. The resource name of the Cloud KMS CryptoKey used to encrypt secret
    /// payloads.
    /// 
    /// For secrets using the UserManaged replication
    /// policy type, Cloud KMS CryptoKeys must reside in the same location as the
    /// replica location.
    /// 
    /// For secrets using the Automatic replication policy
    /// type, Cloud KMS CryptoKeys must reside in `global`.
    /// 
    /// The expected format is `projects/_*_/locations/_*_/keyRings/_*_/cryptoKeys/*`.
    pub kms_key_name: String,
}

/// A replication policy that replicates the Secret payload into the
/// locations specified in Secret.replication.user_managed.replicas
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct UserManaged {

    /// Required. The list of Replicas for this Secret.
    /// 
    /// Cannot be empty.
    pub replicas: ,
}

/// Represents a Replica for this Secret.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct Replica {

    /// The canonical IDs of the location to replicate data.
    /// For example: `"us-east1"`.
    pub location: Option<String>,

    /// Optional. The customer-managed encryption configuration of the User-Managed
    /// Replica. If no configuration is
    /// provided, Google-managed default encryption is used.
    /// 
    /// Updates to the Secret encryption configuration only apply to
    /// SecretVersions added afterwards. They do not apply
    /// retroactively to existing SecretVersions.
    pub customer_managed_encryption: Option<crate::CustomerManagedEncryption>,
}

/// A Pub/Sub topic which Secret Manager will publish to when control plane
/// events occur on this secret.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct Topic {

    /// Required. The resource name of the Pub/Sub topic that will be published to, in the
    /// following format: `projects/_*_/topics/*`. For publication to succeed, the
    /// Secret Manager service agent must have the `pubsub.topic.publish`
    /// permission on the topic. The Pub/Sub Publisher role
    /// (`roles/pubsub.publisher`) includes this permission.
    pub name: String,
}

/// The rotation time and period for a Secret. At next_rotation_time, Secret
/// Manager will send a Pub/Sub notification to the topics configured on the
/// Secret. Secret.topics must be set to configure rotation.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct Rotation {

    /// Optional. Timestamp in UTC at which the Secret is scheduled to rotate. Cannot be
    /// set to less than 300s (5 min) in the future and at most 3153600000s (100
    /// years).
    /// 
    /// next_rotation_time MUST  be set if rotation_period is set.
    pub next_rotation_time: Option<String> /* TODO(#77) - handle .google.protobuf.Timestamp */,

    /// Input only. The Duration between rotation notifications. Must be in seconds
    /// and at least 3600s (1h) and at most 3153600000s (100 years).
    /// 
    /// If rotation_period is set, next_rotation_time must be set.
    /// next_rotation_time will be advanced by this period when the service
    /// automatically sends rotation notifications.
    pub rotation_period: Option<String> /* TODO(#77) - handle .google.protobuf.Duration */,
}

/// Request message for SecretManagerService.AddSecretVersion.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct AddSecretVersionRequest {

    /// Required. The secret payload of the SecretVersion.
    pub payload: Option<crate::SecretPayload>,
}

/// A secret payload resource in the Secret Manager API. This contains the
/// sensitive secret payload that is associated with a SecretVersion.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct SecretPayload {

    /// The secret data. Must be no larger than 64KiB.
    pub data: Option<bytes::Bytes>,

    /// Optional. If specified, SecretManagerService will verify the integrity of the
    /// received data on SecretManagerService.AddSecretVersion calls using
    /// the crc32c checksum and store it to include in future
    /// SecretManagerService.AccessSecretVersion responses. If a checksum is
    /// not provided in the SecretManagerService.AddSecretVersion request, the
    /// SecretManagerService will generate and store one for you.
    /// 
    /// The CRC32C value is encoded as a Int64 for compatibility, and can be
    /// safely downconverted to uint32 in languages that support this type.
    /// https://cloud.google.com/apis/design/design_patterns#integer_types
    pub data_crc_32_c: Option<i64>,
}

/// A secret version resource in the Secret Manager API.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct SecretVersion {

    /// Output only. The resource name of the SecretVersion in the
    /// format `projects/_*_/secrets/_*_/versions/*`.
    /// 
    /// SecretVersion IDs in a Secret start at 1 and
    /// are incremented for each subsequent version of the secret.
    pub name: Option<String>,

    /// Output only. The time at which the SecretVersion was created.
    pub create_time: Option<String> /* TODO(#77) - handle .google.protobuf.Timestamp */,

    /// Output only. The time this SecretVersion was destroyed.
    /// Only present if state is
    /// DESTROYED.
    pub destroy_time: Option<String> /* TODO(#77) - handle .google.protobuf.Timestamp */,

    /// Output only. The current state of the SecretVersion.
    pub state: Option<String>,

    /// The replication status of the SecretVersion.
    pub replication_status: Option<crate::ReplicationStatus>,

    /// Output only. Etag of the currently stored SecretVersion.
    pub etag: Option<String>,

    /// Output only. True if payload checksum specified in SecretPayload object has been
    /// received by SecretManagerService on
    /// SecretManagerService.AddSecretVersion.
    pub client_specified_payload_checksum: Option<bool>,

    /// Optional. Output only. Scheduled destroy time for secret version.
    /// This is a part of the Delayed secret version destroy feature. For a
    /// Secret with a valid version destroy TTL, when a secert version is
    /// destroyed, version is moved to disabled state and it is scheduled for
    /// destruction Version is destroyed only after the scheduled_destroy_time.
    pub scheduled_destroy_time: Option<String> /* TODO(#77) - handle .google.protobuf.Timestamp */,

    /// Output only. The customer-managed encryption status of the SecretVersion. Only
    /// populated if customer-managed encryption is used and Secret is
    /// a Regionalised Secret.
    pub customer_managed_encryption: Option<crate::CustomerManagedEncryptionStatus>,
}

/// The replication status of a SecretVersion.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct ReplicationStatus {

    /// Describes the replication status of a SecretVersion with
    /// automatic replication.
    /// 
    /// Only populated if the parent Secret has an automatic replication
    /// policy.
    pub automatic: Option<crate::AutomaticStatus>,

    /// Describes the replication status of a SecretVersion with
    /// user-managed replication.
    /// 
    /// Only populated if the parent Secret has a user-managed replication
    /// policy.
    pub user_managed: Option<crate::UserManagedStatus>,
}

/// The replication status of a SecretVersion using automatic replication.
/// 
/// Only populated if the parent Secret has an automatic replication
/// policy.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct AutomaticStatus {

    /// Output only. The customer-managed encryption status of the SecretVersion. Only
    /// populated if customer-managed encryption is used.
    pub customer_managed_encryption: Option<crate::CustomerManagedEncryptionStatus>,
}

/// Describes the status of customer-managed encryption.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct CustomerManagedEncryptionStatus {

    /// Required. The resource name of the Cloud KMS CryptoKeyVersion used to encrypt the
    /// secret payload, in the following format:
    /// `projects/_*_/locations/_*_/keyRings/_*_/cryptoKeys/_*_/versions/*`.
    pub kms_key_version_name: String,
}

/// The replication status of a SecretVersion using user-managed
/// replication.
/// 
/// Only populated if the parent Secret has a user-managed replication
/// policy.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct UserManagedStatus {

    /// Output only. The list of replica statuses for the SecretVersion.
    pub replicas: ,
}

/// Describes the status of a user-managed replica for the SecretVersion.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct ReplicaStatus {

    /// Output only. The canonical ID of the replica location.
    /// For example: `"us-east1"`.
    pub location: Option<String>,

    /// Output only. The customer-managed encryption status of the SecretVersion. Only
    /// populated if customer-managed encryption is used.
    pub customer_managed_encryption: Option<crate::CustomerManagedEncryptionStatus>,
}

/// A generic empty message that you can re-use to avoid defining duplicated
/// empty messages in your APIs. A typical example is to use it as the request
/// or the response type of an API method. For instance:
/// 
///     service Foo {
///       rpc Bar(google.protobuf.Empty) returns (google.protobuf.Empty);
///     }
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct Empty {
}

/// Response message for SecretManagerService.ListSecretVersions.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct ListSecretVersionsResponse {

    /// The list of SecretVersions sorted in reverse by
    /// create_time (newest first).
    pub versions: ,

    /// A token to retrieve the next page of results. Pass this value in
    /// ListSecretVersionsRequest.page_token to retrieve the next page.
    pub next_page_token: Option<String>,

    /// The total number of SecretVersions but 0 when the
    /// ListSecretsRequest.filter field is set.
    pub total_size: Option<i32>,
}

/// Response message for SecretManagerService.AccessSecretVersion.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct AccessSecretVersionResponse {

    /// The resource name of the SecretVersion in the format
    /// `projects/_*_/secrets/_*_/versions/*` or
    /// `projects/_*_/locations/_*_/secrets/_*_/versions/*`.
    pub name: Option<String>,

    /// Secret payload
    pub payload: Option<crate::SecretPayload>,
}

/// Request message for SecretManagerService.DisableSecretVersion.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct DisableSecretVersionRequest {

    /// Optional. Etag of the SecretVersion. The request succeeds if it matches
    /// the etag of the currently stored secret version object. If the etag is
    /// omitted, the request succeeds.
    pub etag: Option<String>,
}

/// Request message for SecretManagerService.EnableSecretVersion.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct EnableSecretVersionRequest {

    /// Optional. Etag of the SecretVersion. The request succeeds if it matches
    /// the etag of the currently stored secret version object. If the etag is
    /// omitted, the request succeeds.
    pub etag: Option<String>,
}

/// Request message for SecretManagerService.DestroySecretVersion.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct DestroySecretVersionRequest {

    /// Optional. Etag of the SecretVersion. The request succeeds if it matches
    /// the etag of the currently stored secret version object. If the etag is
    /// omitted, the request succeeds.
    pub etag: Option<String>,
}

/// Request message for `SetIamPolicy` method.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct SetIamPolicyRequest {

    /// REQUIRED: The complete policy to be applied to the `resource`. The size of
    /// the policy is limited to a few 10s of KB. An empty policy is a
    /// valid policy but certain Google Cloud services (such as Projects)
    /// might reject them.
    pub policy: Option<crate::Policy>,

    /// OPTIONAL: A FieldMask specifying which fields of the policy to modify. Only
    /// the fields in the mask will be modified. If no mask is provided, the
    /// following default mask is used:
    /// 
    /// `paths: "bindings, etag"`
    pub update_mask: ,
}

/// An Identity and Access Management (IAM) policy, which specifies access
/// controls for Google Cloud resources.
/// 
/// 
/// A `Policy` is a collection of `bindings`. A `binding` binds one or more
/// `members`, or principals, to a single `role`. Principals can be user
/// accounts, service accounts, Google groups, and domains (such as G Suite). A
/// `role` is a named list of permissions; each `role` can be an IAM predefined
/// role or a user-created custom role.
/// 
/// For some types of Google Cloud resources, a `binding` can also specify a
/// `condition`, which is a logical expression that allows access to a resource
/// only if the expression evaluates to `true`. A condition can add constraints
/// based on attributes of the request, the resource, or both. To learn which
/// resources support conditions in their IAM policies, see the
/// [IAM documentation](https://cloud.google.com/iam/help/conditions/resource-policies).
/// 
/// **JSON example:**
/// 
/// ```norust
///     {
///       "bindings": [
///         {
///           "role": "roles/resourcemanager.organizationAdmin",
///           "members": [
///             "user:mike@example.com",
///             "group:admins@example.com",
///             "domain:google.com",
///             "serviceAccount:my-project-id@appspot.gserviceaccount.com"
///           ]
///         },
///         {
///           "role": "roles/resourcemanager.organizationViewer",
///           "members": [
///             "user:eve@example.com"
///           ],
///           "condition": {
///             "title": "expirable access",
///             "description": "Does not grant access after Sep 2020",
///             "expression": "request.time < timestamp('2020-10-01T00:00:00.000Z')",
///           }
///         }
///       ],
///       "etag": "BwWWja0YfJA=",
///       "version": 3
///     }
/// ```
/// 
/// **YAML example:**
/// 
/// ```norust
///     bindings:
///     - members:
///       - user:mike@example.com
///       - group:admins@example.com
///       - domain:google.com
///       - serviceAccount:my-project-id@appspot.gserviceaccount.com
///       role: roles/resourcemanager.organizationAdmin
///     - members:
///       - user:eve@example.com
///       role: roles/resourcemanager.organizationViewer
///       condition:
///         title: expirable access
///         description: Does not grant access after Sep 2020
///         expression: request.time < timestamp('2020-10-01T00:00:00.000Z')
///     etag: BwWWja0YfJA=
///     version: 3
/// ```
/// 
/// For a description of IAM and its features, see the
/// [IAM documentation](https://cloud.google.com/iam/docs/).
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct Policy {

    /// Specifies the format of the policy.
    /// 
    /// Valid values are `0`, `1`, and `3`. Requests that specify an invalid value
    /// are rejected.
    /// 
    /// Any operation that affects conditional role bindings must specify version
    /// `3`. This requirement applies to the following operations:
    /// 
    /// * Getting a policy that includes a conditional role binding
    /// * Adding a conditional role binding to a policy
    /// * Changing a conditional role binding in a policy
    /// * Removing any role binding, with or without a condition, from a policy
    ///   that includes conditions
    /// 
    /// **Important:** If you use IAM Conditions, you must include the `etag` field
    /// whenever you call `setIamPolicy`. If you omit this field, then IAM allows
    /// you to overwrite a version `3` policy with a version `1` policy, and all of
    /// the conditions in the version `3` policy are lost.
    /// 
    /// If a policy does not include any conditions, operations on that policy may
    /// specify any valid version or leave the field unset.
    /// 
    /// To learn which resources support conditions in their IAM policies, see the
    /// [IAM documentation](https://cloud.google.com/iam/help/conditions/resource-policies).
    pub version: Option<i32>,

    /// Associates a list of `members`, or principals, with a `role`. Optionally,
    /// may specify a `condition` that determines how and when the `bindings` are
    /// applied. Each of the `bindings` must contain at least one principal.
    /// 
    /// The `bindings` in a `Policy` can refer to up to 1,500 principals; up to 250
    /// of these principals can be Google groups. Each occurrence of a principal
    /// counts towards these limits. For example, if the `bindings` grant 50
    /// different roles to `user:alice@example.com`, and not to any other
    /// principal, then you can add another 1,450 principals to the `bindings` in
    /// the `Policy`.
    pub bindings: ,

    /// Specifies cloud audit logging configuration for this policy.
    pub audit_configs: ,

    /// `etag` is used for optimistic concurrency control as a way to help
    /// prevent simultaneous updates of a policy from overwriting each other.
    /// It is strongly suggested that systems make use of the `etag` in the
    /// read-modify-write cycle to perform policy updates in order to avoid race
    /// conditions: An `etag` is returned in the response to `getIamPolicy`, and
    /// systems are expected to put that etag in the request to `setIamPolicy` to
    /// ensure that their change will be applied to the same version of the policy.
    /// 
    /// **Important:** If you use IAM Conditions, you must include the `etag` field
    /// whenever you call `setIamPolicy`. If you omit this field, then IAM allows
    /// you to overwrite a version `3` policy with a version `1` policy, and all of
    /// the conditions in the version `3` policy are lost.
    pub etag: Option<bytes::Bytes>,
}

/// Associates `members`, or principals, with a `role`.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct Binding {

    /// Role that is assigned to the list of `members`, or principals.
    /// For example, `roles/viewer`, `roles/editor`, or `roles/owner`.
    /// 
    /// For an overview of the IAM roles and permissions, see the
    /// [IAM documentation](https://cloud.google.com/iam/docs/roles-overview). For
    /// a list of the available pre-defined roles, see
    /// [here](https://cloud.google.com/iam/docs/understanding-roles).
    pub role: Option<String>,

    /// Specifies the principals requesting access for a Google Cloud resource.
    /// `members` can have the following values:
    /// 
    /// * `allUsers`: A special identifier that represents anyone who is
    ///    on the internet; with or without a Google account.
    /// 
    /// * `allAuthenticatedUsers`: A special identifier that represents anyone
    ///    who is authenticated with a Google account or a service account.
    ///    Does not include identities that come from external identity providers
    ///    (IdPs) through identity federation.
    /// 
    /// * `user:{emailid}`: An email address that represents a specific Google
    ///    account. For example, `alice@example.com` .
    /// 
    /// 
    /// * `serviceAccount:{emailid}`: An email address that represents a Google
    ///    service account. For example,
    ///    `my-other-app@appspot.gserviceaccount.com`.
    /// 
    /// * `serviceAccount:{projectid}.svc.id.goog[{namespace}/{kubernetes-sa}]`: An
    ///    identifier for a
    ///    [Kubernetes service
    ///    account](https://cloud.google.com/kubernetes-engine/docs/how-to/kubernetes-service-accounts).
    ///    For example, `my-project.svc.id.goog[my-namespace/my-kubernetes-sa]`.
    /// 
    /// * `group:{emailid}`: An email address that represents a Google group.
    ///    For example, `admins@example.com`.
    /// 
    /// 
    /// * `domain:{domain}`: The G Suite domain (primary) that represents all the
    ///    users of that domain. For example, `google.com` or `example.com`.
    /// 
    /// 
    /// 
    /// 
    /// * `principal://iam.googleapis.com/locations/global/workforcePools/{pool_id}/subject/{subject_attribute_value}`:
    ///   A single identity in a workforce identity pool.
    /// 
    /// * `principalSet://iam.googleapis.com/locations/global/workforcePools/{pool_id}/group/{group_id}`:
    ///   All workforce identities in a group.
    /// 
    /// * `principalSet://iam.googleapis.com/locations/global/workforcePools/{pool_id}/attribute.{attribute_name}/{attribute_value}`:
    ///   All workforce identities with a specific attribute value.
    /// 
    /// * `principalSet://iam.googleapis.com/locations/global/workforcePools/{pool_id}/*`:
    ///   All identities in a workforce identity pool.
    /// 
    /// * `principal://iam.googleapis.com/projects/{project_number}/locations/global/workloadIdentityPools/{pool_id}/subject/{subject_attribute_value}`:
    ///   A single identity in a workload identity pool.
    /// 
    /// * `principalSet://iam.googleapis.com/projects/{project_number}/locations/global/workloadIdentityPools/{pool_id}/group/{group_id}`:
    ///   A workload identity pool group.
    /// 
    /// * `principalSet://iam.googleapis.com/projects/{project_number}/locations/global/workloadIdentityPools/{pool_id}/attribute.{attribute_name}/{attribute_value}`:
    ///   All identities in a workload identity pool with a certain attribute.
    /// 
    /// * `principalSet://iam.googleapis.com/projects/{project_number}/locations/global/workloadIdentityPools/{pool_id}/*`:
    ///   All identities in a workload identity pool.
    /// 
    /// * `deleted:user:{emailid}?uid={uniqueid}`: An email address (plus unique
    ///    identifier) representing a user that has been recently deleted. For
    ///    example, `alice@example.com?uid=123456789012345678901`. If the user is
    ///    recovered, this value reverts to `user:{emailid}` and the recovered user
    ///    retains the role in the binding.
    /// 
    /// * `deleted:serviceAccount:{emailid}?uid={uniqueid}`: An email address (plus
    ///    unique identifier) representing a service account that has been recently
    ///    deleted. For example,
    ///    `my-other-app@appspot.gserviceaccount.com?uid=123456789012345678901`.
    ///    If the service account is undeleted, this value reverts to
    ///    `serviceAccount:{emailid}` and the undeleted service account retains the
    ///    role in the binding.
    /// 
    /// * `deleted:group:{emailid}?uid={uniqueid}`: An email address (plus unique
    ///    identifier) representing a Google group that has been recently
    ///    deleted. For example, `admins@example.com?uid=123456789012345678901`. If
    ///    the group is recovered, this value reverts to `group:{emailid}` and the
    ///    recovered group retains the role in the binding.
    /// 
    /// * `deleted:principal://iam.googleapis.com/locations/global/workforcePools/{pool_id}/subject/{subject_attribute_value}`:
    ///   Deleted single identity in a workforce identity pool. For example,
    ///   `deleted:principal://iam.googleapis.com/locations/global/workforcePools/my-pool-id/subject/my-subject-attribute-value`.
    pub members: String,

    /// The condition that is associated with this binding.
    /// 
    /// If the condition evaluates to `true`, then this binding applies to the
    /// current request.
    /// 
    /// If the condition evaluates to `false`, then this binding does not apply to
    /// the current request. However, a different role binding might grant the same
    /// role to one or more of the principals in this binding.
    /// 
    /// To learn which resources support conditions in their IAM policies, see the
    /// [IAM
    /// documentation](https://cloud.google.com/iam/help/conditions/resource-policies).
    pub condition: Option<crate::Expr>,
}

/// Represents a textual expression in the Common Expression Language (CEL)
/// syntax. CEL is a C-like expression language. The syntax and semantics of CEL
/// are documented at https://github.com/google/cel-spec.
/// 
/// Example (Comparison):
/// 
///     title: "Summary size limit"
///     description: "Determines if a summary is less than 100 chars"
///     expression: "document.summary.size() < 100"
/// 
/// Example (Equality):
/// 
///     title: "Requestor is owner"
///     description: "Determines if requestor is the document owner"
///     expression: "document.owner == request.auth.claims.email"
/// 
/// Example (Logic):
/// 
///     title: "Public documents"
///     description: "Determine whether the document should be publicly visible"
///     expression: "document.type != 'private' && document.type != 'internal'"
/// 
/// Example (Data Manipulation):
/// 
///     title: "Notification string"
///     description: "Create a notification string with a timestamp."
///     expression: "'New message received at ' + string(document.create_time)"
/// 
/// The exact variables and functions that may be referenced within an expression
/// are determined by the service that evaluates it. See the service
/// documentation for additional information.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct Expr {

    /// Textual representation of an expression in Common Expression Language
    /// syntax.
    pub expression: Option<String>,

    /// Optional. Title for the expression, i.e. a short string describing
    /// its purpose. This can be used e.g. in UIs which allow to enter the
    /// expression.
    pub title: Option<String>,

    /// Optional. Description of the expression. This is a longer text which
    /// describes the expression, e.g. when hovered over it in a UI.
    pub description: Option<String>,

    /// Optional. String indicating the location of the expression for error
    /// reporting, e.g. a file name and a position in the file.
    pub location: Option<String>,
}

/// Specifies the audit configuration for a service.
/// The configuration determines which permission types are logged, and what
/// identities, if any, are exempted from logging.
/// An AuditConfig must have one or more AuditLogConfigs.
/// 
/// If there are AuditConfigs for both `allServices` and a specific service,
/// the union of the two AuditConfigs is used for that service: the log_types
/// specified in each AuditConfig are enabled, and the exempted_members in each
/// AuditLogConfig are exempted.
/// 
/// Example Policy with multiple AuditConfigs:
/// 
///     {
///       "audit_configs": [
///         {
///           "service": "allServices",
///           "audit_log_configs": [
///             {
///               "log_type": "DATA_READ",
///               "exempted_members": [
///                 "user:jose@example.com"
///               ]
///             },
///             {
///               "log_type": "DATA_WRITE"
///             },
///             {
///               "log_type": "ADMIN_READ"
///             }
///           ]
///         },
///         {
///           "service": "sampleservice.googleapis.com",
///           "audit_log_configs": [
///             {
///               "log_type": "DATA_READ"
///             },
///             {
///               "log_type": "DATA_WRITE",
///               "exempted_members": [
///                 "user:aliya@example.com"
///               ]
///             }
///           ]
///         }
///       ]
///     }
/// 
/// For sampleservice, this policy enables DATA_READ, DATA_WRITE and ADMIN_READ
/// logging. It also exempts `jose@example.com` from DATA_READ logging, and
/// `aliya@example.com` from DATA_WRITE logging.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct AuditConfig {

    /// Specifies a service that will be enabled for audit logging.
    /// For example, `storage.googleapis.com`, `cloudsql.googleapis.com`.
    /// `allServices` is a special value that covers all services.
    pub service: Option<String>,

    /// The configuration for logging of each type of permission.
    pub audit_log_configs: ,
}

/// Provides the configuration for logging a type of permissions.
/// Example:
/// 
///     {
///       "audit_log_configs": [
///         {
///           "log_type": "DATA_READ",
///           "exempted_members": [
///             "user:jose@example.com"
///           ]
///         },
///         {
///           "log_type": "DATA_WRITE"
///         }
///       ]
///     }
/// 
/// This enables 'DATA_READ' and 'DATA_WRITE' logging, while exempting
/// jose@example.com from DATA_READ logging.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct AuditLogConfig {

    /// The log type that this config enables.
    pub log_type: Option<String>,

    /// Specifies the identities that do not cause logging for this type of
    /// permission.
    /// Follows the same format of Binding.members.
    pub exempted_members: String,
}

/// Request message for `TestIamPermissions` method.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct TestIamPermissionsRequest {

    /// The set of permissions to check for the `resource`. Permissions with
    /// wildcards (such as `*` or `storage.*`) are not allowed. For more
    /// information see
    /// [IAM Overview](https://cloud.google.com/iam/docs/overview#permissions).
    pub permissions: String,
}

/// Response message for `TestIamPermissions` method.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct TestIamPermissionsResponse {

    /// A subset of `TestPermissionsRequest.permissions` that the caller is
    /// allowed.
    pub permissions: String,
}
