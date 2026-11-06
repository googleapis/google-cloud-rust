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

//! Error types for the Cloud Datastore client library.

pub use google_cloud_gax::error::Error;

/// The standard result type for Cloud Datastore client RPC operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors returned when constructing, validating, or decoding a Datastore [Key][crate::key::Key]
/// or [IncompleteKey][crate::key::IncompleteKey].
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum KeyError {
    /// A key path element had an empty `kind` string.
    #[error("datastore key kind must not be empty")]
    EmptyKind,

    /// A numeric key ID was `0`, which Datastore reserves as unset.
    #[error("datastore numeric key id must be non-zero")]
    ZeroId,

    /// A string key name was empty (`""`).
    #[error("datastore string key name must not be empty")]
    EmptyName,

    /// A key was constructed with an empty path.
    #[error("datastore key path must contain at least one element")]
    EmptyPath,

    /// An ancestor path element was missing its ID or name.
    #[error("datastore ancestor path element for kind `{0}` must be complete")]
    IncompleteAncestor(String),

    /// An incomplete key was provided where a complete key is required.
    #[error("datastore key for kind `{0}` is incomplete (missing id or name)")]
    IncompleteKey(String),

    /// A key's project or database ID did not match the client's partition context.
    #[error(
        "datastore key partition mismatch: expected project `{expected_project}` and database `{expected_database}`, got project `{actual_project}` and database `{actual_database}`"
    )]
    PartitionMismatch {
        /// The project ID configured on the client.
        expected_project: String,
        /// The project ID found on the key.
        actual_project: String,
        /// The database ID configured on the client.
        expected_database: String,
        /// The database ID found on the key.
        actual_database: String,
    },

    /// Failed to decode a URL-safe Base64 key or cursor token.
    #[error("invalid url-safe datastore token: {0}")]
    InvalidUrlSafeEncoding(String),
}

/// Errors returned when converting between Datastore [Value][crate::value::Value]s and Rust types.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ValueError {
    /// The Datastore value had a different variant than expected by the target Rust type.
    #[error("unexpected datastore value type: expected {expected}, got {actual}")]
    UnexpectedType {
        /// The expected Datastore value type name.
        expected: &'static str,
        /// The actual Datastore value type name encountered.
        actual: &'static str,
    },

    /// A 64-bit integer value was out of range for a narrower Rust integer type.
    #[error("datastore integer value {0} is out of range for target type")]
    IntegerOutOfRange(i64),

    /// Datastore does not permit an `ArrayValue` to directly contain another `ArrayValue`.
    #[error("datastore arrays cannot directly contain nested arrays")]
    NestedArray,

    /// An embedded key value was invalid or incomplete.
    #[error(transparent)]
    InvalidKey(#[from] KeyError),

    /// A custom value conversion error.
    #[error("{0}")]
    Custom(String),
}

/// Errors returned when reading or converting a Datastore [Entity][crate::entity::Entity].
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum EntityError {
    /// A required property was missing from the entity.
    #[error("missing required datastore entity property `{0}`")]
    MissingProperty(String),

    /// Converting a named property failed.
    #[error("failed to convert datastore entity property `{name}`: {source}")]
    InvalidProperty {
        /// The name of the property that failed conversion.
        name: String,
        /// The underlying value conversion error.
        #[source]
        source: ValueError,
    },

    /// Expected the entity to have a complete key, but it was missing or incomplete.
    #[error("datastore entity is missing a complete key")]
    MissingKey,

    /// The entity's key was invalid.
    #[error(transparent)]
    InvalidKey(#[from] KeyError),

    /// A custom entity conversion error.
    #[error("{0}")]
    Custom(String),
}
