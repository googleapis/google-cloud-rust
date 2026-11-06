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

//! Derive macros (`FromEntity`, `IntoEntity`, `FromValue`, `IntoValue`) for the Google Cloud Datastore client.
//!
//! # Supported `#[datastore(...)]` Attributes
//!
//! - Container attributes: `#[datastore(rename_all = "camelCase" | "snake_case" | "PascalCase" | "SCREAMING_SNAKE_CASE" | "kebab-case")]`
//! - Field attributes:
//!   - `#[datastore(rename = "property_name")]`
//!   - `#[datastore(exclude_from_indexes)]`
//!   - `#[datastore(key)]` (maps `Option<Key>` or `Key` to `Entity::key()`)
//!   - `#[datastore(default)]` (uses `Default::default()` when the property is missing or null)
//!   - `#[datastore(flatten)]` (inlines nested struct properties into the parent entity)

extern crate proc_macro;

use proc_macro::TokenStream;

/// Derives `google_cloud_datastore::entity::FromEntity` and `TryFrom<Entity>` for a named struct.
#[proc_macro_derive(FromEntity, attributes(datastore))]
pub fn derive_from_entity(_input: TokenStream) -> TokenStream {
    unimplemented!()
}

/// Derives `google_cloud_datastore::entity::IntoEntity` and `From<T> for Entity` for a named struct.
#[proc_macro_derive(IntoEntity, attributes(datastore))]
pub fn derive_into_entity(_input: TokenStream) -> TokenStream {
    unimplemented!()
}

/// Derives `google_cloud_datastore::value::FromValue` for a named struct (stored as an embedded `Entity` value).
#[proc_macro_derive(FromValue, attributes(datastore))]
pub fn derive_from_value(_input: TokenStream) -> TokenStream {
    unimplemented!()
}

/// Derives `google_cloud_datastore::value::IntoValue` for a named struct (stored as an embedded `Entity` value).
#[proc_macro_derive(IntoValue, attributes(datastore))]
pub fn derive_into_value(_input: TokenStream) -> TokenStream {
    unimplemented!()
}
