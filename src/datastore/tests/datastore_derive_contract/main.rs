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

//! Compile-only contract tests for the `google-cloud-datastore-derive` macros:
//! each module pins one input shape that the derives accept.

mod attributes_and_nested;
mod named_struct;

use google_cloud_datastore::entity::{Entity, FromEntity, IntoEntity};
use google_cloud_datastore::error::EntityError;
use google_cloud_datastore::value::{FromValue, IntoValue};

fn assert_from_entity<T: FromEntity + TryFrom<Entity, Error = EntityError>>() {}
fn assert_into_entity<T: IntoEntity + Into<Entity>>() {}
fn assert_from_value<T: FromValue>() {}
fn assert_into_value<T: IntoValue>() {}
