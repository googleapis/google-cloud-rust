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

//! Contract: `#[derive(FromEntity, IntoEntity, FromValue, IntoValue)]` on a struct with named fields.

use google_cloud_datastore::{FromEntity, FromValue, IntoEntity, IntoValue};

#[derive(FromEntity, IntoEntity, FromValue, IntoValue)]
#[allow(dead_code)]
struct TaskRecord {
    title: String,
    priority: i64,
    completed: bool,
    score: f64,
    tags: Vec<String>,
    notes: Option<String>,
}

#[test]
fn contract() {
    super::assert_from_entity::<TaskRecord>();
    super::assert_into_entity::<TaskRecord>();
    super::assert_from_value::<TaskRecord>();
    super::assert_into_value::<TaskRecord>();
}
