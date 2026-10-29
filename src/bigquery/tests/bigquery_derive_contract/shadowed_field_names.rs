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

//! Contract: fields may use names that collide with identifiers used inside
//! the generated code (e.g. `row`, `value`, `name`, `idx`).

use google_cloud_bigquery::query::{FromRow, FromSql};

#[derive(FromRow)]
#[allow(dead_code)]
struct CollidingRow {
    row: i64,
    value: String,
    name: String,
    idx: i64,
}

#[derive(FromSql)]
#[allow(dead_code)]
struct CollidingSql {
    row: i64,
    value: String,
    name: String,
    idx: i64,
}

#[test]
fn contract() {
    super::assert_from_row::<CollidingRow>();
    super::assert_from_sql::<CollidingSql>();
}
