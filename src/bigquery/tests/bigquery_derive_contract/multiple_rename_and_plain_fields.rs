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

//! Contract: renamed and plain fields can be freely mixed in the same struct,
//! and other (non-`bigquery`) attributes on fields, including doc comments,
//! are ignored by the derives.

use google_cloud_bigquery::query::{FromRow, FromSql};

#[derive(FromRow)]
#[allow(dead_code)]
struct MixedRow {
    plain_a: i64,
    #[bigquery(rename = "renamed_b")]
    b: String,
    /// Doc comments are attributes too.
    plain_c: bool,
    #[bigquery(rename = "renamed_d")]
    #[allow(unused)]
    d: Option<f64>,
}

#[derive(FromSql)]
#[allow(dead_code)]
struct MixedSql {
    #[bigquery(rename = "renamed_x")]
    x: i64,
    y: String,
    #[bigquery(rename = "renamed_z")]
    z: Vec<i64>,
}

#[test]
fn contract() {
    super::assert_from_row::<MixedRow>();
    super::assert_from_sql::<MixedSql>();
}
