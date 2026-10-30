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

//! Contract: `ToRow` can be derived on the same struct as `FromRow` or
//! `FromSql`, sharing the same `#[bigquery(...)]` helper attributes, alongside
//! standard derives.

use google_cloud_bigquery::query::{FromRow, FromSql};
use google_cloud_bigquery::write::ToRow;

#[derive(FromRow, ToRow, Debug, Clone, PartialEq)]
struct Customer {
    #[bigquery(rename = "identifier")]
    id: i64,
    name: String,
    address: Address,
}

#[derive(FromSql, ToRow, Debug, Clone, PartialEq)]
struct Address {
    city: String,
}

#[test]
fn contract() {
    super::assert_from_row::<Customer>();
    super::assert_to_row::<Customer>();
    super::assert_from_sql::<Address>();
    super::assert_proto_element::<Address>();
}
