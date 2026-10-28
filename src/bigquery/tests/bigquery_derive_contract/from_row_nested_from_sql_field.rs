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

//! Contract: a `#[derive(FromRow)]` struct may have fields whose types are
//! `#[derive(FromSql)]` structs, including inside `Option` and `Vec`.

use google_cloud_bigquery::query::{FromRow, FromSql};

#[derive(FromSql)]
#[allow(dead_code)]
struct Address {
    city: String,
    zip: String,
}

#[derive(FromRow)]
#[allow(dead_code)]
struct Customer {
    name: String,
    address: Address,
    billing: Option<Address>,
    history: Vec<Address>,
}

#[test]
fn contract() {
    super::assert_from_row::<Customer>();
}
