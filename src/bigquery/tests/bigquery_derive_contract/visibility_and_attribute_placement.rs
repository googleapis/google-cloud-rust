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

//! Contract: the derives accept `pub` / `pub(crate)` / private structs and
//! fields, and structs with other outer attributes before or after the
//! `#[derive]`.

mod inner {
    use google_cloud_bigquery::query::{FromRow, FromSql};

    #[allow(dead_code)]
    #[derive(FromRow, FromSql)]
    #[allow(clippy::all)]
    pub struct PubRow {
        pub id: i64,
        pub(crate) name: String,
        #[bigquery(rename = "secret_col")]
        secret: String,
    }

    #[derive(FromRow)]
    #[allow(dead_code)]
    pub struct PubTuple(pub i64, pub(crate) String);
}

#[derive(google_cloud_bigquery::query::FromSql)]
#[allow(dead_code)]
pub(crate) struct CrateSql {
    a: i64,
}

#[test]
fn contract() {
    super::assert_from_row::<inner::PubRow>();
    super::assert_from_sql::<inner::PubRow>();
    super::assert_from_row::<inner::PubTuple>();
    super::assert_from_sql::<CrateSql>();
}
