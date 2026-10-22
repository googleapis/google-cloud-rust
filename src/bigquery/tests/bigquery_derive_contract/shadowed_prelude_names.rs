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

//! Contract: the derives tolerate user-defined items that shadow common
//! prelude names (`Result`, `Ok`, `Err`, `Option`, `TryFrom`) and names of
//! `google_cloud_bigquery` items (`Row`, `FromSql`, `SqlValue`, `RowError`,
//! `ConvertError`, `query`, `error`). This pins these common names only; it is
//! not a full hygiene guarantee.

#![allow(dead_code, non_camel_case_types)]

type Result<T> = core::result::Result<T, ()>;
struct Ok;
struct Err;
struct Option;
struct TryFrom;
struct Error;
struct Row;
struct FromSql;
struct SqlValue;
struct RowError;
struct ConvertError;
mod query {}
mod error {}

#[derive(google_cloud_bigquery::query::FromRow)]
struct MyRow {
    id: i64,
    #[bigquery(rename = "nested_col")]
    nested: MyStruct,
}

#[derive(google_cloud_bigquery::query::FromRow)]
struct MyTupleRow(i64, MyStruct);

#[derive(google_cloud_bigquery::query::FromSql)]
struct MyStruct {
    value: String,
}

#[derive(google_cloud_bigquery::query::FromSql)]
struct MyGeneric<T>(T);

#[test]
fn contract() {
    super::assert_from_row::<MyRow>();
    super::assert_from_row::<MyTupleRow>();
    super::assert_from_sql::<MyStruct>();
    super::assert_from_sql::<MyGeneric<i64>>();
}
