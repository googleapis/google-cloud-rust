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

//! Contract: `#[derive(ToRow)]` tolerates user-defined items that shadow
//! common prelude names (`Result`, `Ok`, `Err`, `Option`) and names of
//! `google_cloud_bigquery` items (`ToRow`, `StructFields`, `ProtoValue`,
//! `ProtoElement`, `FieldSchema`, `FieldEncoder`, `ConvertError`, `write`,
//! `error`), and fields named like identifiers in the generated code
//! (`fields`, `row`, `field`, `value`, `name`). This pins these common names
//! only; it is not a full hygiene guarantee.

#![allow(dead_code)]

type Result<T> = core::result::Result<T, ()>;
struct Ok;
struct Err;
struct Option;
struct ToRow;
struct StructFields;
struct ProtoValue;
struct ProtoElement;
struct FieldSchema;
struct FieldEncoder;
struct ConvertError;
mod write {}
mod error {}

#[derive(google_cloud_bigquery::write::ToRow)]
struct MyRow {
    fields: i64,
    row: String,
    field: bool,
    value: f64,
    name: String,
    #[bigquery(rename = "nested_col")]
    nested: MyStruct,
}

#[derive(google_cloud_bigquery::write::ToRow)]
struct MyStruct {
    value: String,
}

#[derive(google_cloud_bigquery::write::ToRow)]
struct MyGeneric<T> {
    value: T,
}

#[test]
fn contract() {
    super::assert_to_row::<MyRow>();
    super::assert_proto_element::<MyStruct>();
    super::assert_to_row::<MyGeneric<i64>>();
}
