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

//! Compile-only contract tests for the `google-cloud-bigquery-derive` macros:
//! each module pins one input shape that the derives accept. See the
//! "Compatibility" section in `src/bigquery-derive/src/lib.rs` for the policy.

mod from_row_and_from_sql_on_same_struct;
mod from_row_generic;
mod from_row_named_struct;
mod from_row_nested_from_sql_field;
mod from_row_option_and_vec_fields;
mod from_row_rename;
mod from_row_tuple_struct;
mod from_sql_generic;
mod from_sql_named_struct;
mod from_sql_rename;
mod from_sql_tuple_struct;
mod fully_qualified_derive_path;
mod multiple_rename_and_plain_fields;
mod raw_identifier_fields;
mod shadowed_field_names;
mod shadowed_prelude_names;
mod visibility_and_attribute_placement;

use google_cloud_bigquery::error::RowError;
use google_cloud_bigquery::query::{FromSql, Row};

/// Asserts at compile time that `T` implements `TryFrom<Row>` with
/// `Error = RowError`.
fn assert_from_row<T: TryFrom<Row, Error = RowError>>() {}

/// Asserts at compile time that `T` implements `FromSql`.
fn assert_from_sql<T: FromSql>() {}
