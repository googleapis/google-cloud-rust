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

//! Contract: `#[derive(FromSql)]` on a struct with named fields implements
//! `FromSql`, and composes with the blanket `Option<T>` / `Vec<T>` impls.

use google_cloud_bigquery::query::FromSql;

#[derive(FromSql)]
#[allow(dead_code)]
struct Point {
    x: f64,
    y: f64,
    label: Option<String>,
}

#[test]
fn contract() {
    super::assert_from_sql::<Point>();
    super::assert_from_sql::<Option<Point>>();
    super::assert_from_sql::<Vec<Point>>();
}
