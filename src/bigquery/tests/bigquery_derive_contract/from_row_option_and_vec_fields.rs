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

//! Contract: `#[derive(FromRow)]` accepts `Option<T>`, `Vec<T>` and
//! `Option<Vec<T>>` fields for any `T: FromSql`, in named and tuple structs.

use google_cloud_bigquery::query::FromRow;

#[derive(FromRow)]
#[allow(dead_code)]
struct Nullable {
    maybe: Option<i64>,
    tags: Vec<String>,
    maybe_list: Option<Vec<bool>>,
}

#[derive(FromRow)]
#[allow(dead_code)]
struct NullableTuple(Option<String>, Vec<f64>);

#[test]
fn contract() {
    super::assert_from_row::<Nullable>();
    super::assert_from_row::<NullableTuple>();
}
