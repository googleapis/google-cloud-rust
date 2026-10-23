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

//! Contract: `#[derive(ToRow)]` accepts `Option<T>` and `Vec<T>` fields, and
//! `Vec<u8>` as a `BYTES` value, also inside `Option`.

use google_cloud_bigquery::write::ToRow;

#[derive(ToRow)]
#[allow(dead_code)]
struct Nullable {
    maybe: Option<i64>,
    tags: Vec<String>,
    data: Vec<u8>,
    maybe_data: Option<Vec<u8>>,
}

#[test]
fn contract() {
    super::assert_to_row::<Nullable>();
}
