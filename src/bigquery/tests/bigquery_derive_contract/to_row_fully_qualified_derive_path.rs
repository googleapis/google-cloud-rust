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

//! Contract: `#[derive(ToRow)]` can be invoked through its fully qualified
//! path without importing any traits or types from `google_cloud_bigquery`.

#[derive(google_cloud_bigquery::write::ToRow)]
#[allow(dead_code)]
struct Row1 {
    #[bigquery(rename = "a_col")]
    a: i64,
    inner: Inner,
}

#[derive(google_cloud_bigquery::write::ToRow)]
#[allow(dead_code)]
struct Inner {
    #[bigquery(rename = "b_col")]
    b: String,
}

#[test]
fn contract() {
    super::assert_to_row::<Row1>();
    super::assert_proto_element::<Inner>();
}
