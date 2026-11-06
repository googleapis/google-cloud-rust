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

//! Contract: `#[derive(ToRow)]` accepts `pub`, `pub(crate)`, and private
//! structs and fields, and other outer attributes before or after the
//! `#[derive]`. Renamed and plain fields can be mixed, and other field
//! attributes, including doc comments, can come before or after
//! `#[bigquery(rename = "...")]`.

mod inner {
    use google_cloud_bigquery::write::ToRow;

    #[allow(dead_code)]
    #[derive(ToRow)]
    #[allow(clippy::all)]
    pub struct PubRow {
        pub id: i64,
        pub(crate) name: String,
        #[bigquery(rename = "secret_col")]
        secret: String,
    }
}

#[derive(google_cloud_bigquery::write::ToRow)]
#[allow(dead_code)]
pub(crate) struct MixedRow {
    plain_a: i64,
    /// Doc comments are attributes too.
    #[bigquery(rename = "renamed_b")]
    b: String,
    #[bigquery(rename = "renamed_c")]
    #[allow(unused)]
    c: Option<f64>,
    plain_d: bool,
}

#[test]
fn contract() {
    super::assert_to_row::<inner::PubRow>();
    super::assert_to_row::<MixedRow>();
}
