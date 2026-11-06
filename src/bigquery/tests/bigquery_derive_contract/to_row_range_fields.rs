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

//! Contract: `#[derive(ToRow)]` accepts `Range<T>` fields for each `RANGE`
//! element type, alone, in `Option`, and in `Vec`.

use google_cloud_bigquery::datatypes::Range;
use google_cloud_bigquery::write::ToRow;
use google_cloud_type::model::{Date, DateTime};

#[derive(ToRow)]
#[allow(dead_code)]
struct Ranges {
    days: Range<Date>,
    maybe: Option<Range<DateTime>>,
    many: Vec<Range<wkt::Timestamp>>,
}

#[test]
fn contract() {
    super::assert_to_row::<Ranges>();
}
