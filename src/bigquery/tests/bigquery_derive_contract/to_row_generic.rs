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

//! Contract: `#[derive(ToRow)]` supports generic structs, including multiple
//! type parameters, `where` clauses, and defaulted type parameters. The derive
//! adds a `ProtoValue` bound on each type parameter. A type parameter inside
//! `Option<T>` or `Vec<T>` also needs a `ProtoElement` bound, which the user
//! writes.

use google_cloud_bigquery::write::{ProtoElement, ToRow};

#[derive(ToRow)]
#[allow(dead_code)]
struct Wrapper<T> {
    val: T,
}

#[derive(ToRow)]
#[allow(dead_code)]
struct Pair<A, B> {
    a: A,
    b: B,
}

#[derive(ToRow)]
#[allow(dead_code)]
struct WithWhere<T>
where
    T: std::fmt::Debug + Clone,
{
    val: T,
}

#[derive(ToRow)]
#[allow(dead_code)]
struct WithDefault<T = i64> {
    val: T,
}

#[derive(ToRow)]
#[allow(dead_code)]
struct Batch<T: ProtoElement> {
    first: Option<T>,
    items: Vec<T>,
}

#[test]
fn contract() {
    super::assert_to_row::<Wrapper<String>>();
    super::assert_to_row::<Pair<i64, String>>();
    super::assert_to_row::<WithWhere<String>>();
    super::assert_to_row::<WithDefault>();
    super::assert_to_row::<Batch<i64>>();
}
