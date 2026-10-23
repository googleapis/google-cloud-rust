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

//! Contract: a `#[derive(ToRow)]` struct may have fields whose types are other
//! `#[derive(ToRow)]` structs, directly, in `Option`, and in `Vec`. A generic
//! struct may hold another generic struct without a `'static` bound.

use google_cloud_bigquery::write::{ProtoValue, ToRow};

#[derive(ToRow)]
#[allow(dead_code)]
struct Address {
    city: String,
    zip: String,
}

#[derive(ToRow)]
#[allow(dead_code)]
struct Customer {
    name: String,
    address: Address,
    billing: Option<Address>,
    history: Vec<Address>,
}

#[derive(ToRow)]
#[allow(dead_code)]
struct Inner<T> {
    value: T,
}

#[derive(ToRow)]
#[allow(dead_code)]
struct Outer<T> {
    inner: Inner<T>,
    items: Vec<Inner<T>>,
}

/// Holds for any `T: ProtoValue`, not only for `'static` types.
fn assert_outer<T: ProtoValue>() {
    super::assert_to_row::<Outer<T>>();
}

#[test]
fn contract() {
    super::assert_to_row::<Customer>();
    super::assert_proto_element::<Address>();
    assert_outer::<i64>();
}
