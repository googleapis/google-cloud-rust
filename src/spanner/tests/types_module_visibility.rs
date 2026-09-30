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

//! Integration tests that exercise the public `google_cloud_spanner::types`
//! module from an external-crate perspective.
//!
//! If the `types` module is ever declared `pub(crate)` again, these tests
//! fail to compile with `error[E0603]: module 'types' is private`. That
//! compile-time failure is the regression guard: it locks the public
//! constructors (`types::timestamp()`, `types::string()`, `types::int64()`,
//! etc.) reachable from external crates that depend on
//! `google-cloud-spanner`.

use google_cloud_spanner::statement::Statement;
use google_cloud_spanner::types;
use google_cloud_spanner::value::{Date, Timestamp};
#[cfg(feature = "time")]
use time::OffsetDateTime;

#[test]
fn external_uses_typed_param_constructors() {
    let statement = Statement::builder("SELECT @ts, @s, @n, @d")
        .add_typed_param("ts", Timestamp::clamp(1_700_000_000, 0), types::timestamp())
        .add_typed_param("s", "hello".to_string(), types::string())
        .add_typed_param("n", 42i64, types::int64())
        .add_typed_param(
            "d",
            Date::new().set_year(2025).set_month(1).set_day(1),
            types::date(),
        )
        .build();

    assert!(statement.sql().contains("SELECT @ts, @s, @n, @d"));

    #[cfg(feature = "time")]
    {
        let statement = Statement::builder("SELECT @ts")
            .add_typed_param("ts", OffsetDateTime::now_utc(), types::timestamp())
            .build();
        assert!(statement.sql().contains("SELECT @ts"));
    }
}

#[test]
fn external_reaches_all_primitive_constructors() {
    // Touch each documented primitive constructor so any future visibility
    // regression that hides even one of them fails compilation here.
    let _ = types::int64();
    let _ = types::string();
    let _ = types::timestamp();
    let _ = types::bool();
    let _ = types::bytes();
    let _ = types::date();
    let _ = types::float32();
    let _ = types::float64();
    let _ = types::json();
    let _ = types::numeric();
    let _ = types::uuid();
    let _ = types::interval();
    let _ = types::pg_numeric();
    let _ = types::pg_jsonb();
    let _ = types::pg_oid();
    let _ = types::array(types::int64());
}
