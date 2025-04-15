// Copyright 2024 Google LLC
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

//! Defines traits and helpers to serialize query parameters.
//!
//! Query parameters in the Google APIs can be types other than strings and
//! integers. We need a helper to efficiently serialize parameters of different
//! types. We also want the generator to be relatively simple.
//!
//! The Rust SDK generator produces query parameters as optional fields in the
//! request object. The generator code can be simplified if all the query
//! parameters can be treated uniformly, without any conditionally generated
//! code to handle different types.
//!
//! This module defines some traits and helpers to simplify the code generator.
//!
//! The types are not intended for application developers to use. They are
//! public because we will generate many crates (roughly one per service), and
//! most of these crates will use these helpers.

/// [QueryParameter] is a trait representing types that can be used as a query
/// parameter.
pub trait QueryParameter {
    fn add(self, builder: reqwest::RequestBuilder, name: &str) -> reqwest::RequestBuilder;
}

impl QueryParameter for serde_json::Value {
    fn add(self, builder: reqwest::RequestBuilder, name: &str) -> reqwest::RequestBuilder {
        match self {
            Self::Object(object) => object.into_iter().fold(builder, |builder, (k, v)| {
                v.add(builder, format!("{name}.{k}").as_str())
            }),
            Self::Array(array) => array
                .into_iter()
                .fold(builder, |builder, v| v.add(builder, name)),
            Self::Null => builder,
            Self::String(s) => builder.query(&[(name, s)]),
            Self::Number(n) => builder.query(&[(name, format!("{n}"))]),
            Self::Bool(b) => builder.query(&[(name, b)]),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn split_query(r: &reqwest::Request) -> Vec<&str> {
        r.url()
            .query()
            .unwrap_or_default()
            .split("&")
            .filter(|p| !p.is_empty())
            .collect()
    }

    #[test]
    fn object() -> TestResult {
        let value = json!({
            "a": 123,
            "b": [123, 456, 789],
            "c": "123",
            "d": true,
            "e": {
                "f": "abc",
                "g": false,
                "h": {
                    "i": 42,
                }
            }
        });
        let builder = reqwest::Client::builder()
            .build()?
            .get("https://test.googleapis.com/v1/unused");
        let builder = value.add(builder, "name");
        let request = builder.build()?;
        assert_eq!(
            split_query(&request),
            vec![
                "name.a=123",
                "name.b=123",
                "name.b=456",
                "name.b=789",
                "name.c=123",
                "name.d=true",
                "name.e.f=abc",
                "name.e.g=false",
                "name.e.h.i=42",
            ]
        );
        Ok(())
    }

    #[test]
    fn array() -> TestResult {
        let value = json!([1, 3, 5, 7]);
        let builder = reqwest::Client::builder()
            .build()?
            .get("https://test.googleapis.com/v1/unused");
        let builder = value.add(builder, "name");
        let request = builder.build()?;
        assert_eq!(
            split_query(&request),
            vec!["name=1", "name=3", "name=5", "name=7"]
        );
        Ok(())
    }

    #[test]
    fn null() -> TestResult {
        let value = json!(null);
        let builder = reqwest::Client::builder()
            .build()?
            .get("https://test.googleapis.com/v1/unused");
        let builder = value.add(builder, "name");
        let request = builder.build()?;
        assert_eq!(split_query(&request), Vec::<&str>::new());
        Ok(())
    }

    #[test]
    fn string() -> TestResult {
        let value = json!("abc123");
        let builder = reqwest::Client::builder()
            .build()?
            .get("https://test.googleapis.com/v1/unused");
        let builder = value.add(builder, "name");
        let request = builder.build()?;
        assert_eq!(split_query(&request), vec!["name=abc123"]);
        Ok(())
    }

    #[test]
    fn number() -> TestResult {
        let value = json!(7.5);
        let builder = reqwest::Client::builder()
            .build()?
            .get("https://test.googleapis.com/v1/unused");
        let builder = value.add(builder, "name");
        let request = builder.build()?;
        assert_eq!(split_query(&request), vec!["name=7.5"]);
        Ok(())
    }

    #[test]
    fn boolean() -> TestResult {
        let value = json!(true);
        let builder = reqwest::Client::builder()
            .build()?
            .get("https://test.googleapis.com/v1/unused");
        let builder = value.add(builder, "name");
        let request = builder.build()?;
        assert_eq!(split_query(&request), vec!["name=true"]);
        Ok(())
    }
}
