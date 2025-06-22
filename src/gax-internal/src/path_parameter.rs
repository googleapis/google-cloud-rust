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

//! Handling of missing path parameters.
//!
//! Parameters used to build the request path (aka 'path parameters') are
//! required. But for complicated reasons they may appear in optional fields.
//! The generator needs to return an error when the parameter is missing, and
//! a small helper function makes the generated code easier to read.

use crate::routing_parameter::Segment;

/// Checks if a string field matches a given path template
///
/// If it matches, it returns `Some(value)`. (Having a composable function
/// simplifies the generated code).
///
/// # Example
/// ```
/// # use google_cloud_gax_internal::path_parameter::try_match;
/// # use google_cloud_gax_internal::routing_parameter::Segment;
/// use Segment::{Literal, SingleWildcard};
/// let p = try_match("projects/my-project",
///     &[Literal("projects/"), SingleWildcard]);
/// assert_eq!(p, Some("projects/my-project"));
/// ```
///
/// # Parameters
/// - `value` - the value of the string field
/// - `template` - segments to match the `value` against
pub fn try_match<'a>(value: &'a str, template: &[Segment]) -> Option<&'a str> {
    crate::routing_parameter::value(Some(value), &[], template, &[])
}

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("missing required parameter {0}")]
    MissingRequiredParameter(String),
}

pub fn missing(name: &str) -> gax::error::Error {
    gax::error::Error::binding(Error::MissingRequiredParameter(name.to_string()))
}

#[cfg(test)]
mod tests {
    use super::{Error, Segment};
    use std::error::Error as _;
    use test_case::test_case;

    #[test_case("projects/my-project", Some("projects/my-project"))]
    #[test_case("", None)]
    #[test_case("projects/", None)]
    #[test_case("projects/my-project/", None)]
    #[test_case("projects/my-project/locations/my-location", None)]
    fn try_match(input: &str, expected: Option<&str>) {
        let p = super::try_match(
            input,
            &[Segment::Literal("projects/"), Segment::SingleWildcard],
        );
        assert_eq!(p, expected);
    }

    #[test]
    fn missing() {
        let e = super::missing("abc123");
        let fmt = format!("{e}");
        assert!(fmt.contains("abc123"), "{e:?}");
        let source = e.source().and_then(|e| e.downcast_ref::<Error>());
        assert!(
            matches!(source, Some(Error::MissingRequiredParameter(p)) if p == "abc123"),
            "{e:?}"
        );
    }
}
