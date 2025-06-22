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
use gax::error::binding::{PathMismatch, SubstitutionFail, SubstitutionMismatch};

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

/// Helper to create a `PathMismatch`
///
/// A path may have multiple variable substitutions, any of which can fail. We
/// want to report all such failures.
///
/// This class offers convenient helpers to conditionally add substitutions only
/// if the substitution failed. This simplifies the generated code.
///
/// Example:
/// ```
/// # // These are fields in the request.
/// # let parent: Option<&str> = None;
/// # let id: Option<&i32> = None;
///
/// // Make the builder
/// let builder = PathMismatchBuilder::default();
/// let builder = builder.maybe_add_match_error(
///     parent,
///     "parent",
///     &[Segment::Literal("projects/"), Segment::SingleWildcard],
///     "projects/*");
/// let builder = builder.maybe_add_unset_error(id, "id");
/// // etc.
///
/// // Create the `PathMismatch`
/// let pm = builder.build();
/// ```
#[derive(Debug, Default)]
pub struct PathMismatchBuilder(PathMismatch);

impl PathMismatchBuilder {
    /// Tries to match `value` against the expected `template`. The error is
    /// recorded, if the match is unsuccessful.
    ///
    /// Both `None` and the empty string are classified as unset.
    pub fn maybe_add_error_string(
        mut self,
        value: Option<&str>,
        field_name: &'static str,
        template: &[Segment],
        expecting: &'static str,
    ) -> Self {
        match value {
            None | Some("") => {
                self.0.subs.push(SubstitutionMismatch {
                    field_name,
                    problem: SubstitutionFail::UnsetExpecting(expecting),
                });
            }
            Some(actual) if try_match(actual, template).is_none() => {
                self.0.subs.push(SubstitutionMismatch {
                    field_name,
                    problem: SubstitutionFail::MismatchExpecting(actual.to_string(), expecting),
                });
            }
            _ => {}
        };
        self
    }

    /// Records an unset error if `value` is not set.
    ///
    /// This function is used for integral `T`s, which, when set, always match
    /// their `*` template.
    pub fn maybe_add_error_other<T>(mut self, value: Option<&T>, field_name: &'static str) -> Self {
        if value.is_none() {
            self.0.subs.push(SubstitutionMismatch {
                field_name,
                problem: SubstitutionFail::Unset,
            });
        }
        self
    }

    pub fn build(self) -> PathMismatch {
        self.0
    }
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
    use super::*;
    use std::error::Error as _;
    use test_case::test_case;

    #[test_case("projects/my-project", Some("projects/my-project"))]
    #[test_case("", None)]
    #[test_case("projects/", None)]
    #[test_case("projects/my-project/", None)]
    #[test_case("projects/my-project/locations/my-location", None)]
    fn test_try_match(input: &str, expected: Option<&str>) {
        let p = try_match(
            input,
            &[Segment::Literal("projects/"), Segment::SingleWildcard],
        );
        assert_eq!(p, expected);
    }

    #[test]
    fn path_mismatch_builder_string() {
        let builder = PathMismatchBuilder::default();
        let builder = builder.maybe_add_error_string(
            Some("matches"),
            "matches",
            &[Segment::SingleWildcard],
            "*",
        );
        let builder =
            builder.maybe_add_error_string(None, "unset", &[Segment::SingleWildcard], "*");
        let builder =
            builder.maybe_add_error_string(Some(""), "empty", &[Segment::SingleWildcard], "*");
        let builder = builder.maybe_add_error_string(
            Some("match_fail"),
            "match_fail",
            &[Segment::Literal("projects/"), Segment::SingleWildcard],
            "projects/*",
        );
        let pm = builder.build();

        let expected = vec![
            SubstitutionMismatch {
                field_name: "unset",
                problem: SubstitutionFail::UnsetExpecting("*"),
            },
            SubstitutionMismatch {
                field_name: "empty",
                problem: SubstitutionFail::UnsetExpecting("*"),
            },
            SubstitutionMismatch {
                field_name: "match_fail",
                problem: SubstitutionFail::MismatchExpecting(
                    "match_fail".to_string(),
                    "projects/*",
                ),
            },
        ];

        assert_eq!(pm.subs, expected);
    }

    #[test]
    fn path_mismatch_builder_other() {
        let builder = PathMismatchBuilder::default();
        let builder = builder.maybe_add_error_other(Some(&12345), "set_id");
        let builder = builder.maybe_add_error_other(None::<&u64>, "unset_id");
        let pm = builder.build();

        assert_eq!(
            pm.subs,
            vec![SubstitutionMismatch {
                field_name: "unset_id",
                problem: SubstitutionFail::Unset,
            }]
        );
    }

    #[test]
    fn test_missing() {
        let e = missing("abc123");
        let fmt = format!("{e}");
        assert!(fmt.contains("abc123"), "{e:?}");
        let source = e.source().and_then(|e| e.downcast_ref::<Error>());
        assert!(
            matches!(source, Some(Error::MissingRequiredParameter(p)) if p == "abc123"),
            "{e:?}"
        );
    }
}
