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

/// A trait to simplify generated code for path fields
///
/// The trait is public to avoid a `warn(private_bounds)` on `try_match()`.
pub trait PathField {
    fn try_match(self, template: &[Segment]) -> Self;
    fn maybe_error(
        &self,
        template: &[Segment],
        expecting: &'static str,
    ) -> Option<SubstitutionFail>;
}

fn try_match_impl<'a>(value: Option<&'a str>, template: &[Segment]) -> Option<&'a str> {
    crate::routing_parameter::value(value, &[], template, &[])
}

impl PathField for Option<&str> {
    fn try_match(self, template: &[Segment]) -> Self {
        try_match_impl(self, template)
    }
    fn maybe_error(
        &self,
        template: &[Segment],
        expecting: &'static str,
    ) -> Option<SubstitutionFail> {
        match self {
            None | Some("") => Some(SubstitutionFail::UnsetExpecting(expecting)),
            Some(value) if try_match_impl(Some(value), template).is_none() => Some(
                SubstitutionFail::MismatchExpecting(value.to_string(), expecting),
            ),
            _ => None,
        }
    }
}

impl<T> PathField for Option<&T> {
    fn try_match(self, _template: &[Segment]) -> Self {
        // Note that non-string `T`s, when set, always match their `*` template.
        self
    }
    fn maybe_error(
        &self,
        _template: &[Segment],
        _expecting: &'static str,
    ) -> Option<SubstitutionFail> {
        match self {
            Some(_) => None,
            None => Some(SubstitutionFail::Unset),
        }
    }
}

/// Checks if a field matches a given path template
///
/// If it matches, it returns `value`. (Having a composable function simplifies
/// the generated code).
///
/// # Example - string
/// ```
/// # use google_cloud_gax_internal::path_parameter::try_match;
/// # use google_cloud_gax_internal::routing_parameter::Segment;
/// use Segment::{Literal, SingleWildcard};
/// let p = try_match(Some("projects/my-project"),
///     &[Literal("projects/"), SingleWildcard]);
/// assert_eq!(p, Some("projects/my-project"));
/// ```
///
/// # Example - numeric
/// ```
/// # use google_cloud_gax_internal::path_parameter::try_match;
/// # use google_cloud_gax_internal::routing_parameter::Segment;
/// use Segment::{Literal, SingleWildcard};
/// let p = try_match(Some(&12345), &[SingleWildcard]);
/// assert_eq!(p, Some(&12345));
/// ```
///
/// # Parameters
/// - `value` - the value of the field, as an optional reference
/// - `template` - segments to match the `value` against
pub fn try_match<T: PathField>(value: T, template: &[Segment]) -> T {
    value.try_match(template)
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
/// # use google_cloud_gax_internal::path_parameter::PathMismatchBuilder;
/// # use google_cloud_gax_internal::routing_parameter::Segment;
/// # // These are fields in the request.
/// # let parent: Option<&str> = None;
/// # let id: Option<&i32> = None;
///
/// // Make the builder
/// let builder = PathMismatchBuilder::default();
/// let builder = builder.maybe_add(
///     parent,
///     &[Segment::Literal("projects/"), Segment::SingleWildcard],
///     "parent",
///     "projects/*");
/// let builder = builder.maybe_add(
///     id,
///     &[Segment::SingleWildcard],
///     "id",
///     "*");
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
    pub fn maybe_add<T: PathField>(
        mut self,
        value: T,
        template: &[Segment],
        field_name: &'static str,
        expecting: &'static str,
    ) -> Self {
        if let Some(problem) = value.maybe_error(template, expecting) {
            self.0.subs.push(SubstitutionMismatch {
                field_name,
                problem,
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
    use rpc::model::Code;
    use std::error::Error as _;
    use test_case::test_case;

    #[test_case(Some("projects/my-project"), Some("projects/my-project"))]
    #[test_case(None, None)]
    #[test_case(Some(""), None)]
    #[test_case(Some("projects/"), None)]
    #[test_case(Some("projects//"), None; "also bad")]
    #[test_case(Some("projects/my-project/"), None)]
    #[test_case(Some("projects/my-project/locations/my-location"), None)]
    fn try_match_string(input: Option<&str>, expected: Option<&str>) {
        let p = try_match(
            input,
            &[Segment::Literal("projects/"), Segment::SingleWildcard],
        );
        assert_eq!(p, expected);
    }

    #[test_case(Some(&12345), Some(&12345))]
    #[test_case(Some(&1234.5), Some(&1234.5))]
    #[test_case(Some(&true), Some(&true))]
    #[test_case(Some(&Code::Unknown), Some(&Code::Unknown))]
    #[test_case(None::<&i32>, None)]
    #[test_case(None::<&f32>, None)]
    #[test_case(None::<&bool>, None)]
    #[test_case(None::<&Code>, None)]
    fn try_match_other<T>(input: T, expected: T)
    where
        T: PathField + std::fmt::Debug + PartialEq,
    {
        let p = try_match(input, &[Segment::SingleWildcard]);
        assert_eq!(p, expected);
    }

    #[test]
    fn path_mismatch_builder_string() {
        let builder = PathMismatchBuilder::default();
        let builder =
            builder.maybe_add(Some("matches"), &[Segment::SingleWildcard], "matches", "*");
        let builder = builder.maybe_add(None::<&str>, &[Segment::SingleWildcard], "unset", "*");
        let builder = builder.maybe_add(Some(""), &[Segment::SingleWildcard], "empty", "*");
        let builder = builder.maybe_add(
            Some("match_fail"),
            &[Segment::Literal("projects/"), Segment::SingleWildcard],
            "match_fail",
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
        let builder = builder.maybe_add(Some(&12345), &[Segment::SingleWildcard], "set_id", "*");
        let builder = builder.maybe_add(None::<&u64>, &[Segment::SingleWildcard], "unset_id", "*");
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
    fn missing_error() {
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
