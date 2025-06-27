// Copyright 2025 Google LLC
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

/// A failure to determine the request [URI].
///
/// Some RPCs correspond to multiple URIs. The contents of the request determine
/// which URI is used. The client library considers all possible URIs, and only
/// returns an error if no URIs work.
///
/// The client cannot match a URI when a required parameter is missing, or when
/// it is set to an invalid format.
///
/// For more details on the specification, see: [AIP-127].
///
/// [aip-127]: https://google.aip.dev/127
/// [uri]: https://clouddocs.f5.com/api/irules/HTTP__uri.html
#[derive(thiserror::Error, Debug, PartialEq)]
pub struct BindingError {
    /// A list of all the paths considered, and why exactly the binding failed
    /// for each
    pub paths: Vec<PathMismatch>,
}

/// A failure to bind to a specific [URI].
///
/// The client cannot match a URI when a required parameter is missing, or when
/// it is set to an invalid format.
///
/// [uri]: https://clouddocs.f5.com/api/irules/HTTP__uri.html
#[derive(Debug, Default, PartialEq)]
pub struct PathMismatch {
    /// All missing or misformatted fields needed to bind to this path
    pub subs: Vec<SubstitutionMismatch>,
}

/// Ways substituting a variable from a request into a [URI] can fail.
///
/// [uri]: https://clouddocs.f5.com/api/irules/HTTP__uri.html
#[derive(Debug, PartialEq)]
pub enum SubstitutionFail {
    /// A required field was not set
    Unset,
    /// A required field of a certain format was not set
    UnsetExpecting(&'static str),
    /// A required field was set, but to an invalid format
    ///
    /// # Parameters
    ///
    /// - self.0 - the actual value of the field
    /// - self.1 - the expected format of the field
    MismatchExpecting(String, &'static str),
}

/// A failure to substitute a variable from a request into a [URI].
///
/// [uri]: https://clouddocs.f5.com/api/irules/HTTP__uri.html
#[derive(Debug, PartialEq)]
pub struct SubstitutionMismatch {
    /// The name of the field that was not substituted.
    ///
    /// Nested fields are '.'-separated.
    pub field_name: &'static str,
    /// Why the substitution failed.
    pub problem: SubstitutionFail,
}

impl std::fmt::Display for SubstitutionMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.problem {
            SubstitutionFail::Unset => {
                write!(f, "field `{}` needs to be set.", self.field_name)
            }
            SubstitutionFail::UnsetExpecting(expected) => {
                write!(
                    f,
                    "field `{}` needs to be set and match: '{}'",
                    self.field_name, expected
                )
            }
            SubstitutionFail::MismatchExpecting(actual, expected) => {
                write!(
                    f,
                    "field `{}` should match: '{}'; found: '{}'",
                    self.field_name, expected, actual
                )
            }
        }
    }
}

impl std::fmt::Display for PathMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (i, sub) in self.subs.iter().enumerate() {
            if i != 0 {
                write!(f, " AND ")?;
            }
            write!(f, "{sub}")?;
        }
        Ok(())
    }
}

impl std::fmt::Display for BindingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "at least one of the conditions must be met: ")?;
        for (i, sub) in self.paths.iter().enumerate() {
            if i != 0 {
                write!(f, " OR ")?;
            }
            write!(f, "({}) {}", i + 1, sub)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn fmt_path_mismatch() {
        let pm = PathMismatch {
            subs: vec![
                SubstitutionMismatch {
                    field_name: "parent",
                    problem: SubstitutionFail::MismatchExpecting(
                        "project-id-only".to_string(),
                        "projects/*",
                    ),
                },
                SubstitutionMismatch {
                    field_name: "location",
                    problem: SubstitutionFail::UnsetExpecting("locations/*"),
                },
                SubstitutionMismatch {
                    field_name: "id",
                    problem: SubstitutionFail::Unset,
                },
            ],
        };

        let fmt = format!("{pm}");
        let clauses: Vec<&str> = fmt.split(" AND ").collect();
        assert!(clauses.len() == 3, "{fmt}");
        let c0 = clauses[0];
        assert!(
            c0.contains("parent")
                && !c0.contains("needs to be set")
                && c0.contains("should match")
                && c0.contains("projects/*")
                && c0.contains("found")
                && c0.contains("project-id-only"),
            "{c0}"
        );
        let c1 = clauses[1];
        assert!(
            c1.contains("location")
                && c1.contains("needs to be set")
                && c1.contains("locations/*")
                && !c1.contains("found"),
            "{c1}"
        );
        let c2 = clauses[2];
        assert!(
            c2.contains("id") && c2.contains("needs to be set") && !c2.contains("found"),
            "{c2}"
        );
    }

    #[test]
    fn fmt_binding_error() {
        let e = BindingError {
            paths: vec![
                PathMismatch {
                    subs: vec![SubstitutionMismatch {
                        field_name: "parent",
                        problem: SubstitutionFail::MismatchExpecting(
                            "project-id-only".to_string(),
                            "projects/*",
                        ),
                    }],
                },
                PathMismatch {
                    subs: vec![SubstitutionMismatch {
                        field_name: "location",
                        problem: SubstitutionFail::UnsetExpecting("locations/*"),
                    }],
                },
                PathMismatch {
                    subs: vec![SubstitutionMismatch {
                        field_name: "id",
                        problem: SubstitutionFail::Unset,
                    }],
                },
            ],
        };
        let fmt = format!("{e}");
        assert!(fmt.contains("one of the conditions must be met"), "{fmt}");
        let clauses: Vec<&str> = fmt.split(" OR ").collect();
        assert!(clauses.len() == 3, "{fmt}");
        let c0 = clauses[0];
        assert!(
            c0.contains("(1)")
                && c0.contains("parent")
                && c0.contains("should match")
                && c0.contains("projects/*")
                && c0.contains("project-id-only"),
            "{c0}"
        );
        let c1 = clauses[1];
        assert!(
            c1.contains("(2)") && c1.contains("location") && c1.contains("locations/*"),
            "{c1}"
        );
        let c2 = clauses[2];
        assert!(
            c2.contains("(3)") && c2.contains("id") && c2.contains("needs to be set"),
            "{c2}"
        );
    }
}
