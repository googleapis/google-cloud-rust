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

//! Helper functions to match routing parameters.

use percent_encoding::NON_ALPHANUMERIC;

/// Find a routing parameter value in `haystack` using the (decomposed) template.
///
/// # Example
/// ```
/// # use google_cloud_gax_internal::routing_parameter::*;
/// use Segment::{Literal, SingleWildcard, TrailingMultiWildcard};
/// let matching = value(
///     Some("projects/p/locations/l/instances/i/tables/t"),
///     &[Literal("projects/"), SingleWildcard, Literal("/locations/"), SingleWildcard, Literal("/")],
///     &[Literal("instances/"), SingleWildcard],
///     &[Literal("/tables"), TrailingMultiWildcard]);
/// assert_eq!(matching, Some("instances/i"));
/// ```
///
/// # Parameters
/// - `haystack` - a string where to find the path template.
/// - `prefix` - the initial segments in the template that must match,
///   and are not included in the result.
/// - `matching` - the segments in the template that must match and **are**
///   included in the result.
/// - `suffix` - the trailing segments in the template that must match, and
///   are not include in the result.
pub fn value<'h>(
    haystack: Option<&'h str>,
    prefix: &[Segment],
    matching: &[Segment],
    suffix: &[Segment],
) -> Option<&'h str> {
    let haystack = haystack?; // Consuming Option<> simplifies code generation
    let mut remains = haystack;
    let mut start = 0_usize;
    let mut end = 0_usize;

    for needle in prefix {
        let count = needle.match_size(remains)?;
        start += count;
        end += count;
        remains = &remains[count..];
    }
    for needle in matching {
        let count = needle.match_size(remains)?;
        end += count;
        remains = &remains[count..];
    }
    for needle in suffix {
        let count = needle.match_size(remains)?;
        remains = &remains[count..];
    }
    if !remains.is_empty() || start == end {
        return None;
    }
    Some(&haystack[start..end])
}

/// Format a list of routing parameter key value pairs.
///
/// ```
/// # use google_cloud_gax_internal::routing_parameter::*;
/// let params = format(&[
///     Some(("bucket", "projects/_/buckets/d")),
///     None,
///     Some(("source_bucket", "projects/_/buckets/s")),
///     None,
/// ]);
/// assert_eq!(
///     params,
///     "bucket=projects%2F_%2Fbuckets%2Fd&source_bucket=projects%2F_%2Fbuckets%2Fs");
/// ```
pub fn format(matches: &[Option<(&str, &str)>]) -> String {
    matches
        .iter()
        .flatten()
        .map(|(k, v)| format!("{}={}", enc(k), enc(v)))
        .fold(String::new(), |acc, v| {
            if acc.is_empty() { v } else { acc + "&" + &v }
        })
}

/// Represents a segment in the routing parameter path templates.
///
/// This represents a segment in a path template as defined in:
///
///   <https://google.aip.dev/client-libraries/4222#path_template-syntax>
///
/// We use different branches for `**` when it is a complete string vs. the last
/// segment. As described in AIP-4222 multi-segment wildcards match different
/// things depending on their position.
pub enum Segment {
    /// A literal string, matches its value.
    Literal(&'static str),
    // Matches any value satisfying `[^/]+`.
    SingleWildcard,
    // Matches any value, including empty strings.
    MultiWildcard,
    // Matches any value satisfying `([:/].*)?`
    TrailingMultiWildcard,
}

impl Segment {
    pub(crate) fn match_size(&self, haystack: &str) -> Option<usize> {
        match self {
            Self::Literal(lit) => haystack.starts_with(lit).then_some(lit.len()),
            Self::SingleWildcard => {
                let i = haystack.find('/').unwrap_or(haystack.len());
                (i != 0).then_some(i)
            }
            Self::MultiWildcard => Some(haystack.len()),
            Self::TrailingMultiWildcard => {
                if haystack.is_empty() {
                    return Some(0_usize);
                }
                if haystack.starts_with('/') || haystack.starts_with(':') {
                    return Some(haystack.len());
                }
                None
            }
        }
    }
}

/// The set of characters that are percent encoded.
///
/// The set is defined, by reference, in [AIP-4222]:
///
/// > Both the key and the value must be URL-encoded per [RFC 6570 3.2.2]
///
/// That section in the RFC says:
///
/// > For each defined variable in the variable-list, perform variable
/// > expansion, as defined in Section 3.2.1, with the allowed characters
/// > being those in the unreserved set.
///
/// The "unreseved set" is defined in [section 1.5][RFC 6570 1.5] of the same
/// RFC:
///
/// > unreserved     =  ALPHA / DIGIT / "-" / "." / "_" / "~"
///
/// Which is how we arrive to this this constant.
///
/// [RFC 6570 3.3.2]: https://datatracker.ietf.org/doc/html/rfc6570#section-3.2.2
/// [RFC 6570 1.5]: https://datatracker.ietf.org/doc/html/rfc6570#section-1.5
const UNRESERVED: percent_encoding::AsciiSet = NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

/// Percent encode a string.
///
/// A very short name as this is a private function, and only exists to simplify
/// testing.
fn enc(value: &str) -> percent_encoding::PercentEncode<'_> {
    percent_encoding::utf8_percent_encode(value, &UNRESERVED)
}

#[cfg(test)]
mod tests {
    use super::*;
    use Segment::*;
    use test_case::test_case;

    const TABLE_NAME: &str = "projects/proj_foo/instances/instance_bar/table/table_baz";
    const APP_PROFILE_ID: &str = "profiles/prof_qux";

    struct Request {
        table_name: String,
        app_profile_id: String,
    }

    // This is the code that I expect we will generate for each request.
    // Note that the matches would be generated in reverse order, so the
    // last match wins. Also, literals should be optimized to add the '/' to the
    // body of the literal.
    fn request_body(req: Request) -> String {
        format(&[
            // match for "table_location"
            value(
                Some(&req.table_name),
                &[],
                &[
                    Literal("regions/"),
                    SingleWildcard,
                    Literal("/zones/"),
                    SingleWildcard,
                ],
                &[Literal("/tables/"), SingleWildcard],
            )
            .or_else(|| {
                value(
                    Some(&req.table_name),
                    &[Literal("projects/"), SingleWildcard, Literal("/")],
                    &[Literal("instances/"), SingleWildcard],
                    &[Literal("/tables/"), SingleWildcard],
                )
            })
            .map(|v| ("table_location", v)),
            // match for "routing_id"
            value(
                Some(&req.app_profile_id),
                &[Literal("profiles/")],
                &[SingleWildcard],
                &[],
            )
            .or_else(|| value(Some(&req.app_profile_id), &[], &[MultiWildcard], &[]))
            .or_else(|| {
                value(
                    Some(&req.table_name),
                    &[],
                    &[Literal("projects/"), SingleWildcard],
                    &[TrailingMultiWildcard],
                )
            })
            .map(|v| ("routing_id", v)),
        ])
    }

    #[test_case("", "", ""; "empty")]
    #[test_case("", "profiles/q", "routing_id=q"; "match #3 wins")]
    #[test_case("", "thingy/q/child/c", "routing_id=thingy%2Fq%2Fchild%2Fc"; "match #2 wins")]
    #[test_case("projects/p/instances/i", "", "routing_id=projects%2Fp"; "match #1 wins")]
    #[test_case("projects/p/instances/i/tables/t", "", "table_location=instances%2Fi&routing_id=projects%2Fp"; "one field matches 2 variables")]
    #[test_case("projects/p/instances/i/tables/t", "profiles/q", "table_location=instances%2Fi&routing_id=q"; "multiple variables")]
    #[test_case("projects/p/instances/i/tables/t", "thingy/q/child/c", "table_location=instances%2Fi&routing_id=thingy%2Fq%2Fchild%2Fc"; "multiple variables skipping one template")]
    fn simulated_request(table_name: &str, app_profile_id: &str, want: &str) {
        let got = request_body(Request {
            table_name: table_name.into(),
            app_profile_id: app_profile_id.into(),
        });
        assert_eq!(got.as_str(), want);
    }

    #[test_case("", None; "empty")]
    #[test_case("projects/p/instances/i/tables/t", Some("instances/i"); "success")]
    #[test_case("projects/p/instances/i/tables/t/extra", None; "too much suffix")]
    #[test_case("extra/projects/p/instances/i/tables/t", None; "too much prefix")]
    #[test_case("projects/p/instances//tables/t", None; "empty match")]
    #[test_case("projects/p/i/tables/t", None; "missing keyword")]
    #[test_case("projects/p/instances/i", None; "missing suffix")]
    #[test_case("instances/i/tables/i", None; "missing prefix")]
    fn single_matches(input: &str, want: Option<&str>) {
        let got = value(
            Some(input),
            &[Literal("projects/"), SingleWildcard, Literal("/")],
            &[Literal("instances/"), SingleWildcard],
            &[Literal("/tables/"), SingleWildcard],
        );
        assert_eq!(got, want);
    }

    #[test_case("", None; "empty")]
    #[test_case("projects/p/instances/i/tables/t", Some("instances/i/tables/t"); "success")]
    #[test_case("projects/p/instances/i/tables/t/extra", Some("instances/i/tables/t/extra"); "with extra")]
    #[test_case("projects/p/instances/i/tables", Some("instances/i/tables"); "missing separator")]
    #[test_case("projects/p/instances/i/tables/", Some("instances/i/tables/"); "empty trailing segment")]
    fn matching_multi_segment(input: &str, want: Option<&str>) {
        let got = value(
            Some(input),
            &[Literal("projects/"), SingleWildcard, Literal("/")],
            &[
                Literal("instances/"),
                SingleWildcard,
                Literal("/tables"),
                TrailingMultiWildcard,
            ],
            &[],
        );
        assert_eq!(got, want);
    }

    #[test_case("", None; "empty")]
    #[test_case("projects/p/instances/i/tables/t", Some("projects/p/instances/i/tables/t"); "success")]
    #[test_case("projects/p/instances/i/tables/t/extra", Some("projects/p/instances/i/tables/t/extra"); "with colon extra")]
    #[test_case("projects/p/instances/i/tables/t:extra", Some("projects/p/instances/i/tables/t:extra"); "with slash extra")]
    #[test_case("projects/p/instances/i/tables", Some("projects/p/instances/i/tables"); "missing separator")]
    #[test_case("projects/p/instances/i/tables/", Some("projects/p/instances/i/tables/"); "empty trailing multi segment")]
    #[test_case("projects/p/instances/i/tables-abc123", None; "bad separator on trailing multi segment")]
    fn matching_wildcard_then_multi_segment(input: &str, want: Option<&str>) {
        let got = value(
            Some(input),
            &[],
            &[
                Literal("projects/"),
                SingleWildcard,
                Literal("/instances/"),
                SingleWildcard,
                Literal("/tables"),
                TrailingMultiWildcard,
            ],
            &[],
        );
        assert_eq!(got, want);
    }

    #[test]
    fn example1() {
        let matched = value(Some(APP_PROFILE_ID), &[], &[MultiWildcard], &[]);
        assert_eq!(matched, Some("profiles/prof_qux"));
    }

    #[test]
    fn example2() {
        let matched = value(Some(APP_PROFILE_ID), &[], &[MultiWildcard], &[]);
        assert_eq!(matched, Some("profiles/prof_qux"));
    }

    #[test]
    fn example3a() {
        let matched = value(
            Some(TABLE_NAME),
            &[],
            &[
                Literal("projects"),
                Literal("/"),
                SingleWildcard,
                Literal("/"),
                Literal("instances"),
                Literal("/"),
                SingleWildcard,
                TrailingMultiWildcard,
            ],
            &[],
        );
        assert_eq!(
            matched,
            Some("projects/proj_foo/instances/instance_bar/table/table_baz")
        );
    }

    #[test]
    fn example3b() {
        let matched = value(
            Some(TABLE_NAME),
            &[],
            &[
                Literal("regions"),
                Literal("/"),
                SingleWildcard,
                Literal("zones"),
                Literal("/"),
                SingleWildcard,
                TrailingMultiWildcard,
            ],
            &[],
        );
        assert_eq!(matched, None);
    }

    #[test]
    fn example3c() {
        let matched = value(
            Some(TABLE_NAME),
            &[],
            &[
                Literal("regions"),
                Literal("/"),
                SingleWildcard,
                Literal("zones"),
                Literal("/"),
                SingleWildcard,
                TrailingMultiWildcard,
            ],
            &[],
        );
        assert_eq!(matched, None);
        let matched = value(
            Some(TABLE_NAME),
            &[],
            &[
                Literal("projects"),
                Literal("/"),
                SingleWildcard,
                Literal("/"),
                Literal("instances"),
                Literal("/"),
                SingleWildcard,
                TrailingMultiWildcard,
            ],
            &[],
        );
        assert_eq!(
            matched,
            Some("projects/proj_foo/instances/instance_bar/table/table_baz")
        );
    }

    #[test]
    fn example4() {
        let matched = value(
            Some(TABLE_NAME),
            &[],
            &[Literal("projects"), Literal("/"), SingleWildcard],
            &[TrailingMultiWildcard],
        );
        assert_eq!(matched, Some("projects/proj_foo"));
    }

    #[test]
    fn example5() {
        let matched = value(
            Some(TABLE_NAME),
            &[],
            &[Literal("projects"), Literal("/"), SingleWildcard],
            &[TrailingMultiWildcard],
        );
        assert_eq!(matched, Some("projects/proj_foo"));
        let matched = value(
            Some(TABLE_NAME),
            &[],
            &[
                Literal("projects"),
                Literal("/"),
                SingleWildcard,
                Literal("/"),
                Literal("instances"),
                Literal("/"),
                SingleWildcard,
            ],
            &[TrailingMultiWildcard],
        );
        assert_eq!(matched, Some("projects/proj_foo/instances/instance_bar"));
    }

    #[test]
    fn example6a() {
        let matched = value(
            Some(TABLE_NAME),
            &[],
            &[Literal("projects"), Literal("/"), SingleWildcard],
            &[
                Literal("/"),
                Literal("instances"),
                Literal("/"),
                SingleWildcard,
                TrailingMultiWildcard,
            ],
        );
        assert_eq!(matched, Some("projects/proj_foo"));
        let matched = value(
            Some(TABLE_NAME),
            &[
                Literal("projects"),
                Literal("/"),
                SingleWildcard,
                Literal("/"),
            ],
            &[Literal("instances"), Literal("/"), SingleWildcard],
            &[TrailingMultiWildcard],
        );
        assert_eq!(matched, Some("instances/instance_bar"));
    }

    #[test]
    fn example6b() {
        let matched = value(
            Some(TABLE_NAME),
            &[],
            &[Literal("projects"), Literal("/"), SingleWildcard],
            &[TrailingMultiWildcard],
        );
        assert_eq!(matched, Some("projects/proj_foo"));
        let matched = value(
            Some(TABLE_NAME),
            &[
                Literal("projects"),
                Literal("/"),
                SingleWildcard,
                Literal("/"),
            ],
            &[Literal("instances"), Literal("/"), SingleWildcard],
            &[TrailingMultiWildcard],
        );
        assert_eq!(matched, Some("instances/instance_bar"));
    }

    #[test]
    fn example7() {
        let matched = value(
            Some(TABLE_NAME),
            &[],
            &[Literal("projects"), Literal("/"), SingleWildcard],
            &[TrailingMultiWildcard],
        );
        assert_eq!(matched, Some("projects/proj_foo"));
        let matched = value(Some(APP_PROFILE_ID), &[], &[MultiWildcard], &[]);
        assert_eq!(matched, Some("profiles/prof_qux"));
    }

    #[test]
    fn example8() {
        let matched = value(
            Some(TABLE_NAME),
            &[],
            &[Literal("projects"), Literal("/"), SingleWildcard],
            &[TrailingMultiWildcard],
        );
        assert_eq!(matched, Some("projects/proj_foo"));
        let matched = value(
            Some(TABLE_NAME),
            &[],
            &[Literal("regions"), Literal("/"), SingleWildcard],
            &[TrailingMultiWildcard],
        );
        assert_eq!(matched, None);
        let matched = value(Some(APP_PROFILE_ID), &[], &[MultiWildcard], &[]);
        assert_eq!(matched, Some("profiles/prof_qux"));
    }

    #[test]
    fn example9() {
        let matched = value(
            Some(TABLE_NAME),
            &[
                Literal("projects"),
                Literal("/"),
                SingleWildcard,
                Literal("/"),
            ],
            &[Literal("instances"), Literal("/"), SingleWildcard],
            &[Literal("/"), Literal("table"), Literal("/"), SingleWildcard],
        );
        assert_eq!(matched, Some("instances/instance_bar"));
        let matched = value(
            Some(TABLE_NAME),
            &[],
            &[
                Literal("regions"),
                Literal("/"),
                SingleWildcard,
                Literal("/"),
                Literal("zones"),
                Literal("/"),
                SingleWildcard,
            ],
            &[Literal("tables"), Literal("/"), SingleWildcard],
        );
        assert_eq!(matched, None);
        let matched = value(
            Some(TABLE_NAME),
            &[],
            &[Literal("projects"), Literal("/"), SingleWildcard],
            &[TrailingMultiWildcard],
        );
        assert_eq!(matched, Some("projects/proj_foo"));
        let matched = value(Some(APP_PROFILE_ID), &[], &[MultiWildcard], &[]);
        assert_eq!(matched, Some("profiles/prof_qux"));
        let matched = value(
            Some(APP_PROFILE_ID),
            &[Literal("profiles"), Literal("/")],
            &[SingleWildcard],
            &[],
        );
        assert_eq!(matched, Some("prof_qux"));
    }

    #[test_case("projects/p", "projects%2Fp")]
    #[test_case("kebab-case", "kebab-case")]
    #[test_case("dot.name", "dot.name")]
    #[test_case("under_score", "under_score")]
    #[test_case("tilde~123", "tilde~123")]
    fn encode(input: &str, want: &str) {
        let got = enc(input);
        assert_eq!(&got.to_string(), want);
    }
}
