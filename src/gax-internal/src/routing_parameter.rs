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

/// Find a routing parameter in `haytack` using the (decomposed) template.
/// 
/// # Example
/// ```
/// # use google_cloud_gax_internal::routing_parameter::*;
/// let matching = find_matching(
///     "projects/p/instances/i/tables/t",
///     &["projects/", "*", "/"],
///     &["instances/", "*"],
///     &["/tables/", "**"]);
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
pub fn find_matching<'h>(
    haystack: &'h str,
    prefix: &[&'static str],
    matching: &[&'static str],
    suffix: &[&'static str],
) -> Option<&'h str> {
    let mut remains = haystack;
    let mut start = 0_usize;
    let mut end = 0_usize;

    for needle in prefix {
        let count = match *needle {
            "*" => consume_single(remains),
            p => consume_literal(remains, p),
        }?;
        start += count;
        end += count;
        remains = &remains[count..];
    }
    for needle in matching {
        let count = match *needle {
            "*" => consume_single(remains),
            "**" => consume_multi(remains),
            p => consume_literal(remains, p),
        }?;
        end += count;
        remains = &remains[count..];
    }
    for needle in suffix {
        let count = match *needle {
            "*" => consume_single(remains),
            "**" => consume_multi(remains),
            p => consume_literal(remains, p),
        }?;
        remains = &remains[count..];
    }
    if !remains.is_empty() || start == end {
        return None;
    }
    Some(&haystack[start..end])
}

/// Format a routing parameter key value pair.
/// 
/// This is just a helper to simplify the code generation.
pub fn format((k, v): (&str, &str)) -> String {
    format!("{k}={v}")
}

fn consume_single(remains: &str) -> Option<usize> {
    let i = remains.find('/').unwrap_or(remains.len());
    (i != 0).then_some(i)
}

fn consume_multi(remains: &str) -> Option<usize> {
    let i = remains.len();
    (i != 0).then_some(i)
}

fn consume_literal(remains: &str, literal: &str) -> Option<usize> {
    remains.starts_with(literal).then_some(literal.len())
}

#[cfg(test)]
mod test {
    use super::*;
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
    fn request_body(req: Request) -> Option<String> {
        let x_goog_request_params = [
            // match for "table_location"
            find_matching(
                &req.table_name,
                &[],
                &["regions/", "*", "/zones/", "*"],
                &["/tables/", "*"],
            )
            .or_else(|| {
                find_matching(
                    &req.table_name,
                    &["projects/", "*", "/"],
                    &["instances/", "*"],
                    &["/tables/", "*"],
                )
            })
            .map(|v| ("table_location", v)),
            // match for "routing_id"
            find_matching(&req.app_profile_id, &["profiles/"], &["*"], &[])
                .or_else(|| find_matching(&req.app_profile_id, &[], &["**"], &[]))
                .or_else(|| find_matching(&req.table_name, &[], &["projects/", "*"], &["/", "**"]))
                .map(|v| ("routing_id", v)),
        ];
        let mut i = x_goog_request_params.into_iter().flatten();
        let s = i.next().map(super::format)?;
        Some(i.fold(s, |s, p| s + "&" + &super::format(p)))
    }

    #[test_case("", "", None; "empty")]
    #[test_case("", "profiles/q", Some("routing_id=q"); "match #3 wins")]
    #[test_case("", "thingy/q/child/c", Some("routing_id=thingy/q/child/c"); "match #2 wins")]
    #[test_case("projects/p/instances/i", "", Some("routing_id=projects/p"); "match #1 wins")]
    #[test_case("projects/p/instances/i/tables/t", "", Some("table_location=instances/i&routing_id=projects/p"); "one field matches 2 vables wins")]
    #[test_case("projects/p/instances/i/tables/t", "profiles/q", Some("table_location=instances/i&routing_id=q"); "multiple variables")]
    fn simulated_request(table_name: &str, app_profile_id: &str, want: Option<&str>) {
        let got = request_body(Request {
            table_name: table_name.into(),
            app_profile_id: app_profile_id.into(),
        });
        assert_eq!(got.as_deref(), want);
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
        let got = find_matching(input, &["projects/", "*", "/"], &["instances/", "*"], &["/tables/", "*"]);
        assert_eq!(got, want);
    }

    #[test_case("", None; "empty")]
    #[test_case("projects/p/instances/i/tables/t", Some("instances/i/tables/t"); "success")]
    #[test_case("projects/p/instances/i/tables/t/extra", Some("instances/i/tables/t/extra"); "with extra")]
    #[test_case("projects/p/instances/i/tables", None; "missing separateor")]
    #[test_case("projects/p/instances/i/tables/", None; "empty segment")]
    fn matching_multi_segment(input: &str, want: Option<&str>) {
        let got = find_matching(input, &["projects/", "*", "/"], &["instances/", "*", "/tables/", "**"], &[]);
        assert_eq!(got, want);
    }

    #[test_case("", None; "empty")]
    #[test_case("projects/p/instances/i/tables/t", Some("projects/p/instances/i/tables/t"); "success")]
    #[test_case("projects/p/instances/i/tables/t/extra", Some("projects/p/instances/i/tables/t/extra"); "with extra")]
    #[test_case("projects/p/instances/i/tables", None; "missing separateor")]
    #[test_case("projects/p/instances/i/tables/", None; "empty segment")]
    fn matching_wildcard_then_multi_segment(input: &str, want: Option<&str>) {
        let got = find_matching(input, &[], &["projects/", "*", "/instances/", "*", "/tables/", "**"], &[]);
        assert_eq!(got, want);
    }

    #[test]
    fn example1() {
        let matched = find_matching(APP_PROFILE_ID, &[], &["**"], &[]);
        assert_eq!(matched, Some("profiles/prof_qux"));
    }

    #[test]
    fn example2() {
        let matched = find_matching(APP_PROFILE_ID, &[], &["**"], &[]);
        assert_eq!(matched, Some("profiles/prof_qux"));
    }

    #[test]
    fn example3a() {
        let matched = find_matching(
            TABLE_NAME,
            &[],
            &["projects", "/", "*", "/", "instances", "/", "*", "/", "**"],
            &[],
        );
        assert_eq!(
            matched,
            Some("projects/proj_foo/instances/instance_bar/table/table_baz")
        );
    }

    #[test]
    fn example3b() {
        let matched = find_matching(
            TABLE_NAME,
            &[],
            &["regions", "/", "*", "zones", "/", "*", "/", "**"],
            &[],
        );
        assert_eq!(matched, None);
    }

    #[test]
    fn example3c() {
        let matched = find_matching(
            TABLE_NAME,
            &[],
            &["regions", "/", "*", "zones", "/", "*", "/", "**"],
            &[],
        );
        assert_eq!(matched, None);
        let matched = find_matching(
            TABLE_NAME,
            &[],
            &["projects", "/", "*", "/", "instances", "/", "*", "/", "**"],
            &[],
        );
        assert_eq!(
            matched,
            Some("projects/proj_foo/instances/instance_bar/table/table_baz")
        );
    }

    #[test]
    fn example4() {
        let matched = find_matching(TABLE_NAME, &[], &["projects", "/", "*"], &["/", "**"]);
        assert_eq!(matched, Some("projects/proj_foo"));
    }

    #[test]
    fn example5() {
        let matched = find_matching(TABLE_NAME, &[], &["projects", "/", "*"], &["/", "**"]);
        assert_eq!(matched, Some("projects/proj_foo"));
        let matched = find_matching(
            TABLE_NAME,
            &[],
            &["projects", "/", "*", "/", "instances", "/", "*"],
            &["/", "**"],
        );
        assert_eq!(matched, Some("projects/proj_foo/instances/instance_bar"));
    }

    #[test]
    fn example6a() {
        let matched = find_matching(
            TABLE_NAME,
            &[],
            &["projects", "/", "*"],
            &["/", "instances", "/", "*", "/", "**"],
        );
        assert_eq!(matched, Some("projects/proj_foo"));
        let matched = find_matching(
            TABLE_NAME,
            &["projects", "/", "*", "/"],
            &["instances", "/", "*"],
            &["/", "**"],
        );
        assert_eq!(matched, Some("instances/instance_bar"));
    }

    #[test]
    fn example6b() {
        let matched = find_matching(TABLE_NAME, &[], &["projects", "/", "*"], &["/", "**"]);
        assert_eq!(matched, Some("projects/proj_foo"));
        let matched = find_matching(
            TABLE_NAME,
            &["projects", "/", "*", "/"],
            &["instances", "/", "*"],
            &["/", "**"],
        );
        assert_eq!(matched, Some("instances/instance_bar"));
    }

    #[test]
    fn example7() {
        let matched = find_matching(TABLE_NAME, &[], &["projects", "/", "*"], &["/", "**"]);
        assert_eq!(matched, Some("projects/proj_foo"));
        let matched = find_matching(APP_PROFILE_ID, &[], &["**"], &[]);
        assert_eq!(matched, Some("profiles/prof_qux"));
    }

    #[test]
    fn example8() {
        let matched = find_matching(TABLE_NAME, &[], &["projects", "/", "*"], &["/", "**"]);
        assert_eq!(matched, Some("projects/proj_foo"));
        let matched = find_matching(TABLE_NAME, &[], &["regions", "/", "*"], &["/", "**"]);
        assert_eq!(matched, None);
        let matched = find_matching(APP_PROFILE_ID, &[], &["**"], &[]);
        assert_eq!(matched, Some("profiles/prof_qux"));
    }

    #[test]
    fn example9() {
        let matched = find_matching(
            TABLE_NAME,
            &["projects", "/", "*", "/"],
            &["instances", "/", "*"],
            &["/", "table", "/", "*"],
        );
        assert_eq!(matched, Some("instances/instance_bar"));
        let matched = find_matching(
            TABLE_NAME,
            &[],
            &["regions", "/", "*", "/", "zones", "/", "*"],
            &["tables", "/", "*"],
        );
        assert_eq!(matched, None);
        let matched = find_matching(TABLE_NAME, &[], &["projects", "/", "*"], &["/", "**"]);
        assert_eq!(matched, Some("projects/proj_foo"));
        let matched = find_matching(APP_PROFILE_ID, &[], &["**"], &[]);
        assert_eq!(matched, Some("profiles/prof_qux"));
        let matched = find_matching(APP_PROFILE_ID, &["profiles", "/"], &["*"], &[]);
        assert_eq!(matched, Some("prof_qux"));
    }
}
