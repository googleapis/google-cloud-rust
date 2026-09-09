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

use crate::{Error, Result};
use gaxi::path_parameter::{PathMismatchBuilder, try_match};
use gaxi::routing_parameter::Segment;
use google_cloud_gax::error::binding::BindingError;

pub(crate) fn validate_table(table: &str) -> Result<()> {
    let segments = &[
        Segment::Literal("projects/"),
        Segment::SingleWildcard,
        Segment::Literal("/datasets/"),
        Segment::SingleWildcard,
        Segment::Literal("/tables/"),
        Segment::SingleWildcard,
    ];
    try_match(Some(table), segments)
        .ok_or_else(|| {
            let builder = PathMismatchBuilder::default().maybe_add(
                Some(table),
                segments,
                "table",
                "projects/*/datasets/*/tables/*",
            );
            Error::binding(BindingError {
                paths: vec![builder.build()],
            })
        })
        .map(|_| ())
}

pub(crate) fn validate_stream(stream: &str) -> Result<()> {
    let segments = &[
        Segment::Literal("projects/"),
        Segment::SingleWildcard,
        Segment::Literal("/datasets/"),
        Segment::SingleWildcard,
        Segment::Literal("/tables/"),
        Segment::SingleWildcard,
        Segment::Literal("/streams/"),
        Segment::SingleWildcard,
    ];
    try_match(Some(stream), segments)
        .ok_or_else(|| {
            let builder = PathMismatchBuilder::default().maybe_add(
                Some(stream),
                segments,
                "write_stream",
                "projects/*/datasets/*/tables/*/streams/*",
            );
            Error::binding(BindingError {
                paths: vec![builder.build()],
            })
        })
        .map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_case::test_case;

    #[test]
    fn valid_table() {
        assert!(validate_table("projects/p/datasets/d/tables/t").is_ok());
    }

    #[test_case("projects/p")]
    #[test_case("projects/p/tables/t")]
    #[test_case("projects/p/datasets/d/tables/")]
    #[test_case("projects/p/instances/i/tables/t")]
    #[test_case("projects/p/datasets/d/tables/t/streams")]
    #[test_case("projects/p/datasets/d/tables/t/streams/_default")]
    fn bad_table_format(table: &str) {
        let err = validate_table(table).expect_err("should fail locally on bad format");
        assert!(err.is_binding(), "{err:?}");
    }

    #[test]
    fn valid_stream() {
        assert!(validate_stream("projects/p/datasets/d/tables/t/streams/s").is_ok());
    }

    #[test_case("projects/p")]
    #[test_case("projects/p/tables/t")]
    #[test_case("projects/p/datasets/d/tables/t")]
    #[test_case("projects/p/datasets/d/tables/t/streams/")]
    fn bad_stream_format(stream: &str) {
        let err = validate_stream(stream).expect_err("should fail locally on bad format");
        assert!(err.is_binding(), "{err:?}");
    }
}
