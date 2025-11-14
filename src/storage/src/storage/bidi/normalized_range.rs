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

use crate::error::ReadError;
use crate::google::storage::v2::ReadRange as ProtoRange;

type ReadResult<T> = std::result::Result<T, ReadError>;

/// A normalized range represents a range of bytes normalized with a positive
/// offset.
///
/// The client library needs to keep track of pending reads ranges, and resend
/// them if the stream needs to be resumed. The range needs to be updated as
/// the library receives data, to avoid requesting the same portion of a range.
///
/// This can be tedious because applications may request ranges with negative
/// offsets (representing the last bytes of the object), and with or without a
/// length limit.
///
/// After the first response arrives these requested ranges can be normalized to
/// have a positive offset. This struct represent such normalized changes.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct NormalizedRange {
    offset: i64,
    length: Option<i64>,
}

impl NormalizedRange {
    /// Creates a new unbounded range starting at a given offset.
    pub fn new(offset: i64) -> ReadResult<Self> {
        if offset < 0 {
            return Err(ReadError::BadOffsetInBidiResponse(offset));
        }
        Ok(Self {
            offset,
            length: None,
        })
    }

    /// Sets the length.
    pub fn with_length(mut self, length: i64) -> ReadResult<Self> {
        if length < 0 {
            return Err(ReadError::BadLengthInBidiResponse(length));
        }
        self.length = Some(length);
        Ok(self)
    }

    /// Creates a new normalized range from a read response.
    pub fn from_proto(response: ProtoRange) -> ReadResult<Self> {
        match (response.read_offset, response.read_length) {
            (o, 0) => Self::new(o),
            (o, l) => Self::new(o)?.with_length(l),
        }
    }

    pub fn offset(&self) -> i64 {
        self.offset
    }

    pub fn length(&self) -> Option<i64> {
        self.length
    }

    pub fn as_proto(&self, id: i64) -> ProtoRange {
        ProtoRange {
            read_id: id,
            read_offset: self.offset,
            read_length: self.length.unwrap_or_default(),
        }
    }

    pub fn matching_offset(&self, requested_offset: u64) -> bool {
        self.offset as u64 == requested_offset
    }

    pub fn update(&mut self, response: ProtoRange) -> ReadResult<()> {
        let update = NormalizedRange::from_proto(response)?;
        if update.offset != self.offset {
            return Err(ReadError::OutOfOrderBidiResponse {
                got: update.offset,
                expected: self.offset,
            });
        }
        match (self.length, update.length) {
            (None, _) => (),
            (Some(_), None) => (),
            (Some(expected), Some(got)) if got <= expected => (),
            (Some(expected), Some(got)) => {
                return Err(ReadError::LongRead {
                    got: got as u64,
                    expected: expected as u64,
                });
            }
        };
        self.offset = update.offset + update.length().unwrap_or_default();
        self.length = match (&self.length, &update.length) {
            (None, _) => None,
            (Some(l), None) => Some(*l),
            (Some(expected), Some(got)) if expected < got => {
                return Err(ReadError::LongRead {
                    got: *got as u64,
                    expected: *expected as u64,
                });
            }
            (Some(expected), Some(got)) => Some(*expected - *got),
        };
        Ok(())
    }

    pub fn handle_empty(&self, end: bool) -> ReadResult<()> {
        match (end, self.length) {
            (true, Some(l)) if l > 0 => Err(ReadError::ShortRead(l as u64)),
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::proto_range;
    use super::*;

    #[test]
    fn without_length() -> anyhow::Result<()> {
        let got = NormalizedRange::new(100)?;
        let want = NormalizedRange {
            offset: 100,
            length: None,
        };
        assert_eq!(got, want);
        assert_eq!(got.offset(), 100, "{got:?}");
        assert!(got.length().is_none(), "{got:?}");

        let proto = got.as_proto(123456);
        let want = ProtoRange {
            read_id: 123456,
            read_offset: 100,
            read_length: 0,
        };
        assert_eq!(proto, want);

        assert!(got.matching_offset(100_u64), "{got:?}");
        assert!(!got.matching_offset(105_u64), "{got:?}");

        Ok(())
    }

    #[test]
    fn bad_offset() {
        let got = NormalizedRange::new(-100);
        assert!(
            matches!(got, Err(ReadError::BadOffsetInBidiResponse(_))),
            "{got:?}"
        );
    }

    #[test]
    fn with_length() -> anyhow::Result<()> {
        let input = NormalizedRange::new(100)?;

        let got = input.with_length(50)?;
        let want = NormalizedRange {
            offset: 100,
            length: Some(50),
        };
        assert_eq!(got, want);
        assert_eq!(got.offset(), 100, "{got:?}");
        assert_eq!(got.length(), Some(50), "{got:?}");

        let proto = got.as_proto(123456);
        let want = ProtoRange {
            read_id: 123456,
            read_offset: 100,
            read_length: 50,
        };
        assert_eq!(proto, want);

        assert!(got.matching_offset(100_u64), "{got:?}");
        assert!(!got.matching_offset(105_u64), "{got:?}");

        Ok(())
    }

    #[test]
    fn bad_length() -> anyhow::Result<()> {
        let got = NormalizedRange::new(100)?.with_length(-50);
        assert!(
            matches!(got, Err(ReadError::BadLengthInBidiResponse(_))),
            "{got:?}"
        );
        Ok(())
    }

    #[test]
    fn update_errors() -> anyhow::Result<()> {
        let mut normalized = NormalizedRange::new(100)?.with_length(50)?;

        let response = ProtoRange {
            read_offset: -50,
            ..ProtoRange::default()
        };
        let got = normalized.update(response);
        assert!(got.is_err(), "{got:?}");

        let response = proto_range(50, 0);
        let got = normalized.update(response);
        assert!(
            matches!(got, Err(ReadError::OutOfOrderBidiResponse { .. })),
            "{got:?}"
        );

        let response = proto_range(200, 0);
        let got = normalized.update(response);
        assert!(
            matches!(got, Err(ReadError::OutOfOrderBidiResponse { .. })),
            "{got:?}"
        );
        Ok(())
    }

    #[test]
    fn update_with_length() -> anyhow::Result<()> {
        let mut normalized = NormalizedRange::new(100)?.with_length(200)?;
        let response = proto_range(100, 25);
        normalized.update(response)?;
        assert_eq!((normalized.offset(), normalized.length()), (125, Some(175)));

        let response = proto_range(125, 50);
        normalized.update(response)?;
        assert_eq!((normalized.offset(), normalized.length()), (175, Some(125)));

        let response = proto_range(175, 0);
        normalized.update(response)?;
        assert_eq!((normalized.offset(), normalized.length()), (175, Some(125)));
        Ok(())
    }

    #[test]
    fn update_without_length() -> anyhow::Result<()> {
        let mut normalized = NormalizedRange::new(100)?;
        let response = proto_range(100, 25);
        normalized.update(response)?;
        assert_eq!((normalized.offset(), normalized.length()), (125, None));

        let response = proto_range(125, 50);
        normalized.update(response)?;
        assert_eq!((normalized.offset(), normalized.length()), (175, None));

        let response = proto_range(175, 0);
        normalized.update(response)?;
        assert_eq!((normalized.offset(), normalized.length()), (175, None));
        Ok(())
    }

    #[test]
    fn update_with_bad_length() -> anyhow::Result<()> {
        let mut normalized = NormalizedRange::new(100)?.with_length(200)?;
        let response = proto_range(100, 300);
        let result = normalized.update(response);
        assert!(
            matches!(result, Err(ReadError::LongRead { expected, got }) if expected == 200_u64 && got == 300_u64),
            "{result:?}"
        );
        Ok(())
    }

    #[test]
    fn handle_empty() -> anyhow::Result<()> {
        let normalized = NormalizedRange::new(100)?.with_length(50)?;
        let result = normalized.handle_empty(false);
        assert!(result.is_ok(), "{result:?}");
        let result = normalized.handle_empty(true);
        assert!(matches!(result, Err(ReadError::ShortRead(_))), "{result:?}");

        let normalized = NormalizedRange::new(100)?;
        let result = normalized.handle_empty(false);
        assert!(result.is_ok(), "{result:?}");
        let result = normalized.handle_empty(true);
        assert!(result.is_ok(), "{result:?}");

        Ok(())
    }
}
