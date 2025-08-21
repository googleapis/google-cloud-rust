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

use super::{ChecksumMismatch, ObjectChecksums};

pub fn update(known: &mut ObjectChecksums, computed: ObjectChecksums) {
    known.crc32c = known.crc32c.or(computed.crc32c);
    if known.md5_hash.is_empty() {
        known.md5_hash = computed.md5_hash;
    }
}

/// Compare the received object checksums vs. the computed checksums.
///
/// If the `crc32c` field is `None`, or the `md5_mash` field is empty, they do
/// not participate in the comparison. That accounts for disabled checksums in
/// the client (where only CRC32C is enabled by default) and for missing MD5
/// hashes on the server (common with composed objects).
pub fn validate(
    expected: &ObjectChecksums,
    received: &Option<ObjectChecksums>,
) -> Result<(), ChecksumMismatch> {
    let Some(recv) = received else {
        return Ok(());
    };
    let crc32c = match (expected.crc32c, recv.crc32c) {
        (Some(e), Some(r)) if e != r => Some((r, e)),
        _ => None,
    };
    let md5 = match (&expected.md5_hash, &recv.md5_hash) {
        (e, r) if e.is_empty() || r.is_empty() || e == r => None,
        (e, r) => Some((r.clone(), e.clone())),
    };
    match (crc32c, md5) {
        (None, None) => Ok(()),
        (Some((got, want)), None) => Err(ChecksumMismatch::Crc32c { got, want }),
        (None, Some((got, want))) => Err(ChecksumMismatch::Md5 { got, want }),
        (Some(crc32c), Some(md5)) => {
            let got = ObjectChecksums::new()
                .set_crc32c(crc32c.0)
                .set_md5_hash(md5.0);
            let want = ObjectChecksums::new()
                .set_crc32c(crc32c.1)
                .set_md5_hash(md5.1);
            Err(ChecksumMismatch::Both {
                got: Box::new(got),
                want: Box::new(want),
            })
        }
    }
}

/// Computes a checksum or hash for [Cloud Storage] transfers.
#[derive(Clone, Debug)]
pub struct Checksum {
    pub crc32c: Option<Crc32c>,
    pub md5_hash: Option<Md5>,
}

impl Checksum {
    pub fn update(&mut self, offset: u64, data: &bytes::Bytes) {
        if let Some(crc32c) = &mut self.crc32c {
            crc32c.update(offset, data);
        }
        if let Some(md5) = &mut self.md5_hash {
            md5.update(offset, data);
        }
    }

    pub fn finalize(&self) -> ObjectChecksums {
        let res = ObjectChecksums::new();
        let res = self
            .crc32c
            .iter()
            .fold(res, |r, b| r.set_crc32c(b.finalize()));
        self.md5_hash
            .iter()
            .fold(res, |r, b| r.set_md5_hash(b.finalize()))
    }
}

/// Automatically computes the CRC32C checksum.
#[derive(Clone, Debug, Default)]
pub struct Crc32c {
    checksum: u32,
    offset: u64,
}

impl Crc32c {
    fn update(&mut self, offset: u64, data: &bytes::Bytes) {
        self.offset = self::checked_update(self.offset, offset, data, |data| {
            self.checksum = crc32c::crc32c_append(self.checksum, data)
        })
    }

    fn finalize(&self) -> u32 {
        self.checksum
    }
}

/// Automatically computes the MD5 checksum.
#[derive(Clone, Default)]
pub struct Md5 {
    hasher: md5::Context,
    offset: u64,
}

impl Md5 {
    fn update(&mut self, offset: u64, data: &bytes::Bytes) {
        self.offset = self::checked_update(self.offset, offset, data, |data| {
            self.hasher.consume(data);
        });
    }

    fn finalize(&self) -> bytes::Bytes {
        let digest = self.hasher.clone().finalize();
        bytes::Bytes::from_owner(Vec::from_iter(digest.0))
    }
}

impl std::fmt::Debug for Md5 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Md5")
            .field("hasher", &"[skipped]")
            .field("offset", &self.offset)
            .finish()
    }
}

fn checked_update<F>(current: u64, offset: u64, data: &bytes::Bytes, updater: F) -> u64
where
    F: FnOnce(&bytes::Bytes),
{
    let end = offset + data.len() as u64;
    if (offset..end).contains(&current) {
        let data = data.clone().split_off((current - offset) as usize);
        updater(&data);
        end
    } else {
        current
    }
}

pub(crate) struct ChecksummedSource<S> {
    offset: u64,
    checksum: Checksum,
    source: S,
}

use crate::streaming_source::{Seek, SizeHint, StreamingSource};

impl<S> ChecksummedSource<S> {
    pub fn new(checksum: Checksum, source: S) -> Self {
        Self {
            offset: 0,
            checksum,
            source,
        }
    }

    pub fn final_checksum(&self) -> ObjectChecksums {
        self.checksum.finalize()
    }
}

impl<S> StreamingSource for ChecksummedSource<S>
where
    S: StreamingSource + Send + Sync,
{
    type Error = S::Error;
    async fn next(&mut self) -> Option<Result<bytes::Bytes, Self::Error>> {
        match self.source.next().await {
            None => None,
            Some(Ok(b)) => {
                self.checksum.update(self.offset, &b);
                self.offset += b.len() as u64;
                Some(Ok(b))
            }
            Some(Err(e)) => Some(Err(e)),
        }
    }
    async fn size_hint(&self) -> Result<SizeHint, Self::Error> {
        self.source.size_hint().await
    }
}

impl<S> Seek for ChecksummedSource<S>
where
    S: Seek + Send + Sync,
{
    type Error = S::Error;
    async fn seek(&mut self, offset: u64) -> Result<(), Self::Error> {
        match self.source.seek(offset).await {
            Ok(_) => {
                self.offset = offset;
                Ok(())
            }
            Err(e) => {
                // With an unknown state for the offset, ignore all future
                // data when computing the checksums.
                self.offset = u64::MAX;
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::streaming_source::tests::MockSeekSource;
    use test_case::test_case;

    pub(super) fn empty() -> bytes::Bytes {
        bytes::Bytes::new()
    }

    pub(super) fn data() -> bytes::Bytes {
        bytes::Bytes::from_static(b"the quick brown fox jumps over the lazy dog")
    }

    pub fn both() -> ObjectChecksums {
        ObjectChecksums::new()
            .set_crc32c(0x01020304_u32)
            .set_md5_hash(bytes::Bytes::from_static(b"abc"))
    }

    pub fn crc32c_only() -> ObjectChecksums {
        ObjectChecksums::new().set_crc32c(0x01020304_u32)
    }

    pub fn md5_only() -> ObjectChecksums {
        ObjectChecksums::new().set_md5_hash(bytes::Bytes::from_static(b"abc"))
    }

    #[test_case(both(), None)]
    #[test_case(both(), Some(both()))]
    #[test_case(both(), Some(crc32c_only()))]
    #[test_case(both(), Some(md5_only()))]
    #[test_case(crc32c_only(), None)]
    #[test_case(crc32c_only(), Some(both()))]
    #[test_case(crc32c_only(), Some(crc32c_only()))]
    #[test_case(crc32c_only(), Some(md5_only()))]
    #[test_case(md5_only(), None)]
    #[test_case(md5_only(), Some(both()))]
    #[test_case(md5_only(), Some(crc32c_only()))]
    #[test_case(md5_only(), Some(md5_only()))]
    fn validate_ok(expected: ObjectChecksums, received: Option<ObjectChecksums>) {
        let compare = super::validate(&expected, &received);
        assert!(compare.is_ok(), "{compare:?}");
    }

    #[test_case(crc32c_only(), crc32c_only().set_crc32c(0_u32))]
    #[test_case(both(), crc32c_only().set_crc32c(0_u32))]
    fn validate_bad_crc32c(expected: ObjectChecksums, received: ObjectChecksums) {
        let err = super::validate(&expected, &Some(received.clone()))
            .expect_err("values should not match");
        assert!(matches!(&err, &ChecksumMismatch::Crc32c { .. }), "{err:?}");
    }

    #[test_case(md5_only(), md5_only().set_md5_hash(bytes::Bytes::from_static(b"cde")))]
    #[test_case(both(), md5_only().set_md5_hash(bytes::Bytes::from_static(b"cde")))]
    fn validate_bad_md5(expected: ObjectChecksums, received: ObjectChecksums) {
        let err = super::validate(&expected, &Some(received.clone()))
            .expect_err("values should not match");
        assert!(matches!(&err, &ChecksumMismatch::Md5 { .. }), "{err:?}");
    }

    #[test_case(both(), both().set_crc32c(0_u32).set_md5_hash(bytes::Bytes::from_static(b"cde")))]
    fn validate_bad_both(expected: ObjectChecksums, received: ObjectChecksums) {
        let err = super::validate(&expected, &Some(received.clone()))
            .expect_err("values should not match");
        assert!(matches!(&err, &ChecksumMismatch::Both { .. }), "{err:?}");
    }

    #[test]
    fn none() {
        let mut engine = Checksum {
            crc32c: None,
            md5_hash: None,
        };
        engine.update(0, &data());
        assert_eq!(engine.finalize(), ObjectChecksums::new());
    }

    #[test_case(empty())]
    #[test_case(data())]
    fn crc32c_basic(input: bytes::Bytes) {
        let mut engine = Checksum {
            crc32c: Some(Crc32c::default()),
            md5_hash: None,
        };
        engine.update(0, &input);
        let want = crc32c::crc32c(&input);
        assert_eq!(engine.finalize(), ObjectChecksums::new().set_crc32c(want));
    }

    #[test]
    fn crc32c_in_parts() {
        let input = data();

        let mut engine = Checksum {
            crc32c: Some(Crc32c::default()),
            md5_hash: None,
        };
        engine.update(0, &input.slice(0..4));
        engine.update(0, &input.slice(0..4));
        engine.update(4, &input.slice(4..8));
        engine.update(6, &input.slice(6..12));
        engine.update(8, &input.slice(8..));
        // Out of range data should be ignored.
        engine.update(100, &input.slice(0..));
        let want = crc32c::crc32c(&data());
        assert_eq!(engine.finalize(), ObjectChecksums::new().set_crc32c(want));
    }

    #[test_case(empty())]
    #[test_case(data())]
    fn md5_basic(input: bytes::Bytes) {
        let mut engine = Checksum {
            crc32c: None,
            md5_hash: Some(Md5::default()),
        };
        engine.update(0, &input);
        let digest = md5::compute(&input);
        let want = bytes::Bytes::from_owner(Vec::from_iter(digest.0));
        assert_eq!(engine.finalize(), ObjectChecksums::new().set_md5_hash(want));
    }

    #[test]
    fn md5_in_parts() {
        let input = data();
        let mut engine = Checksum {
            crc32c: None,
            md5_hash: Some(Md5::default()),
        };
        let digest = md5::compute(&input);
        let want = bytes::Bytes::from_owner(Vec::from_iter(digest.0));

        engine.update(0, &input.slice(0..4));
        engine.update(0, &input.slice(0..4));
        engine.update(4, &input.slice(4..8));
        engine.update(6, &input.slice(6..12));
        engine.update(8, &input.slice(8..));
        // Out of range data should be ignored.
        engine.update(100, &input.slice(0..));
        assert_eq!(engine.finalize(), ObjectChecksums::new().set_md5_hash(want));
    }

    #[test]
    fn md5_and_crc32_in_parts() {
        let input = data();
        let mut engine = Checksum {
            crc32c: Some(Crc32c::default()),
            md5_hash: Some(Md5::default()),
        };
        let digest = md5::compute(&input);
        let md5_want = bytes::Bytes::from_owner(Vec::from_iter(digest.0));
        let crc32_want = crc32c::crc32c(&input);

        engine.update(0, &input.slice(0..4));
        engine.update(0, &input.slice(0..4));
        engine.update(4, &input.slice(4..8));
        engine.update(6, &input.slice(6..12));
        engine.update(0, &input.slice(0..4));
        engine.update(8, &input.slice(8..));
        // Out of range data should be ignored.
        engine.update(100, &input.slice(0..));
        assert_eq!(
            engine.finalize(),
            ObjectChecksums::new()
                .set_md5_hash(md5_want)
                .set_crc32c(crc32_want)
        );
    }

    #[test]
    fn crc32_and_md5_in_parts() {
        let input = data();
        let mut engine = Checksum {
            crc32c: Some(Crc32c::default()),
            md5_hash: Some(Md5::default()),
        };
        let digest = md5::compute(&input);
        let md5_want = bytes::Bytes::from_owner(Vec::from_iter(digest.0));
        let crc32_want = crc32c::crc32c(&input);

        engine.update(0, &input.slice(0..4));
        engine.update(0, &input.slice(0..4));
        engine.update(4, &input.slice(4..8));
        engine.update(6, &input.slice(6..12));
        engine.update(0, &input.slice(0..4));
        engine.update(8, &input.slice(8..));
        // Out of range data should be ignored.
        engine.update(100, &input.slice(0..));
        assert_eq!(
            engine.finalize(),
            ObjectChecksums::new()
                .set_md5_hash(md5_want)
                .set_crc32c(crc32_want)
        );
    }

    #[test]
    fn md5_debug() {
        let engine = Md5::default();
        let fmt = format!("{engine:?}");
        assert!(fmt.contains("Md5"), "{fmt}");
        assert!(fmt.contains("hasher"), "{fmt}");
        assert!(fmt.contains("offset"), "{fmt}");
    }

    #[tokio::test]
    async fn checksummed_source() -> anyhow::Result<()> {
        let input = [
            "the ", "quick ", "brown ", "fox ", "jumps ", "over ", "the ", "lazy ", "dog",
        ];
        let source = crate::streaming_source::IterSource::new(
            input.map(|s| bytes::Bytes::from_static(s.as_bytes())),
        );
        let want_hint = source.size_hint().await?;
        let mut source = ChecksummedSource::new(
            Checksum {
                crc32c: Some(Crc32c::default()),
                md5_hash: None,
            },
            source,
        );
        let got_hint = source.size_hint().await?;
        assert_eq!(got_hint.lower(), want_hint.lower());
        assert_eq!(got_hint.upper(), want_hint.upper());

        for expected in input.iter().take(3) {
            let got = source.next().await.transpose()?;
            assert_eq!(got, Some(bytes::Bytes::from_static(expected.as_bytes())));
        }
        source.seek(0).await?;
        for expected in input.iter().take(5) {
            let got = source.next().await.transpose()?;
            assert_eq!(got, Some(bytes::Bytes::from_static(expected.as_bytes())));
        }
        source.seek(16).await?;
        for _ in input.iter() {
            let _ = source.next().await.transpose()?;
        }

        let want = crc32c::crc32c("the quick brown fox jumps over the lazy dog".as_bytes());
        let got = source.final_checksum();
        assert_eq!(got, ObjectChecksums::new().set_crc32c(want));
        Ok(())
    }

    #[tokio::test]
    async fn checksummed_source_errors() -> anyhow::Result<()> {
        use std::io::{Error, ErrorKind};

        let mut source = MockSeekSource::new();
        source
            .expect_next()
            .once()
            .returning(|| Some(Err(Error::new(ErrorKind::BrokenPipe, "test-only"))));
        source
            .expect_seek()
            .once()
            .returning(|_| Err(Error::new(ErrorKind::FileTooLarge, "test-only")));

        let mut ck = ChecksummedSource::new(
            Checksum {
                crc32c: None,
                md5_hash: None,
            },
            source,
        );
        let err = ck.next().await.transpose().unwrap_err();
        assert_eq!(err.kind(), ErrorKind::BrokenPipe, "{err:?}");

        let err = ck.seek(0).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::FileTooLarge, "{err:?}");
        assert_eq!(ck.offset, u64::MAX);

        Ok(())
    }
}
