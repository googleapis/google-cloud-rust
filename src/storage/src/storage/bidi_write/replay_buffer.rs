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

//! Retains unacknowledged chunks, trims acknowledged data, and provides chunks
//! for resending upon stream reconnect.

use crate::google::storage::v2::{
    BidiWriteObjectRequest, ChecksummedData, bidi_write_object_request::Data,
};
use bytes::Bytes;
use std::collections::VecDeque;

/// Defines the default capacity of the [`ReplayBuffer`] in bytes (32 MiB).
// TODO(#5716): Remove once ReplayBuffer capacity is configured via CommonOptions.
pub const DEFAULT_REPLAY_BUFFER_SIZE: usize = 32 * 1024 * 1024;

/// Represents an unacknowledged data chunk retained in the [`ReplayBuffer`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplayChunk {
    /// Holds the logical starting write offset of this chunk.
    pub write_offset: i64,
    /// Contains the raw payload bytes.
    pub data: Bytes,
    /// Stores the precomputed CRC32C checksum of [`data`][Self::data].
    pub crc32c: u32,
}

impl ReplayChunk {
    /// Creates a new [`ReplayChunk`].
    pub fn new(write_offset: i64, data: Bytes, crc32c: u32) -> Self {
        Self {
            write_offset,
            data,
            crc32c,
        }
    }

    /// Returns the ending byte offset (exclusive) of this chunk.
    pub fn end_offset(&self) -> i64 {
        self.write_offset + self.data.len() as i64
    }

    /// Converts this [`ReplayChunk`] into a [`BidiWriteObjectRequest`] for transmission.
    pub fn to_request(&self) -> BidiWriteObjectRequest {
        BidiWriteObjectRequest {
            write_offset: self.write_offset,
            data: Some(Data::ChecksummedData(ChecksummedData {
                content: self.data.clone(),
                crc32c: Some(self.crc32c),
            })),
            ..BidiWriteObjectRequest::default()
        }
    }
}

/// Manages an in-memory FIFO queue of unacknowledged chunks up to a configurable capacity.
#[derive(Debug)]
pub struct ReplayBuffer {
    queue: VecDeque<ReplayChunk>,
    unpersisted_bytes: usize,
    capacity: usize,
}

impl Default for ReplayBuffer {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplayBuffer {
    /// Creates a new, empty [`ReplayBuffer`] with the default capacity ([`DEFAULT_REPLAY_BUFFER_SIZE`]).
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_REPLAY_BUFFER_SIZE)
    }

    /// Creates a new, empty [`ReplayBuffer`] with a specified capacity in bytes.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            queue: VecDeque::new(),
            unpersisted_bytes: 0,
            capacity,
        }
    }

    /// Returns the configured capacity of the [`ReplayBuffer`] in bytes.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Enqueues an unacknowledged [`ReplayChunk`] to the [`ReplayBuffer`].
    pub fn push(&mut self, chunk: ReplayChunk) {
        self.unpersisted_bytes += chunk.data.len();
        self.queue.push_back(chunk);
    }

    /// Trims acknowledged chunks up to `persisted_size`.
    ///
    /// If `persisted_size` lands inside a chunk, that chunk is sliced in-place
    /// and its CRC32C is recomputed for the unpersisted sub-slice only.
    pub fn ack(&mut self, persisted_size: i64) {
        while let Some(front) = self.queue.front() {
            if front.end_offset() <= persisted_size {
                let chunk = self.queue.pop_front().expect("front chunk must exist");
                self.unpersisted_bytes -= chunk.data.len();
            } else {
                break;
            }
        }

        if let Some(front) = self.queue.front_mut()
            && front.write_offset < persisted_size
        {
            let trimmed_bytes = (persisted_size - front.write_offset) as usize;
            // SAFETY: persisted_size is guaranteed to be within the bounds of the chunk because front.write_offset < persisted_size and front.end_offset() > persisted_size.
            front.data = front.data.slice(trimmed_bytes..);
            front.write_offset = persisted_size;
            front.crc32c = crc32c::crc32c(&front.data);
            self.unpersisted_bytes -= trimmed_bytes;
        }
    }

    /// Returns `true` if the buffered byte count has reached or exceeded [`capacity`][Self::capacity].
    pub fn is_full(&self) -> bool {
        self.unpersisted_bytes >= self.capacity
    }

    /// Returns `true` if the [`ReplayBuffer`] contains no chunks.
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// Returns the number of chunks currently held in the [`ReplayBuffer`].
    pub fn num_chunks(&self) -> usize {
        self.queue.len()
    }

    /// Returns the total unpersisted byte count currently retained in the [`ReplayBuffer`].
    pub fn unpersisted_bytes(&self) -> usize {
        self.unpersisted_bytes
    }

    /// Returns an iterator over the unpersisted [`ReplayChunk`]s in FIFO order for replay.
    pub fn chunks_to_replay(&self) -> impl Iterator<Item = &ReplayChunk> {
        self.queue.iter()
    }

    /// Clears all chunks from the [`ReplayBuffer`] and resets byte tracking.
    pub fn clear(&mut self) {
        self.queue.clear();
        self.unpersisted_bytes = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_buffer_state() {
        // Arrange.
        let mut buf = ReplayBuffer::new();

        // Assert.
        assert!(buf.is_empty());
        assert_eq!(buf.num_chunks(), 0);
        assert_eq!(buf.unpersisted_bytes(), 0);
        assert!(!buf.is_full());

        // Act.
        buf.ack(100);

        // Assert.
        assert!(buf.is_empty());
    }

    #[test]
    fn push_and_ack_full_chunks() {
        // Arrange.
        let mut buf = ReplayBuffer::new();
        let chunk1 = Bytes::from_static(b"hello ");
        let chunk2 = Bytes::from_static(b"world!");

        // Act.
        buf.push(ReplayChunk::new(0, chunk1.clone(), crc32c::crc32c(&chunk1)));
        buf.push(ReplayChunk::new(6, chunk2.clone(), crc32c::crc32c(&chunk2)));

        // Assert.
        assert_eq!(buf.num_chunks(), 2);
        assert_eq!(buf.unpersisted_bytes(), 12);

        // Act.
        // Acknowledge partially up to 4 bytes (within chunk1)
        buf.ack(4);

        // Assert.
        assert_eq!(buf.num_chunks(), 2);
        assert_eq!(buf.unpersisted_bytes(), 8);

        let chunks: Vec<_> = buf.chunks_to_replay().cloned().collect();
        assert_eq!(chunks[0].write_offset, 4);
        assert_eq!(chunks[0].data.as_ref(), b"o ");
        assert_eq!(chunks[0].crc32c, crc32c::crc32c(b"o "));
        assert_eq!(chunks[1].write_offset, 6);
        assert_eq!(chunks[1].data.as_ref(), b"world!");
        assert_eq!(chunks[1].crc32c, crc32c::crc32c(b"world!"));

        // Act.
        // Acknowledge fully past chunk1 up to 10 (within chunk2)
        buf.ack(10);

        // Assert.
        assert_eq!(buf.num_chunks(), 1);
        assert_eq!(buf.unpersisted_bytes(), 2);

        let chunks: Vec<_> = buf.chunks_to_replay().cloned().collect();
        assert_eq!(chunks[0].write_offset, 10);
        assert_eq!(chunks[0].data.as_ref(), b"d!");
        assert_eq!(chunks[0].crc32c, crc32c::crc32c(b"d!"));

        // Act.
        // Acknowledge all remaining bytes.
        buf.ack(12);

        // Assert.
        assert!(buf.is_empty());
        assert_eq!(buf.unpersisted_bytes(), 0);
    }

    #[test]
    fn ack_duplicate_or_earlier_offset() {
        // Arrange.
        let mut buf = ReplayBuffer::new();
        let chunk = Bytes::from_static(b"abcdef");
        buf.push(ReplayChunk::new(10, chunk.clone(), crc32c::crc32c(&chunk)));

        // Act.
        // Acknowledge offset earlier than front write_offset.
        buf.ack(5);

        // Assert.
        assert_eq!(buf.num_chunks(), 1);
        assert_eq!(buf.unpersisted_bytes(), 6);

        // Act.
        // Acknowledge exact write_offset of the front chunk.
        buf.ack(10);

        // Assert.
        assert_eq!(buf.num_chunks(), 1);
        assert_eq!(buf.unpersisted_bytes(), 6);
    }

    #[test]
    fn is_full_threshold() {
        // Arrange.
        let mut buf = ReplayBuffer::new();
        let huge_chunk = Bytes::from(vec![0u8; DEFAULT_REPLAY_BUFFER_SIZE]);

        // Act.
        buf.push(ReplayChunk::new(0, huge_chunk, 0));

        // Assert.
        assert!(buf.is_full());

        // Act.
        buf.ack(1);

        // Assert.
        assert!(!buf.is_full());
    }

    #[test]
    fn chunk_to_request_conversion() {
        // Arrange.
        let data = Bytes::from_static(b"replay data");
        let crc = crc32c::crc32c(&data);
        let chunk = ReplayChunk::new(42, data.clone(), crc);

        // Assert end_offset calculation.
        assert_eq!(chunk.end_offset(), 42 + data.len() as i64);

        // Act.
        let req = chunk.to_request();

        // Assert.
        assert_eq!(req.write_offset, 42);
        if let Some(Data::ChecksummedData(cd)) = req.data {
            assert_eq!(cd.content, data);
            assert_eq!(cd.crc32c, Some(crc));
        } else {
            panic!("expected ChecksummedData");
        }
    }

    #[test]
    fn clear_resets_size_and_queue() {
        // Arrange.
        let mut buf = ReplayBuffer::new();
        let chunk = Bytes::from_static(b"test data");
        buf.push(ReplayChunk::new(0, chunk, 0));
        assert!(!buf.is_empty());
        assert!(buf.unpersisted_bytes() > 0);

        // Act.
        buf.clear();

        // Assert.
        assert!(buf.is_empty());
        assert_eq!(buf.unpersisted_bytes(), 0);
    }

    #[test]
    fn with_capacity_default() {
        // Arrange & Act.
        let buf = ReplayBuffer::new();

        // Assert.
        assert_eq!(buf.capacity(), DEFAULT_REPLAY_BUFFER_SIZE);
    }

    #[test]
    fn with_capacity_custom() {
        // Arrange.
        const CUSTOM_CAPACITY: usize = 64 * 1024 * 1024; // 64 MiB

        // Act.
        let buf = ReplayBuffer::with_capacity(CUSTOM_CAPACITY);

        // Assert.
        assert_eq!(buf.capacity(), CUSTOM_CAPACITY);
    }

    #[test]
    fn is_full_with_custom_capacity() {
        // Arrange.
        // Use a micro-capacity of 100 bytes for deterministic testing.
        let mut buf = ReplayBuffer::with_capacity(100);
        let chunk = Bytes::from(vec![0u8; 100]);

        // Act.
        buf.push(ReplayChunk::new(0, chunk, 0));

        // Assert.
        assert!(buf.is_full());

        // Act.
        buf.ack(1);

        // Assert.
        assert!(!buf.is_full());
    }
}
