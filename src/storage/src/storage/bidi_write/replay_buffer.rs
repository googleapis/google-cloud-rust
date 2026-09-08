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

/// Defines the maximum capacity of the replay buffer in bytes (32 MiB).
pub const MAX_REPLAY_BUFFER_SIZE: usize = 32 * 1024 * 1024;

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
    /// Creates a new replay chunk.
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

    /// Converts this replay chunk into a [`BidiWriteObjectRequest`] for transmission.
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

/// Manages an in-memory FIFO queue of unacknowledged chunks up to [`MAX_REPLAY_BUFFER_SIZE`].
#[derive(Debug, Default)]
pub struct ReplayBuffer {
    queue: VecDeque<ReplayChunk>,
    current_size: usize,
}

impl ReplayBuffer {
    /// Creates a new, empty replay buffer.
    pub fn new() -> Self {
        Self {
            queue: VecDeque::new(),
            current_size: 0,
        }
    }

    /// Enqueues an unacknowledged [`ReplayChunk`] to the replay buffer.
    pub fn push(&mut self, chunk: ReplayChunk) {
        self.current_size += chunk.data.len();
        self.queue.push_back(chunk);
    }

    /// Trims acknowledged chunks up to `persisted_size`.
    ///
    /// If `persisted_size` lands inside a chunk, that chunk is sliced in-place
    /// and its CRC32C is recomputed for the unpersisted sub-slice only.
    pub fn acknowledge(&mut self, persisted_size: i64) {
        while let Some(front) = self.queue.front()
            && front.end_offset() <= persisted_size
        {
            if let Some(chunk) = self.queue.pop_front() {
                self.current_size -= chunk.data.len();
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
            self.current_size -= trimmed_bytes;
        }
    }

    /// Returns `true` if the buffered byte count has reached or exceeded
    /// [`MAX_REPLAY_BUFFER_SIZE`].
    pub fn is_full(&self) -> bool {
        self.current_size >= MAX_REPLAY_BUFFER_SIZE
    }

    /// Returns `true` if the replay buffer contains no chunks.
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// Returns the number of chunks currently held in the buffer.
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    /// Returns the total unpersisted byte count currently retained in the buffer.
    pub fn current_size(&self) -> usize {
        self.current_size
    }

    /// Returns an iterator over the unpersisted [`ReplayChunk`]s in FIFO order for replay.
    pub fn chunks_to_replay(&self) -> impl Iterator<Item = &ReplayChunk> {
        self.queue.iter()
    }

    /// Clears all chunks from the buffer and resets byte tracking.
    pub fn clear(&mut self) {
        self.queue.clear();
        self.current_size = 0;
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
        assert_eq!(buf.len(), 0);
        assert_eq!(buf.current_size(), 0);
        assert!(!buf.is_full());

        // Act.
        buf.acknowledge(100);

        // Assert.
        assert!(buf.is_empty());
    }

    #[test]
    fn push_and_acknowledge_full_chunks() {
        // Arrange.
        let mut buf = ReplayBuffer::new();
        let chunk1 = Bytes::from_static(b"hello ");
        let chunk2 = Bytes::from_static(b"world!");

        // Act.
        buf.push(ReplayChunk::new(0, chunk1.clone(), crc32c::crc32c(&chunk1)));
        buf.push(ReplayChunk::new(6, chunk2.clone(), crc32c::crc32c(&chunk2)));

        // Assert.
        assert_eq!(buf.len(), 2);
        assert_eq!(buf.current_size(), 12);

        // Act.
        // Acknowledge partially up to 4 bytes (within chunk1)
        buf.acknowledge(4);

        // Assert.
        assert_eq!(buf.len(), 2);
        assert_eq!(buf.current_size(), 8);

        let chunks: Vec<_> = buf.chunks_to_replay().cloned().collect();
        assert_eq!(chunks[0].write_offset, 4);
        assert_eq!(chunks[0].data.as_ref(), b"o ");
        assert_eq!(chunks[0].crc32c, crc32c::crc32c(b"o "));
        assert_eq!(chunks[1].write_offset, 6);
        assert_eq!(chunks[1].data.as_ref(), b"world!");
        assert_eq!(chunks[1].crc32c, crc32c::crc32c(b"world!"));

        // Act.
        // Acknowledge fully past chunk1 up to 10 (within chunk2)
        buf.acknowledge(10);

        // Assert.
        assert_eq!(buf.len(), 1);
        assert_eq!(buf.current_size(), 2);

        let chunks: Vec<_> = buf.chunks_to_replay().cloned().collect();
        assert_eq!(chunks[0].write_offset, 10);
        assert_eq!(chunks[0].data.as_ref(), b"d!");
        assert_eq!(chunks[0].crc32c, crc32c::crc32c(b"d!"));

        // Act.
        // Acknowledge all remaining bytes.
        buf.acknowledge(12);

        // Assert.
        assert!(buf.is_empty());
        assert_eq!(buf.current_size(), 0);
    }

    #[test]
    fn acknowledge_duplicate_or_earlier_offset() {
        // Arrange.
        let mut buf = ReplayBuffer::new();
        let chunk = Bytes::from_static(b"abcdef");
        buf.push(ReplayChunk::new(10, chunk.clone(), crc32c::crc32c(&chunk)));

        // Act.
        // Acknowledge offset earlier than front write_offset.
        buf.acknowledge(5);

        // Assert.
        assert_eq!(buf.len(), 1);
        assert_eq!(buf.current_size(), 6);

        // Act.
        // Acknowledge exact write_offset of the front chunk.
        buf.acknowledge(10);

        // Assert.
        assert_eq!(buf.len(), 1);
        assert_eq!(buf.current_size(), 6);
    }

    #[test]
    fn is_full_threshold() {
        // Arrange.
        let mut buf = ReplayBuffer::new();
        let huge_chunk = Bytes::from(vec![0u8; MAX_REPLAY_BUFFER_SIZE]);

        // Act.
        buf.push(ReplayChunk::new(0, huge_chunk, 0));

        // Assert.
        assert!(buf.is_full());

        // Act.
        buf.acknowledge(1);

        // Assert.
        assert!(!buf.is_full());
    }

    #[test]
    fn chunk_to_request_conversion() {
        // Arrange.
        let data = Bytes::from_static(b"replay data");
        let crc = crc32c::crc32c(&data);
        let chunk = ReplayChunk::new(42, data.clone(), crc);

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
        assert!(buf.current_size() > 0);

        // Act.
        buf.clear();

        // Assert.
        assert!(buf.is_empty());
        assert_eq!(buf.current_size(), 0);
    }
}
