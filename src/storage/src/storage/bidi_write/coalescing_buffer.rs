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

//! Batches client append operations into standard [`COALESCING_CHUNK_SIZE`] (2 MiB) chunks.

use super::MAX_WRITE_CHUNK_SIZE;
use bytes::{Bytes, BytesMut};

/// Defines the maximum coalesced chunk size (2 MiB) for appendable upload writes.
///
/// Matches the GCS protocol limit [`MAX_WRITE_CHUNK_SIZE`].
pub const COALESCING_CHUNK_SIZE: usize = MAX_WRITE_CHUNK_SIZE;

/// Batches small write operations into standard [`COALESCING_CHUNK_SIZE`] (2 MiB) chunks.
///
/// Provides zero-copy slicing for incoming chunks larger than or equal to [`COALESCING_CHUNK_SIZE`]
/// and lazily buffers residual partial writes until filled or explicitly flushed via
/// [`flush`](Self::flush).
///
/// Once chunks are coalesced and returned to the caller, downstream write failures are handled at
/// the transport and application level: transient stream failures are replayed by the background
/// worker, and terminal errors are recovered by reopening the object at the server's acknowledged
/// `persisted_size`.
#[derive(Debug)]
pub struct CoalescingBuffer {
    buffer: BytesMut,
}

impl Default for CoalescingBuffer {
    fn default() -> Self {
        Self::new()
    }
}

impl CoalescingBuffer {
    /// Creates a new, empty coalescing buffer with lazy allocation.
    pub fn new() -> Self {
        Self {
            buffer: BytesMut::new(),
        }
    }

    /// Ingests incoming data and returns all complete 2 MiB chunks formed.
    pub fn push(&mut self, mut chunk: Bytes) -> Vec<Bytes> {
        let mut ready_chunks = Vec::new();

        // 1. If buffer has partial data, fill it up to 2 MiB first.
        if !self.buffer.is_empty() {
            let needed = COALESCING_CHUNK_SIZE - self.buffer.len();
            if chunk.len() < needed {
                self.buffer.extend_from_slice(&chunk);
                return ready_chunks;
            }
            // SAFETY: `needed` is guaranteed to be within the bounds of `chunk`.
            self.buffer.extend_from_slice(&chunk[..needed]);
            ready_chunks.push(self.buffer.split().freeze());
            chunk = chunk.slice(needed..);
        }

        // 2. Fast path: slice complete 2 MiB chunks directly from `chunk` (zero-copy).
        while chunk.len() >= COALESCING_CHUNK_SIZE {
            ready_chunks.push(chunk.slice(..COALESCING_CHUNK_SIZE));
            chunk = chunk.slice(COALESCING_CHUNK_SIZE..);
        }

        // 3. Store remaining trailing bytes (< 2 MiB) in accumulator with lazy capacity allocation.
        if !chunk.is_empty() {
            if self.buffer.capacity() < COALESCING_CHUNK_SIZE {
                self.buffer
                    .reserve(COALESCING_CHUNK_SIZE - self.buffer.len());
            }
            self.buffer.extend_from_slice(&chunk);
        }

        ready_chunks
    }

    /// Drains any residual unsealed buffered bytes (< 2 MiB).
    pub fn flush(&mut self) -> Option<Bytes> {
        if self.buffer.is_empty() {
            return None;
        }
        let residual = self.buffer.split().freeze();
        Some(residual)
    }

    /// Returns `true` if the coalescing buffer contains no bytes.
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Returns the number of unsealed bytes currently stored in the buffer.
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    /// Returns the allocated byte capacity of the internal accumulator.
    pub fn capacity(&self) -> usize {
        self.buffer.capacity()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_buffer_is_empty() {
        // Arrange & Act.
        let mut buf = CoalescingBuffer::new();

        // Assert.
        assert!(buf.is_empty());
        assert_eq!(buf.len(), 0);
        assert_eq!(buf.flush(), None);
    }

    #[test]
    fn push_empty_chunk() {
        // Arrange.
        let mut buf = CoalescingBuffer::new();

        // Act.
        let ready = buf.push(Bytes::new());

        // Assert.
        assert!(ready.is_empty());
        assert!(buf.is_empty());
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn small_appends_coalesce_into_2mib() {
        // Arrange.
        let mut buf = CoalescingBuffer::new();
        let payload_1mib = Bytes::from(vec![1u8; 1024 * 1024]);

        // Act & Assert 1: First 1 MiB -> no ready chunks, buffered in accumulator.
        let ready1 = buf.push(payload_1mib.clone());
        assert!(ready1.is_empty());
        assert_eq!(buf.len(), 1024 * 1024);
        assert!(!buf.is_empty());

        // Act & Assert 2: Second 1 MiB -> completes 2 MiB coalesced chunk.
        let ready2 = buf.push(payload_1mib.clone());
        assert_eq!(ready2.len(), 1);
        assert_eq!(ready2[0].len(), COALESCING_CHUNK_SIZE);
        assert_eq!(&ready2[0][..1024 * 1024], payload_1mib.as_ref());
        assert_eq!(&ready2[0][1024 * 1024..], payload_1mib.as_ref());
        assert!(buf.is_empty());
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn large_append_fast_path_zero_copy() {
        // Arrange.
        let mut buf = CoalescingBuffer::new();
        let payload_5mib = Bytes::from(vec![42u8; 5 * 1024 * 1024]);

        // Act.
        let ready = buf.push(payload_5mib.clone());

        // Assert: 2 complete chunks returned immediately via zero-copy slicing.
        assert_eq!(ready.len(), 2);
        assert_eq!(ready[0].len(), COALESCING_CHUNK_SIZE);
        assert_eq!(ready[1].len(), COALESCING_CHUNK_SIZE);

        // Verify zero-copy: chunk memory pointers match the exact slices of the source payload.
        assert_eq!(
            ready[0].as_ptr(),
            payload_5mib[..COALESCING_CHUNK_SIZE].as_ptr()
        );
        assert_eq!(
            ready[1].as_ptr(),
            payload_5mib[COALESCING_CHUNK_SIZE..2 * COALESCING_CHUNK_SIZE].as_ptr()
        );

        // Remaining 1 MiB is held in accumulator.
        assert_eq!(buf.len(), 1024 * 1024);
        assert!(!buf.is_empty());

        let residual = buf.flush();
        assert_eq!(residual.map(|b| b.len()), Some(1024 * 1024));
        assert!(buf.is_empty());
    }

    #[test]
    fn lazy_allocation_and_flush_capacity() {
        // Arrange.
        let mut buf = CoalescingBuffer::new();
        assert_eq!(buf.capacity(), 0);

        // Act & Assert 1: Chunks >= 2 MiB follow fast path without allocating in buffer.
        let exact = Bytes::from(vec![1u8; COALESCING_CHUNK_SIZE]);
        let ready = buf.push(exact);
        assert_eq!(ready.len(), 1);
        assert_eq!(buf.capacity(), 0);

        // Act & Assert 2: Residual data (< 2 MiB) triggers lazy allocation.
        let partial = Bytes::from_static(b"hello world");
        let ready = buf.push(partial);
        assert!(ready.is_empty());
        assert!(buf.capacity() >= COALESCING_CHUNK_SIZE);

        // Act & Assert 3: Flushing drains data without eagerly re-allocating.
        let flushed = buf.flush();
        assert!(flushed.is_some());
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn default_buffer() {
        // Arrange & Act.
        let buf = CoalescingBuffer::default();

        // Assert.
        assert!(buf.is_empty());
        assert_eq!(buf.len(), 0);
        assert_eq!(buf.capacity(), 0);
    }

    #[test]
    fn mixed_small_and_large_appends() {
        // Arrange.
        let mut buf = CoalescingBuffer::new();
        let small = Bytes::from(vec![0xAAu8; 512 * 1024]); // 512 KiB
        let large = Bytes::from(vec![0xBBu8; 3 * 1024 * 1024]); // 3 MiB

        // Act 1: Push 512 KiB.
        let ready1 = buf.push(small);
        // Assert 1.
        assert!(ready1.is_empty());
        assert_eq!(buf.len(), 512 * 1024);

        // Act 2: Push 3 MiB (needs 1.5 MiB to seal first chunk, then 1.5 MiB remains).
        let ready2 = buf.push(large);
        // Assert 2.
        assert_eq!(ready2.len(), 1);
        assert_eq!(ready2[0].len(), COALESCING_CHUNK_SIZE);
        assert_eq!(buf.len(), (1024 + 512) * 1024);

        // Act 3: Flush residual.
        let residual = buf.flush().unwrap();
        // Assert 3.
        assert_eq!(residual.len(), (1024 + 512) * 1024);
        assert!(buf.is_empty());
    }

    #[test]
    fn exact_chunk_size_append() {
        // Arrange.
        let mut buf = CoalescingBuffer::new();
        let exact = Bytes::from(vec![0x77u8; COALESCING_CHUNK_SIZE]);

        // Act.
        let ready = buf.push(exact.clone());

        // Assert.
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0], exact);
        assert!(buf.is_empty());
        assert_eq!(buf.flush(), None);
    }

    #[test]
    fn multiple_flushes() {
        // Arrange.
        let mut buf = CoalescingBuffer::new();
        let payload = Bytes::from_static(b"hello world");

        // Act.
        buf.push(payload.clone());

        // Assert.
        assert_eq!(buf.flush(), Some(payload));
        assert_eq!(buf.flush(), None);
        assert_eq!(buf.flush(), None);
    }

    #[test]
    fn flush_empty_buffer() {
        // Arrange.
        let mut buf = CoalescingBuffer::new();

        // Act & Assert.
        assert_eq!(buf.flush(), None);
    }
}
