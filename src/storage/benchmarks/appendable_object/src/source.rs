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

use bytes::Bytes;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

pub struct StatelessSource {
    rng: StdRng,
}

impl StatelessSource {
    pub fn new() -> Self {
        Self {
            rng: StdRng::seed_from_u64(42), // Deterministic seed
        }
    }

    pub fn next_chunk(&mut self, size: usize) -> Bytes {
        let mut buffer = vec![0u8; size];
        self.rng.fill_bytes(&mut buffer);
        Bytes::from(buffer)
    }
}
