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

//! Mock RNG for internal tests.

use rand::rand_core;

pub(crate) struct MockRng {
    value: u64,
}

impl MockRng {
    pub fn new(value: u64) -> Self {
        Self { value }
    }
}

impl rand::RngCore for MockRng {
    fn next_u32(&mut self) -> u32 {
        self.value as u32
    }
    fn next_u64(&mut self) -> u64 {
        self.value
    }
    fn fill_bytes(&mut self, dst: &mut [u8]) {
        rand_core::impls::fill_bytes_via_next(self, dst);
    }
}
