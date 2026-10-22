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

//! Detect duplicate fields while deserializing a message.
//!
//! The generated deserializers reject duplicate fields, treating a field's JSON
//! name and original proto name as aliases. Assigning each field of a message a
//! consecutive index allows this type to detect repeats without allocating.

/// A fixed-size bitset recording which fields of a message have been seen.
///
/// `N` is the number of 64-bit words needed to hold one bit per field, that is,
/// `field_count.div_ceil(64)`.
#[derive(Clone, Debug)]
pub struct SeenFields<const N: usize>([u64; N]);

/// The number of fields recorded by each word.
const BITS: usize = u64::BITS as usize;

impl<const N: usize> SeenFields<N> {
    /// Creates a bitset with no fields recorded.
    #[must_use]
    pub const fn new() -> Self {
        Self([0; N])
    }

    /// Records the field at `index`.
    ///
    /// Returns `false` if the field was already recorded, mirroring
    /// [`std::collections::HashSet::insert`].
    ///
    /// # Panics
    ///
    /// If `index` is `N * 64` or larger. The generated code only uses indices
    /// within range.
    pub const fn insert(&mut self, index: usize) -> bool {
        let bit = 1_u64 << (index % BITS);
        let word = &mut self.0[index / BITS];
        let seen = *word & bit != 0;
        *word |= bit;
        !seen
    }
}

impl<const N: usize> Default for SeenFields<N> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_case::test_case;

    #[test_case(0)]
    #[test_case(1)]
    #[test_case(62)]
    #[test_case(63)]
    fn insert_detects_repeats(index: usize) {
        let mut fields = SeenFields::<1>::new();
        assert!(fields.insert(index));
        assert!(!fields.insert(index));
    }

    #[test_case(64)]
    #[test_case(127)]
    #[test_case(128)]
    #[test_case(191)]
    fn insert_detects_repeats_past_the_first_word(index: usize) {
        let mut fields = SeenFields::<3>::new();
        assert!(fields.insert(index));
        assert!(!fields.insert(index));
    }

    #[test_case(63, 64; "across the first word boundary")]
    #[test_case(127, 128; "across the second word boundary")]
    fn adjacent_fields_are_independent(lower: usize, upper: usize) {
        let mut fields = SeenFields::<3>::new();
        assert!(fields.insert(lower));
        assert!(fields.insert(upper));
        assert!(!fields.insert(lower));
        assert!(!fields.insert(upper));
    }

    #[test]
    fn all_fields_are_independent() {
        let mut fields = SeenFields::<3>::new();
        for index in 0..192 {
            assert!(fields.insert(index), "first insert of {index}");
        }
        for index in 0..192 {
            assert!(!fields.insert(index), "second insert of {index}");
        }
    }

    #[test]
    #[should_panic(expected = "index out of bounds")]
    fn insert_into_zero_words_panics() {
        SeenFields::<0>::new().insert(0);
    }

    #[test]
    #[should_panic(expected = "index out of bounds")]
    fn insert_past_one_word_panics() {
        SeenFields::<1>::new().insert(64);
    }

    #[test]
    #[should_panic(expected = "index out of bounds")]
    fn insert_past_three_words_panics() {
        SeenFields::<3>::new().insert(192);
    }

    #[test]
    fn insert_in_const_context() {
        const FIELDS: SeenFields<2> = {
            let mut fields = SeenFields::new();
            fields.insert(63);
            fields.insert(64);
            fields
        };
        let mut fields = FIELDS;
        assert!(!fields.insert(63));
        assert!(!fields.insert(64));
        assert!(fields.insert(65));
    }

    #[test]
    fn default_matches_new() {
        let mut fields = SeenFields::<1>::default();
        assert!(fields.insert(0));
        assert!(!fields.insert(0));
    }
}
