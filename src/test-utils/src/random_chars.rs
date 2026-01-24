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

//! Helper functions to generate random resource names.

use rand::{Rng, distr::Distribution};

/// Generate random characters from a provided list.
///
/// The integration tests often have to generate random resource names. The
/// valid characters for a resource name vary by service, some accept
/// alphanumeric strings (including uppercase), some reject uppercase letters,
/// some accept `_`, etc.
///
/// # Example
/// ```
/// use google_cloud_test_utils::random_chars::RandomChars;
/// let chars = RandomChars::new("abcde");
/// let got: String = chars.sample(32);
/// ```
pub struct RandomChars {
    chars: Vec<char>,
}

impl RandomChars {
    /// Initializes a new generator of random characters.
    pub fn new(chars: &str) -> Self {
        Self {
            chars: chars.chars().collect(),
        }
    }

    /// Create a string with `n` characters from the character set.
    pub fn sample(&self, n: usize) -> String {
        rand::rng().sample_iter(self).take(n).collect()
    }
}

impl Distribution<char> for RandomChars {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> char {
        let index = rng.random_range(0..self.chars.len());
        self.chars[index]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distr() {
        let chars = RandomChars::new("abcde");
        let got: String = rand::rng().sample_iter(chars).take(64).collect();
        assert!(
            !got.contains(|c| !("abcde".contains(c))),
            "{got:?} contains unexpected character"
        );
    }

    #[test]
    fn string() {
        let got = RandomChars::new("abcde").sample(64);
        assert!(
            !got.contains(|c| !("abcde".contains(c))),
            "{got:?} contains unexpected character"
        );
    }
}
