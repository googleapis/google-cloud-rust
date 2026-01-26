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

use rand::{
    Rng,
    distr::{Distribution, Uniform},
};

const LOWERCASE_ALPHANUMERIC_CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";

/// Sample a `u8`, uniformly distributed over ASCII lowercase letters and numbers: a-z and 0-9.
///
/// # Example
/// ```
/// use google_cloud_test_utils::resource_names::LowercaseAlphanumeric;
/// let got: String = LowercaseAlphanumeric.random_string(32);
/// assert_eq!(got.len(), 32);
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct LowercaseAlphanumeric;

impl LowercaseAlphanumeric {
    /// Create a string with `n` characters from the character set.
    pub fn random_string(&self, n: usize) -> String {
        rand::rng()
            .sample_iter(self)
            .take(n)
            .map(char::from)
            .collect()
    }
}

impl Distribution<u8> for LowercaseAlphanumeric {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> u8 {
        let u = Uniform::new(0, LOWERCASE_ALPHANUMERIC_CHARSET.len())
            .expect("hard-coded uniform distribution is initialized successfully")
            .sample(rng);
        LOWERCASE_ALPHANUMERIC_CHARSET[u]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lowercase() {
        let got: String = rand::rng()
            .sample_iter(&LowercaseAlphanumeric)
            .take(128)
            .map(char::from)
            .collect();
        for (idx, c) in got.chars().enumerate() {
            assert!(
                c.is_ascii(),
                "character at {idx} ({c}) is not ASCII in {got}"
            );
            assert!(
                c.is_ascii_lowercase() || c.is_ascii_digit(),
                "character at {idx} ({c}) is not in expected character class in {got}"
            );
        }
    }

    #[test]
    fn lowercase_string() {
        let got = LowercaseAlphanumeric.random_string(32);
        assert_eq!(got.len(), 32, "{got:?}");
        for (idx, c) in got.chars().enumerate() {
            assert!(
                c.is_ascii(),
                "character at {idx} ({c}) is not ASCII in {got}"
            );
            assert!(
                c.is_ascii_lowercase() || c.is_ascii_digit(),
                "character at {idx} ({c}) is not in expected character class in {got}"
            );
        }
    }
}
