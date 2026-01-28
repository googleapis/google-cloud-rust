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

//! Helper functions and types to generate random resource names.

use rand::{
    Rng,
    distr::{Alphanumeric, Distribution, Uniform},
};

/// A common prefix for resource ids.
///
/// Where possible, we use this prefix for randomly generated resource ids.
pub const PREFIX: &str = "rust-sdk-testing-";

/// The maximum length for a secret ID.
const SECRET_ID_LENGTH: usize = 64;

const BUCKET_ID_LENGTH: usize = 63;

const WORKFLOW_ID_LENGTH: usize = 64;

/// Generate a random bucket id.
pub fn random_bucket_id() -> String {
    let id = LowercaseAlphanumeric.random_string(BUCKET_ID_LENGTH - PREFIX.len());
    format!("{PREFIX}{id}")
}

/// Generate a random workflow id.
pub fn random_workflow_id() -> String {
    let id: String = rand::rng()
        .sample_iter(&Alphanumeric)
        .take(WORKFLOW_ID_LENGTH - PREFIX.len())
        .map(char::from)
        .collect();
    format!("{PREFIX}{id}")
}

/// Generate a random secret id.
pub fn random_secret_id() -> String {
    let id: String = rand::rng()
        .sample_iter(&Alphanumeric)
        .take(SECRET_ID_LENGTH - PREFIX.len())
        .map(char::from)
        .collect();
    format!("{PREFIX}{id}")
}

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
    use anyhow::bail;

    #[test]
    fn bucket_id() {
        assert!(
            PREFIX.len() < BUCKET_ID_LENGTH,
            "{PREFIX} length ({}) should be smaller than {BUCKET_ID_LENGTH}",
            PREFIX.len()
        );
        let got = random_bucket_id();
        assert!(
            got.len() <= BUCKET_ID_LENGTH,
            "{got} has more than {BUCKET_ID_LENGTH} characters"
        );
        let suffix = got.strip_prefix(PREFIX);
        assert!(suffix.is_some(), "{got} should start with {PREFIX}");
        let test = is_ascii_lowercase_alphanumeric(suffix.unwrap());
        assert!(test.is_ok(), "{test:?}");
    }

    #[test]
    fn workflow_id() {
        assert!(
            PREFIX.len() < WORKFLOW_ID_LENGTH,
            "{PREFIX} length ({}) should be smaller than {WORKFLOW_ID_LENGTH}",
            PREFIX.len()
        );
        let got = random_workflow_id();
        assert!(
            got.len() <= WORKFLOW_ID_LENGTH,
            "{got} has more than {WORKFLOW_ID_LENGTH} characters"
        );
        let suffix = got
            .strip_prefix(PREFIX)
            .expect("{got} should start with {PREFIX}");
        assert!(
            suffix.chars().all(|c| c.is_alphanumeric()),
            "the suffix should be alphanumeric: {suffix}"
        );
    }

    #[test]
    fn secret_id() {
        assert!(
            PREFIX.len() < SECRET_ID_LENGTH,
            "{PREFIX} length ({}) should be smaller than {SECRET_ID_LENGTH}",
            PREFIX.len()
        );
        let got = random_workflow_id();
        assert!(
            got.len() <= SECRET_ID_LENGTH,
            "{got} has more than {SECRET_ID_LENGTH} characters"
        );
        let suffix = got
            .strip_prefix(PREFIX)
            .expect("{got} should start with {PREFIX}");
        assert!(
            suffix.chars().all(|c| c.is_alphanumeric()),
            "the suffix should be alphanumeric: {suffix}"
        );
    }

    #[test]
    fn lowercase() {
        let got: String = rand::rng()
            .sample_iter(&LowercaseAlphanumeric)
            .take(128)
            .map(char::from)
            .collect();
        let test = is_ascii_lowercase_alphanumeric(&got);
        assert!(test.is_ok(), "{test:?}");
    }

    #[test]
    fn lowercase_string() {
        let got = LowercaseAlphanumeric.random_string(32);
        assert_eq!(got.len(), 32, "{got:?}");
        let test = is_ascii_lowercase_alphanumeric(&got);
        assert!(test.is_ok(), "{test:?}");
    }

    fn is_ascii_lowercase_alphanumeric(got: &str) -> anyhow::Result<()> {
        for (idx, c) in got.chars().enumerate() {
            if !c.is_ascii() {
                bail!("character at {idx} ({c}) is not ASCII in {got}")
            }
            if !c.is_ascii_lowercase() && !c.is_ascii_digit() {
                bail!("character at {idx} ({c}) is not in expected character class in {got}");
            }
        }
        Ok(())
    }
}
