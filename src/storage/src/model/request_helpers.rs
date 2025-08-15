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

//! Types used in the request builders ([ReadObject] and/or [UploadObject])
//! to improve type safety or ergonomics.
//!
//! [ReadObject]: crate::builder::storage::ReadObject
//! [UploadObject]: crate::builder::storage::UploadObject

use crate::error::KeyAes256Error;
use sha2::{Digest, Sha256};

#[derive(Debug)]
/// KeyAes256 represents an AES-256 encryption key used with the
/// Customer-Supplied Encryption Keys (CSEK) feature.
///
/// This key must be exactly 32 bytes in length and should be provided in its
/// raw (unencoded) byte format.
///
/// # Examples
///
/// Creating a `KeyAes256` instance from a valid byte slice:
/// ```
/// # use google_cloud_storage::{model::request_helpers::KeyAes256, error::KeyAes256Error};
/// let raw_key_bytes: [u8; 32] = [0x42; 32]; // Example 32-byte key
/// let key_aes_256 = KeyAes256::new(&raw_key_bytes)?;
/// # Ok::<(), KeyAes256Error>(())
/// ```
///
/// Handling an error for an invalid key length:
/// ```
/// # use google_cloud_storage::{model::request_helpers::KeyAes256, error::KeyAes256Error};
/// let invalid_key_bytes: &[u8] = b"too_short_key"; // Less than 32 bytes
/// let result = KeyAes256::new(invalid_key_bytes);
///
/// assert!(matches!(result, Err(KeyAes256Error::InvalidLength)));
/// ```
pub struct KeyAes256 {
    key: [u8; 32],
}

impl KeyAes256 {
    /// Attempts to create a new [KeyAes256].
    ///
    /// This conversion will succeed only if the input slice is exactly 32 bytes long.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::{model::request_helpers::KeyAes256, error::KeyAes256Error};
    /// let raw_key_bytes: [u8; 32] = [0x42; 32]; // Example 32-byte key
    /// let key_aes_256 = KeyAes256::new(&raw_key_bytes)?;
    /// # Ok::<(), KeyAes256Error>(())
    /// ```
    pub fn new(key: &[u8]) -> std::result::Result<Self, KeyAes256Error> {
        match key.len() {
            32 => Ok(Self {
                key: key[..32].try_into().unwrap(),
            }),
            _ => Err(KeyAes256Error::InvalidLength),
        }
    }
}

impl std::convert::From<KeyAes256> for crate::model::CommonObjectRequestParams {
    fn from(value: KeyAes256) -> Self {
        crate::model::CommonObjectRequestParams::new()
            .set_encryption_algorithm("AES256")
            .set_encryption_key_bytes(value.key.to_vec())
            .set_encryption_key_sha256_bytes(Sha256::digest(value.key).as_slice().to_owned())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use base64::{Engine, prelude::BASE64_STANDARD};
    use test_case::test_case;

    type Result = anyhow::Result<()>;

    /// This is used by the request builder tests.
    pub(crate) fn create_key_helper() -> (Vec<u8>, String, Vec<u8>, String) {
        // Make a 32-byte key.
        let key = vec![b'a'; 32];
        let key_base64 = BASE64_STANDARD.encode(key.clone());

        let key_sha256 = Sha256::digest(key.clone());
        let key_sha256_base64 = BASE64_STANDARD.encode(key_sha256);
        (key, key_base64, key_sha256.to_vec(), key_sha256_base64)
    }

    #[test]
    // This tests converting to KeyAes256 from some different types
    // that can get converted to &[u8].
    fn test_key_aes_256() -> Result {
        let v_slice: &[u8] = &[b'c'; 32];
        KeyAes256::new(v_slice)?;

        let v_vec: Vec<u8> = vec![b'a'; 32];
        KeyAes256::new(&v_vec)?;

        let v_array: [u8; 32] = [b'a'; 32];
        KeyAes256::new(&v_array)?;

        let v_bytes: bytes::Bytes = bytes::Bytes::copy_from_slice(&v_array);
        KeyAes256::new(&v_bytes)?;

        Ok(())
    }

    #[test_case(&[b'a'; 0]; "no bytes")]
    #[test_case(&[b'a'; 1]; "not enough bytes")]
    #[test_case(&[b'a'; 33]; "too many bytes")]
    fn test_key_aes_256_err(input: &[u8]) {
        KeyAes256::new(input).unwrap_err();
    }

    #[test]
    fn test_key_aes_256_to_control_model_object() -> Result {
        let (key, _, key_sha256, _) = create_key_helper();
        let key_aes_256 = KeyAes256::new(&key)?;
        let params = crate::model::CommonObjectRequestParams::from(key_aes_256);
        assert_eq!(params.encryption_algorithm, "AES256");
        assert_eq!(params.encryption_key_bytes, key);
        assert_eq!(params.encryption_key_sha256_bytes, key_sha256);
        Ok(())
    }
}
