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

// [START storage_generate_encryption_key]
use base64::{Engine as _, engine::general_purpose};
use rand::RngCore;

pub fn sample() -> String {
    // Generates a 256 bit (32 byte) AES encryption key and prints the base64 representation.
    //
    // This is included for demonstration purposes. You should generate your own key.
    // Please remember that encryption keys should be handled with a comprehensive security policy.
    let mut key = [0u8; 32];
    rand::rng().fill_bytes(&mut key);
    let key = general_purpose::STANDARD.encode(key);
    println!("Sample encryption key: {}", key);
    key
}
// [END storage_generate_encryption_key]
