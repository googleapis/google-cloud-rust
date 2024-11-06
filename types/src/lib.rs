// Copyright 2024 Google LLC
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

mod any;
pub use crate::any::*;
mod duration;
pub use crate::duration::*;
mod field_mask;
pub use crate::field_mask::*;
mod timestamp;
pub use crate::timestamp::*;

#[cfg(test)]
mod test {
    use serde_json::json;
    use std::error::Error;

    #[serde_with::serde_as]
    #[serde_with::skip_serializing_none]
    #[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]
    #[serde(rename_all = "camelCase")]
    #[non_exhaustive]
    pub struct Int64 {
        #[serde_as(as = "serde_with::DisplayFromStr")]
        pub int64: i64,
    }

    #[test]
    fn test_serialize_large_i64() -> Result<(), Box<dyn Error>> {
        // 1 << 60 is too large to be represented as a JSON number, those are
        // always IEEE 754 double precision floating point numbers, which only
        // has about 52 bits of mantissa.
        let value = 1152921504606846976i64;
        let msg = Int64 {
            int64: value,
            ..Default::default()
        };
        let got = serde_json::to_value(msg)?;
        let want = json!({"int64": "1152921504606846976"});
        assert_eq!(want, got);
        Ok(())
    }

    #[test]
    fn test_deserialize_large_i64() -> Result<(), Box<dyn Error>> {
        // 1 << 60 is too large to be represented as a JSON number, those are
        // always IEEE 754 double precision floating point numbers, which only
        // has about 52 bits of mantissa.
        let got = serde_json::from_value::<Int64>(json!({"int64": "1152921504606846976"}))?;
        let want = Int64 {
            int64: 1152921504606846976i64,
            ..Default::default()
        };
        assert_eq!(want, got);
        Ok(())
    }

    #[serde_with::serde_as]
    #[serde_with::skip_serializing_none]
    #[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]
    #[serde(rename_all = "camelCase")]
    #[non_exhaustive]
    pub struct MessageWithBytes {
        #[serde_as(as = "serde_with::base64::Base64")]
        pub payload: bytes::Bytes,
    }

    #[test]
    fn test_serialize_bytes() -> Result<(), Box<dyn Error>> {
        // 1 << 60 is too large to be represented as a JSON number, those are
        // always IEEE 754 double precision floating point numbers, which only
        // has about 52 bits of mantissa.
        let b = bytes::Bytes::from("the quick brown fox jumps over the laze dog");
        let msg = MessageWithBytes {
            payload: b,
            ..Default::default()
        };
        let got = serde_json::to_value(msg)?;
        let want =
            json!({"payload": "dGhlIHF1aWNrIGJyb3duIGZveCBqdW1wcyBvdmVyIHRoZSBsYXplIGRvZw=="});
        assert_eq!(want, got);
        Ok(())
    }

    #[test]
    fn test_deserialize_bytes() -> Result<(), Box<dyn Error>> {
        // 1 << 60 is too large to be represented as a JSON number, those are
        // always IEEE 754 double precision floating point numbers, which only
        // has about 52 bits of mantissa.
        let got = serde_json::from_str::<Int64>(r###"{"int64":"1152921504606846976"}"###)?;
        let want = Int64 {
            int64: 1152921504606846976i64,
            ..Default::default()
        };
        assert_eq!(want, got);
        Ok(())
    }
}
