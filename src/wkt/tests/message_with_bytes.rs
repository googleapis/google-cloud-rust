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

#[cfg(test)]
mod test {
    use serde_json::json;
    type Result = std::result::Result<(), Box<dyn std::error::Error>>;

    #[serde_with::serde_as]
    #[serde_with::skip_serializing_none]
    #[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct MessageWithBytes {
        #[serde_as(as = "serde_with::base64::Base64")]
        pub singular: bytes::Bytes,
        #[serde_as(as = "Option<serde_with::base64::Base64>")]
        pub optional: Option<bytes::Bytes>,
        #[serde_as(as = "Vec<serde_with::base64::Base64>")]
        pub repeated: Vec<bytes::Bytes>,
    }

    #[test]
    fn test_serialize_singular() -> Result {
        let b = bytes::Bytes::from("the quick brown fox jumps over the laze dog");
        let msg = MessageWithBytes {
            singular: b,
            ..Default::default()
        };
        let got = serde_json::to_value(&msg)?;
        let want = json!({"singular": "dGhlIHF1aWNrIGJyb3duIGZveCBqdW1wcyBvdmVyIHRoZSBsYXplIGRvZw==", "repeated": []});
        assert_eq!(want, got);

        let roundtrip = serde_json::from_value::<MessageWithBytes>(got)?;
        assert_eq!(msg, roundtrip);
        Ok(())
    }

    #[test]
    fn test_serialize_optional() -> Result {
        let b = bytes::Bytes::from("the quick brown fox jumps over the laze dog");
        let msg = MessageWithBytes {
            optional: Some(b),
            ..Default::default()
        };
        let got = serde_json::to_value(&msg)?;
        let want = json!({"singular": "", "optional": "dGhlIHF1aWNrIGJyb3duIGZveCBqdW1wcyBvdmVyIHRoZSBsYXplIGRvZw==", "repeated": []});
        assert_eq!(want, got);

        let roundtrip = serde_json::from_value::<MessageWithBytes>(got)?;
        assert_eq!(msg, roundtrip);
        Ok(())
    }

    #[test]
    fn test_serialize_repeated() -> Result {
        let b = bytes::Bytes::from("the quick brown fox jumps over the laze dog");
        let msg = MessageWithBytes {
            repeated: vec![b],
            ..Default::default()
        };
        let got = serde_json::to_value(&msg)?;
        let want = json!({"singular": "", "repeated": ["dGhlIHF1aWNrIGJyb3duIGZveCBqdW1wcyBvdmVyIHRoZSBsYXplIGRvZw=="]});
        assert_eq!(want, got);

        let roundtrip = serde_json::from_value::<MessageWithBytes>(got)?;
        assert_eq!(msg, roundtrip);
        Ok(())
    }
}
