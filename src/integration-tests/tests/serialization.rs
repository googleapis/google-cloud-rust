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

//! Verify sidekick generate types with the expected serialization behavior.

#[cfg(test)]
mod serialization {
    use anyhow::Result;

    #[test]
    fn multiple_serde_attributes() -> Result<()> {
        let input = Test {
            f_bytes: bytes::Bytes::from("the quick brown fox jumps over the lazy dog"),
            ..Default::default()
        };
        let got = serde_json::to_value(&input)?;
        let want = serde_json::json!({
            "fancyName": "dGhlIHF1aWNrIGJyb3duIGZveCBqdW1wcyBvdmVyIHRoZSBsYXp5IGRvZw=="
        });
        assert_eq!(got, want);

        let input = Test {
            f_string: "the quick brown fox jumps over the lazy dog".to_string(),
            ..Default::default()
        };
        let got = serde_json::to_value(&input)?;
        let want = serde_json::json!({
            "fString": "the quick brown fox jumps over the lazy dog"
        });
        assert_eq!(got, want);

        let input = Test::default();
        let got = serde_json::to_value(&input)?;
        let want = serde_json::json!({});
        assert_eq!(got, want);

        Ok(())
    }

    #[serde_with::serde_as]
    #[derive(Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
    struct Test {
        #[serde(rename = "fancyName")]
        #[serde(skip_serializing_if = "bytes::Bytes::is_empty")]
        #[serde_as(as = "serde_with::base64::Base64")]
        f_bytes: bytes::Bytes,

        #[serde(rename = "fString")]
        #[serde(skip_serializing_if = "String::is_empty")]
        f_string: String,
    }
}
