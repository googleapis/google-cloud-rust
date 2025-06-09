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
    use common::MessageWithBytes;
    use serde_json::json;
    type Result = anyhow::Result<()>;

    #[test]
    fn test_serialize_singular() -> Result {
        let b = bytes::Bytes::from("the quick brown fox jumps over the laze dog");
        let msg = MessageWithBytes::new().set_singular(b);
        let got = serde_json::to_value(&msg)?;
        let want =
            json!({"singular": "dGhlIHF1aWNrIGJyb3duIGZveCBqdW1wcyBvdmVyIHRoZSBsYXplIGRvZw=="});
        assert_eq!(want, got);

        let roundtrip = serde_json::from_value::<MessageWithBytes>(got)?;
        assert_eq!(msg, roundtrip);
        Ok(())
    }

    #[test]
    fn test_serialize_optional() -> Result {
        let b = bytes::Bytes::from("the quick brown fox jumps over the laze dog");
        let msg = MessageWithBytes::new().set_optional(b);
        let got = serde_json::to_value(&msg)?;
        let want =
            json!({"optional": "dGhlIHF1aWNrIGJyb3duIGZveCBqdW1wcyBvdmVyIHRoZSBsYXplIGRvZw=="});
        assert_eq!(want, got);

        let roundtrip = serde_json::from_value::<MessageWithBytes>(got)?;
        assert_eq!(msg, roundtrip);
        Ok(())
    }

    #[test]
    fn test_serialize_repeated() -> Result {
        let b = bytes::Bytes::from("the quick brown fox jumps over the laze dog");
        let msg = MessageWithBytes::new().set_repeated([b]);
        let got = serde_json::to_value(&msg)?;
        let want =
            json!({"repeated": ["dGhlIHF1aWNrIGJyb3duIGZveCBqdW1wcyBvdmVyIHRoZSBsYXplIGRvZw=="]});
        assert_eq!(want, got);

        let roundtrip = serde_json::from_value::<MessageWithBytes>(got)?;
        assert_eq!(msg, roundtrip);
        Ok(())
    }

    #[test]
    fn test_serialize_map() -> Result {
        let b = bytes::Bytes::from("the quick brown fox jumps over the laze dog");
        let msg = MessageWithBytes::new().set_map([("quick", b)]);
        let got = serde_json::to_value(&msg)?;
        let want = json!({"map": {"quick": "dGhlIHF1aWNrIGJyb3duIGZveCBqdW1wcyBvdmVyIHRoZSBsYXplIGRvZw=="}});
        assert_eq!(want, got);

        let roundtrip = serde_json::from_value::<MessageWithBytes>(got)?;
        assert_eq!(msg, roundtrip);
        Ok(())
    }
}
