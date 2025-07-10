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
mod tests {
    use serde_json::json;
    type Result = std::result::Result<(), Box<dyn std::error::Error>>;

    #[serde_with::serde_as]
    #[serde_with::skip_serializing_none]
    #[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct FieldNameOverrides {
        pub simple_snake: String,
        #[serde(rename = "dataCrc32c")]
        pub data_crc_32_c: String,
    }

    #[test]
    fn test_serialize_names() -> Result {
        let msg = FieldNameOverrides {
            simple_snake: "123".to_string(),
            data_crc_32_c: "abc".to_string(),
        };
        let got = serde_json::to_value(&msg)?;
        let want = json!({"simpleSnake": "123", "dataCrc32c": "abc"});
        assert_eq!(want, got);

        Ok(())
    }
}
