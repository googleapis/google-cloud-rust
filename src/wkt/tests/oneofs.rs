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

//! Test serialization for oneofs.
//!
//! This shows (1) what we want the generator to produce for `oneof` fields,
//! and (2) that this serializes as we want.

#[cfg(test)]
mod test {
    use serde_json::json;
    type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

    #[test]
    fn test_oneof_single_string() -> TestResult {
        let input = MessageWithOneOf::default()
            .set_single_string(SingleString::StringContents("test-only".to_string()));
        let got = serde_json::to_value(&input)?;
        let want = json!({
            "stringContents": "test-only"
        });
        assert_eq!(got, want);
        Ok(())
    }

    #[test]
    fn test_oneof_two_strings() -> TestResult {
        let input = MessageWithOneOf::default()
            .set_two_strings(TwoStrings::StringContentsTwo("test-only".to_string()));
        let got = serde_json::to_value(&input)?;
        let want = json!({
            "stringContentsTwo": "test-only"
        });
        assert_eq!(got, want);
        Ok(())
    }

    #[test]
    fn test_oneof_one_message() -> TestResult {
        let input = MessageWithOneOf::default().set_one_message(OneMessage::MessageValue(
            Message::default().set_parent("parent-value"),
        ));
        let got = serde_json::to_value(&input)?;
        let want = json!({
            "messageValue": { "parent": "parent-value" }
        });
        assert_eq!(got, want);
        Ok(())
    }

    #[test]
    fn test_oneof_mixed() -> TestResult {
        let input = MessageWithOneOf::default().set_mixed(Mixed::AnotherMessageValue(
            Message::default().set_parent("parent-value"),
        ));
        let got = serde_json::to_value(&input)?;
        let want = json!({
            "anotherMessageValue": { "parent": "parent-value" }
        });
        assert_eq!(got, want);

        let input =
            MessageWithOneOf::default().set_mixed(Mixed::StringValue("string-value".to_string()));
        let got = serde_json::to_value(&input)?;
        let want = json!({
            "stringValue": "string-value"
        });
        assert_eq!(got, want);

        let input = MessageWithOneOf::default().set_mixed(Mixed::DurationValue(
            gcp_sdk_wkt::Duration::clamp(123, 456_000_000),
        ));
        let got = serde_json::to_value(&input)?;
        let want = json!({
            "durationValue": "123.456000000s"
        });
        assert_eq!(got, want);

        Ok(())
    }

    #[serde_with::serde_as]
    #[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]
    #[serde(default, rename_all = "camelCase")]
    #[non_exhaustive]
    pub struct MessageWithOneOf {
        #[serde(flatten, skip_serializing_if = "Option::is_none")]
        pub single_string: Option<SingleString>,

        #[serde(flatten, skip_serializing_if = "Option::is_none")]
        pub two_strings: Option<TwoStrings>,

        #[serde(flatten, skip_serializing_if = "Option::is_none")]
        pub one_message: Option<OneMessage>,

        #[serde(flatten, skip_serializing_if = "Option::is_none")]
        pub mixed: Option<Mixed>,
    }

    impl MessageWithOneOf {
        pub fn set_single_string<T: Into<SingleString>>(mut self, v: T) -> Self {
            self.single_string = Some(v.into());
            self
        }
        pub fn set_two_strings<T: Into<TwoStrings>>(mut self, v: T) -> Self {
            self.two_strings = Some(v.into());
            self
        }

        pub fn set_one_message<T: Into<OneMessage>>(mut self, v: T) -> Self {
            self.one_message = Some(v.into());
            self
        }

        pub fn set_mixed<T: Into<Mixed>>(mut self, v: T) -> Self {
            self.mixed = Some(v.into());
            self
        }
    }

    #[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
    #[serde(rename_all = "camelCase")]
    #[non_exhaustive]
    pub enum SingleString {
        /// Use a long name so we can see they are renamed to camelCase.
        StringContents(String),
    }

    #[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
    #[serde(rename_all = "camelCase")]
    #[non_exhaustive]
    pub enum TwoStrings {
        /// Use a long name so we can see they are renamed to camelCase.
        StringContentsOne(String),
        StringContentsTwo(String),
    }

    #[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
    #[serde(rename_all = "camelCase")]
    #[non_exhaustive]
    pub enum OneMessage {
        MessageValue(Message),
    }

    #[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
    #[serde(rename_all = "camelCase")]
    #[non_exhaustive]
    pub enum Mixed {
        AnotherMessageValue(Message),
        StringValue(String),
        DurationValue(gcp_sdk_wkt::Duration),
    }

    #[serde_with::serde_as]
    #[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]
    #[serde(default, rename_all = "camelCase")]
    #[non_exhaustive]
    pub struct Message {
        #[serde(skip_serializing_if = "String::is_empty")]
        pub parent: String,
    }

    impl Message {
        pub fn set_parent<T: Into<String>>(mut self, v: T) -> Self {
            self.parent = v.into();
            self
        }
    }
}
