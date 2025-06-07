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
    use common::message_with_one_of::{Message, Mixed, SingleString, TwoStrings};
    use common::{__MessageWithOneOf, MessageWithOneOf};
    use google_cloud_wkt::Duration;
    use serde_json::{Value, json};
    use test_case::test_case;
    type Result = anyhow::Result<()>;

    #[test_case(MessageWithOneOf::new(), json!({}))]
    fn test_ser(input: MessageWithOneOf, want: Value) -> Result {
        let got = serde_json::to_value(__MessageWithOneOf(input))?;
        assert_eq!(got, want);
        Ok(())
    }

    #[test_case(MessageWithOneOf::new(), json!({}))]
    fn test_de(want: MessageWithOneOf, input: Value) -> Result {
        let got = serde_json::from_value::<__MessageWithOneOf>(input)?;
        assert_eq!(got.0, want);
        Ok(())
    }

    #[test]
    fn test_oneof_single_string() -> Result {
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
    fn test_oneof_two_strings() -> Result {
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
    fn test_oneof_one_message() -> Result {
        let input = MessageWithOneOf::default()
            .set_message_value(Message::default().set_parent("parent-value"));
        let got = serde_json::to_value(&input)?;
        let want = json!({
            "messageValue": { "parent": "parent-value" }
        });
        assert_eq!(got, want);
        Ok(())
    }

    #[test]
    fn test_oneof_mixed() -> Result {
        let input = MessageWithOneOf::default()
            .set_another_message(Message::default().set_parent("parent-value"));
        let got = serde_json::to_value(&input)?;
        let want = json!({
            "anotherMessage": { "parent": "parent-value" }
        });
        assert_eq!(got, want);

        let input =
            MessageWithOneOf::default().set_mixed(Mixed::String("string-value".to_string()));
        let got = serde_json::to_value(&input)?;
        let want = json!({
            "string": "string-value"
        });
        assert_eq!(got, want);

        let input = MessageWithOneOf::default().set_duration(Duration::clamp(123, 456_000_000));
        let got = serde_json::to_value(&input)?;
        let want = json!({
            "duration": "123.456s"
        });
        assert_eq!(got, want);

        Ok(())
    }
}
