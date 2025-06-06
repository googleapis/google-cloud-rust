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

#[cfg(test)]
mod test {
    use serde_json::{Value, json};
    use test_case::test_case;
    type Result = anyhow::Result<()>;

    #[allow(dead_code)]
    mod protos {
        use google_cloud_wkt as wkt;
        include!("generated/mod.rs");
    }
    use protos::{MessageWithEnum, message_with_enum::TestEnum};

    #[test_case(json!({"singular": "RED"}), TestEnum::Red)]
    #[test_case(json!({"singular": 1}), TestEnum::Red)]
    fn test_singular(input: Value, want: TestEnum) -> Result {
        let got = serde_json::from_value::<MessageWithEnum>(input)?;
        assert_eq!(got.singular, want, "{got:?}");
        let roundtrip = serde_json::from_value(serde_json::to_value(&got)?)?;
        assert_eq!(got, roundtrip);
        Ok(())
    }

    #[test_case(json!({}))]
    #[test_case(json!({"singular": null}))]
    #[test_case(json!({"singular": 0}))]
    #[test_case(json!({"singular": "TEST_ENUM_UNSPECIFIED"}))]
    fn test_singular_default(input: Value) -> Result {
        let got = serde_json::from_value::<MessageWithEnum>(input)?;
        assert_eq!(got, MessageWithEnum::default());
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, json!({}));
        Ok(())
    }

    #[test_case(json!({"optional": "RED"}), TestEnum::Red)]
    #[test_case(json!({"optional": 1}), TestEnum::Red)]
    fn test_optional(input: Value, want: TestEnum) -> Result {
        let got = serde_json::from_value::<MessageWithEnum>(input)?;
        assert_eq!(got.optional, Some(want), "{got:?}");
        let roundtrip = serde_json::from_value(serde_json::to_value(&got)?)?;
        assert_eq!(got, roundtrip);
        Ok(())
    }

    #[test_case(json!({}))]
    #[test_case(json!({"optional": null}))]
    fn test_optional_default(input: Value) -> Result {
        let got = serde_json::from_value::<MessageWithEnum>(input)?;
        assert_eq!(got, MessageWithEnum::default());
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, json!({}));
        Ok(())
    }

    #[test]
    fn test_repeated() -> Result {
        let msg = MessageWithEnum::new().set_repeated([TestEnum::Red, TestEnum::Unspecified]);
        let got = serde_json::to_value(&msg)?;
        let want = json!({"repeated": [1, 0]});
        assert_eq!(want, got);
        let roundtrip = serde_json::from_value(serde_json::to_value(&msg)?)?;
        assert_eq!(msg, roundtrip);
        Ok(())
    }

    #[test_case(json!({}))]
    #[test_case(json!({"repeated": []}))]
    #[test_case(json!({"repeated": null}))]
    fn test_repeated_default(input: Value) -> Result {
        let got = serde_json::from_value::<MessageWithEnum>(input)?;
        assert_eq!(got, MessageWithEnum::default());
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, json!({}));
        Ok(())
    }

    #[test]
    fn test_map() -> Result {
        let input = json!({"map": { "favorite": "RED", "not-so-much": "GREEN"}});
        let got = serde_json::from_value::<MessageWithEnum>(input.clone())?;
        let want = MessageWithEnum::new().set_map([
            ("favorite", TestEnum::Red),
            ("not-so-much", TestEnum::Green),
        ]);
        assert_eq!(want, got);
        let roundtrip = serde_json::from_value(serde_json::to_value(&got)?)?;
        assert_eq!(got, roundtrip);
        Ok(())
    }

    #[test_case(json!({}))]
    #[test_case(json!({"map": {}}))]
    #[test_case(json!({"map": null}))]
    fn test_map_default(input: Value) -> Result {
        let got = serde_json::from_value::<MessageWithEnum>(input)?;
        assert_eq!(got, MessageWithEnum::default());
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, json!({}));
        Ok(())
    }
}
