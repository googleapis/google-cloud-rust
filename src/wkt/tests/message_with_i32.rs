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
    use protos::MessageWithI32;

    #[test_case(123, 123)]
    #[test_case(-234, -234)]
    fn test_singular<T>(input: T, want: i32) -> Result
    where
        T: serde::ser::Serialize,
    {
        let value = json!({"singular": input});
        let got = serde_json::from_value::<MessageWithI32>(value)?;
        let output = json!({"singular": want});
        assert_eq!(got, MessageWithI32::new().set_singular(want));
        let trip = serde_json::to_value(&got)?;
        assert_eq!(trip, output);
        Ok(())
    }

    #[test_case(json!({}))]
    #[test_case(json!({"singular": 0}))]
    fn test_singular_default(input: Value) -> Result {
        let want = MessageWithI32::new().set_singular(0);
        let got = serde_json::from_value::<MessageWithI32>(input)?;
        assert_eq!(got, want);
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, json!({}));
        Ok(())
    }

    #[test_case(0, 0)]
    #[test_case(123, 123)]
    #[test_case(-234, -234)]
    fn test_optional<T>(input: T, want: i32) -> Result
    where
        T: serde::ser::Serialize,
    {
        let value = json!({"optional": input});
        let got = serde_json::from_value::<MessageWithI32>(value)?;
        let output = json!({"optional": want});
        assert_eq!(got, MessageWithI32::new().set_optional(want));
        let trip = serde_json::to_value(&got)?;
        assert_eq!(trip, output);
        Ok(())
    }

    #[test_case(json!({}))]
    #[test_case(json!({"optional": null}))]
    fn test_optional_none(input: Value) -> Result {
        let want = MessageWithI32::new().set_or_clear_optional(None::<i32>);
        let got = serde_json::from_value::<MessageWithI32>(input)?;
        assert_eq!(got, want);
        Ok(())
    }

    #[test_case(0, 0)]
    #[test_case(123, 123)]
    #[test_case(-234, -234)]
    fn test_repeated<T>(input: T, want: i32) -> Result
    where
        T: serde::ser::Serialize,
    {
        let value = json!({"repeated": [input]});
        let got = serde_json::from_value::<MessageWithI32>(value)?;
        let output = json!({"repeated": [want]});
        assert_eq!(got, MessageWithI32::new().set_repeated([want]));
        let trip = serde_json::to_value(&got)?;
        assert_eq!(trip, output);
        Ok(())
    }

    #[test_case(json!({}))]
    #[test_case(json!({"repeated": []}))]
    fn test_repeated_default(input: Value) -> Result {
        let want = MessageWithI32::new();
        let got = serde_json::from_value::<MessageWithI32>(input)?;
        assert_eq!(got, want);
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, json!({}));
        Ok(())
    }

    #[test_case(0, 0)]
    #[test_case(123, 123)]
    #[test_case(-234, -234)]
    fn test_map_value<T>(input: T, want: i32) -> Result
    where
        T: serde::ser::Serialize,
    {
        let value = json!({"mapValue": {"test": input}});
        let got = serde_json::from_value::<MessageWithI32>(value)?;
        let output = json!({"mapValue": {"test": want}});
        assert_eq!(
            got,
            MessageWithI32::new().set_map_value([("test".to_string(), want)])
        );
        let trip = serde_json::to_value(&got)?;
        assert_eq!(trip, output);
        Ok(())
    }

    #[test_case(json!({}))]
    #[test_case(json!({"mapValue": {}}))]
    fn test_map_value_default(input: Value) -> Result {
        let want = MessageWithI32::default();
        let got = serde_json::from_value::<MessageWithI32>(input)?;
        assert_eq!(got, want);
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, json!({}));
        Ok(())
    }

    #[test_case(json!({}))]
    #[test_case(json!({"mapKey": {}}))]
    fn test_map_key_default(input: Value) -> Result {
        let want = MessageWithI32::default();
        let got = serde_json::from_value::<MessageWithI32>(input)?;
        assert_eq!(got, want);
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, json!({}));
        Ok(())
    }

    #[test_case(json!({}))]
    #[test_case(json!({"mapKeyValue": {}}))]
    fn test_map_key_value_default(input: Value) -> Result {
        let want = MessageWithI32::default();
        let got = serde_json::from_value::<MessageWithI32>(input)?;
        assert_eq!(got, want);
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, json!({}));
        Ok(())
    }
}
