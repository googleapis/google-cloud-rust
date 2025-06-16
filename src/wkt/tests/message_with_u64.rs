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
    use common::{__MessageWithU64, MessageWithU64};
    use serde_json::{Value, json};
    use test_case::test_case;
    type Result = anyhow::Result<()>;

    #[test_case(MessageWithU64::new(), json!({}))]
    #[test_case(MessageWithU64::new().set_singular(0_u64), json!({}))]
    #[test_case(MessageWithU64::new().set_singular(42_u64), json!({"singular": "42"}))]
    #[test_case(MessageWithU64::new().set_optional(0_u64), json!({"optional": "0"}))]
    #[test_case(MessageWithU64::new().set_or_clear_optional(None::<u64>), json!({}))]
    #[test_case(MessageWithU64::new().set_optional(42_u64), json!({"optional": "42"}))]
    #[test_case(MessageWithU64::new().set_repeated([0_u64;0]), json!({}))]
    #[test_case(MessageWithU64::new().set_repeated([0_u64, 1, 2]), json!({"repeated": ["0", "1", "2"]}))]
    #[test_case(MessageWithU64::new().set_map_value([("", 0_u64);0]), json!({}))]
    #[test_case(MessageWithU64::new().set_map_value([("", 0_u64)]), json!({"mapValue": {"": "0"}}))]
    #[test_case(MessageWithU64::new().set_map_key([(0_u64, "");0]), json!({}))]
    #[test_case(MessageWithU64::new().set_map_key([(0_u64, "")]), json!({"mapKey": {"0": ""}}))]
    #[test_case(MessageWithU64::new().set_map_key_value([(0_u64, 0_u64);0]), json!({}))]
    #[test_case(MessageWithU64::new().set_map_key_value([(0_u64, 0_u64)]), json!({"mapKeyValue": {"0": "0"}}))]
    fn test_ser(input: MessageWithU64, want: Value) -> Result {
        let got = serde_json::to_value(__MessageWithU64(input))?;
        assert_eq!(got, want);
        Ok(())
    }

    #[test_case(MessageWithU64::new(), json!({}))]
    #[test_case(MessageWithU64::new().set_singular(0_u64), json!({"singular": null}))]
    #[test_case(MessageWithU64::new().set_singular(0_u64), json!({}))]
    #[test_case(MessageWithU64::new().set_singular(42_u64), json!({"singular": "42"}))]
    #[test_case(MessageWithU64::new().set_optional(0_u64), json!({"optional": "0"}))]
    #[test_case(MessageWithU64::new().set_or_clear_optional(None::<u64>), json!({}))]
    #[test_case(MessageWithU64::new().set_optional(42_u64), json!({"optional": "42"}))]
    #[test_case(MessageWithU64::new().set_repeated([0_u64;0]), json!({}))]
    #[test_case(MessageWithU64::new().set_repeated([0_u64, 1, 2]), json!({"repeated": [0, 1, 2]}))]
    #[test_case(MessageWithU64::new().set_repeated([0_u64, 1, 20]), json!({"repeated": [0.0, "1.0", 2e1]}))]
    #[test_case(MessageWithU64::new().set_map_value([("", 0_u64);0]), json!({}))]
    #[test_case(MessageWithU64::new().set_map_value([("", 0_u64)]), json!({"mapValue": {"": "0"}}))]
    #[test_case(MessageWithU64::new().set_map_key([(0_u64, "");0]), json!({}))]
    #[test_case(MessageWithU64::new().set_map_key([(0_u64, "")]), json!({"mapKey": {"0": ""}}))]
    #[test_case(MessageWithU64::new().set_map_key_value([(0_u64, 0_u64);0]), json!({}))]
    #[test_case(MessageWithU64::new().set_map_key_value([(0_u64, 0_u64)]), json!({"mapKeyValue": {"0": "0"}}))]
    #[test_case(MessageWithU64::new().set_map_key_value([(0_u64, 0_u64)]), json!({"map_key_value": {"0": "0"}}))]
    fn test_de(want: MessageWithU64, input: Value) -> Result {
        let got = serde_json::from_value::<__MessageWithU64>(input)?;
        assert_eq!(got.0, want);
        Ok(())
    }

    #[test_case(json!({"unknown": "test-value"}))]
    #[test_case(json!({"unknown": "test-value", "moreUnknown": {"a": 1, "b": 2}}))]
    fn test_unknown(input: Value) -> Result {
        let deser = serde_json::from_value::<__MessageWithU64>(input.clone())?;
        let got = serde_json::to_value(deser)?;
        assert_eq!(got, input);
        Ok(())
    }

    #[test_case(123, 123)]
    #[test_case("345", 345)]
    #[test_case("567.0", 567)]
    fn test_singular<T>(input: T, want: u64) -> Result
    where
        T: serde::ser::Serialize,
    {
        let value = json!({"singular": input});
        let got = serde_json::from_value::<MessageWithU64>(value)?;
        let output = json!({"singular": want.to_string()});
        assert_eq!(got, MessageWithU64::new().set_singular(want));
        let trip = serde_json::to_value(&got)?;
        assert_eq!(trip, output);
        Ok(())
    }

    #[test_case(json!({}))]
    #[test_case(json!({"singular": 0}))]
    #[test_case(json!({"singular": null}))]
    fn test_singular_default(input: Value) -> Result {
        let want = MessageWithU64::new().set_singular(0_u64);
        let got = serde_json::from_value::<MessageWithU64>(input)?;
        assert_eq!(got, want);
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, json!({}));
        Ok(())
    }

    #[test_case(0, 0)]
    #[test_case(123, 123)]
    #[test_case("345", 345)]
    #[test_case("567.0", 567)]
    fn test_optional<T>(input: T, want: u64) -> Result
    where
        T: serde::ser::Serialize,
    {
        let value = json!({"optional": input});
        let got = serde_json::from_value::<MessageWithU64>(value)?;
        let output = json!({"optional": want.to_string()});
        assert_eq!(got, MessageWithU64::new().set_optional(want));
        let trip = serde_json::to_value(&got)?;
        assert_eq!(trip, output);
        Ok(())
    }

    #[test_case(json!({}))]
    #[test_case(json!({"optional": null}))]
    fn test_optional_none(input: Value) -> Result {
        let want = MessageWithU64::new().set_or_clear_optional(None::<u64>);
        let got = serde_json::from_value::<MessageWithU64>(input)?;
        assert_eq!(got, want);
        Ok(())
    }

    #[test_case(0, 0)]
    #[test_case(123, 123)]
    #[test_case("345", 345)]
    #[test_case("567.0", 567)]
    fn test_repeated<T>(input: T, want: u64) -> Result
    where
        T: serde::ser::Serialize,
    {
        let value = json!({"repeated": [input]});
        let got = serde_json::from_value::<MessageWithU64>(value)?;
        let output = json!({"repeated": [want.to_string()]});
        assert_eq!(got, MessageWithU64::new().set_repeated([want]));
        let trip = serde_json::to_value(&got)?;
        assert_eq!(trip, output);
        Ok(())
    }

    #[test_case(json!({}))]
    #[test_case(json!({"repeated": []}))]
    #[test_case(json!({"repeated": null}))]
    fn test_repeated_default(input: Value) -> Result {
        let want = MessageWithU64::new();
        let got = serde_json::from_value::<MessageWithU64>(input)?;
        assert_eq!(got, want);
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, json!({}));
        Ok(())
    }

    #[test_case(0, 0)]
    #[test_case(123, 123)]
    #[test_case("345", 345)]
    #[test_case("567.0", 567)]
    fn test_map_value<T>(input: T, want: u64) -> Result
    where
        T: serde::ser::Serialize,
    {
        let value = json!({"mapValue": {"test": input}});
        let got = serde_json::from_value::<MessageWithU64>(value)?;
        let output = json!({"mapValue": {"test": want.to_string()}});
        assert_eq!(
            got,
            MessageWithU64::new().set_map_value([("test".to_string(), want)])
        );
        let trip = serde_json::to_value(&got)?;
        assert_eq!(trip, output);
        Ok(())
    }

    #[test_case(json!({}))]
    #[test_case(json!({"mapValue": {}}))]
    #[test_case(json!({"mapValue": null}))]
    fn test_map_value_default(input: Value) -> Result {
        let want = MessageWithU64::default();
        let got = serde_json::from_value::<MessageWithU64>(input)?;
        assert_eq!(got, want);
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, json!({}));
        Ok(())
    }

    #[test_case("0", 0)]
    #[test_case("123", 123)]
    #[test_case("345", 345)]
    #[test_case("567.0", 567)]
    fn test_map_key<T>(input: T, want: u64) -> Result
    where
        T: Into<String>,
    {
        let value = json!({"mapKey": {input: "test"}});
        let got = serde_json::from_value::<MessageWithU64>(value)?;
        let output = json!({"mapKey": {want.to_string(): "test"}});
        assert_eq!(
            got,
            MessageWithU64::new().set_map_key([(want, "test".to_string())])
        );
        let trip = serde_json::to_value(&got)?;
        assert_eq!(trip, output);
        Ok(())
    }

    #[test_case(json!({}))]
    #[test_case(json!({"mapKey": {}}))]
    #[test_case(json!({"mapKey": null}))]
    fn test_map_key_default(input: Value) -> Result {
        let want = MessageWithU64::default();
        let got = serde_json::from_value::<MessageWithU64>(input)?;
        assert_eq!(got, want);
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, json!({}));
        Ok(())
    }

    #[test_case("0", "0", 0, 0; "string zero")]
    #[test_case("0", 0, 0, 0)]
    #[test_case("0.0", 0, 0, 0)]
    #[test_case("123", 234, 123, 234)]
    #[test_case("123.0", "345", 123, 345)]
    fn test_map_key_value<K, V>(key: K, value: V, want_key: u64, want_value: u64) -> Result
    where
        K: ToString,
        V: ToString,
    {
        let value = json!({"mapKeyValue": {key.to_string(): value.to_string()}});
        let got = serde_json::from_value::<MessageWithU64>(value)?;
        let output = json!({"mapKeyValue": {want_key.to_string(): want_value.to_string()}});
        assert_eq!(
            got,
            MessageWithU64::new().set_map_key_value([(want_key, want_value)])
        );
        let trip = serde_json::to_value(&got)?;
        assert_eq!(trip, output);
        Ok(())
    }

    #[test_case(json!({}))]
    #[test_case(json!({"mapKeyValue": {}}))]
    #[test_case(json!({"mapKeyValue": null}))]
    fn test_map_key_value_default(input: Value) -> Result {
        let want = MessageWithU64::default();
        let got = serde_json::from_value::<MessageWithU64>(input)?;
        assert_eq!(got, want);
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, json!({}));
        Ok(())
    }
}
