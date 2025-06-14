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
    use common::{__MessageWithNullValue, MessageWithNullValue};
    use google_cloud_wkt::NullValue;
    use serde_json::{Value, json};
    use test_case::test_case;
    type Result = anyhow::Result<()>;

    #[test_case(MessageWithNullValue::new(), json!({}))]
    #[test_case(MessageWithNullValue::new().set_singular(NullValue), json!({}))]
    #[test_case(MessageWithNullValue::new().set_optional(NullValue), json!({"optional": null}))]
    #[test_case(MessageWithNullValue::new().set_repeated([NullValue]), json!({"repeated": [null]}))]
    #[test_case(MessageWithNullValue::new().set_map([("a", NullValue), ("b", NullValue)]), json!({"map": {"a": null, "b": null}}))]
    fn test_ser(input: MessageWithNullValue, want: Value) -> Result {
        let got = serde_json::to_value(__MessageWithNullValue(input))?;
        assert_eq!(got, want);
        Ok(())
    }

    #[test_case(MessageWithNullValue::new(), json!({}))]
    #[test_case(MessageWithNullValue::new().set_singular(NullValue), json!({"singular": null}))]
    #[test_case(MessageWithNullValue::new().set_optional(NullValue), json!({"optional": null}))]
    #[test_case(MessageWithNullValue::new().set_repeated([NullValue]), json!({"repeated": [null]}))]
    #[test_case(MessageWithNullValue::new().set_map([("a", NullValue), ("b", NullValue)]), json!({"map": {"a": null, "b": null}}))]
    fn test_de(want: MessageWithNullValue, input: Value) -> Result {
        let got = serde_json::from_value::<__MessageWithNullValue>(input)?;
        assert_eq!(got.0, want);
        Ok(())
    }

    #[test_case(r#"{"singular": null, "singular": null}"#)]
    #[test_case(r#"{"optional": null, "optional": null}"#)]
    #[test_case(r#"{"repeated": [],   "repeated": []}"#)]
    #[test_case(r#"{"map": {},        "map": {}}"#)]
    fn reject_duplicate_fields(input: &str) -> Result {
        let err = serde_json::from_str::<__MessageWithNullValue>(input).unwrap_err();
        assert!(err.is_data(), "{err:?}");
        Ok(())
    }
}
