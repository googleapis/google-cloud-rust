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
    use google_cloud_wkt::FieldMask;
    use serde_json::{Value, json};
    use test_case::test_case;

    type Result = anyhow::Result<()>;

    #[allow(dead_code)]
    mod protos {
        use google_cloud_wkt as wkt;
        include!("generated/mod.rs");
    }
    use protos::MessageWithFieldMask;

    #[test_case(json!({"singular": ""}), FieldMask::default())]
    #[test_case(json!({"singular": "a,b,c"}), FieldMask::default().set_paths(["a", "b", "c"]))]
    fn test_singular(value: Value, want: FieldMask) -> Result {
        let got = serde_json::from_value::<MessageWithFieldMask>(value.clone())?;
        assert_eq!(got.singular, Some(want));
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, value);
        Ok(())
    }

    #[test_case(json!({}))]
    #[test_case(json!({"singular": null}))]
    fn test_singular_default(input: Value) -> Result {
        let got = serde_json::from_value::<MessageWithFieldMask>(input)?;
        let want = MessageWithFieldMask::default();
        assert_eq!(got, want);
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, json!({}));
        Ok(())
    }

    #[test_case(json!({"optional": ""}), FieldMask::default())]
    #[test_case(json!({"optional": "a,b,c"}), FieldMask::default().set_paths(["a", "b", "c"]))]
    fn test_optional(value: Value, want: FieldMask) -> Result {
        let got = serde_json::from_value::<MessageWithFieldMask>(value.clone())?;
        assert_eq!(got.optional, Some(want));
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, value);
        Ok(())
    }

    #[test_case(json!({}))]
    #[test_case(json!({"optional": null}))]
    fn test_optional_default(input: Value) -> Result {
        let got = serde_json::from_value::<MessageWithFieldMask>(input)?;
        let want = MessageWithFieldMask::default();
        assert_eq!(got, want);
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, json!({}));
        Ok(())
    }

    #[test_case(json!({"repeated": [""]}), MessageWithFieldMask::new().set_repeated([FieldMask::default()]))]
    #[test_case(json!({"repeated": ["a,b,c"]}), MessageWithFieldMask::new().set_repeated([FieldMask::default().set_paths(["a", "b", "c"])]))]
    fn test_repeated(value: Value, want: MessageWithFieldMask) -> Result {
        let got = serde_json::from_value::<MessageWithFieldMask>(value.clone())?;
        assert_eq!(got, want);
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, value);
        Ok(())
    }

    #[test_case(json!({}))]
    #[test_case(json!({"repeated": []}))]
    // TODO(#2328) - #[test_case(json!({"repeated": null}))]
    fn test_repeated_default(input: Value) -> Result {
        let got = serde_json::from_value::<MessageWithFieldMask>(input)?;
        let want = MessageWithFieldMask::default();
        assert_eq!(got, want);
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, json!({}));
        Ok(())
    }

    #[test_case(json!({"map": {"key": ""}}), MessageWithFieldMask::new().set_map([("key", FieldMask::default())]))]
    #[test_case(json!({"map": {"key": "a,b,c"}}), MessageWithFieldMask::new().set_map([("key", FieldMask::default().set_paths(["a", "b", "c"]))]))]
    fn test_map(value: Value, want: MessageWithFieldMask) -> Result {
        let got = serde_json::from_value::<MessageWithFieldMask>(value.clone())?;
        assert_eq!(got, want);
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, value);
        Ok(())
    }

    #[test_case(json!({}))]
    #[test_case(json!({"map": {}}))]
    // TODO(#2328) - #[test_case(json!({"map": null}))]
    fn test_map_default(input: Value) -> Result {
        let got = serde_json::from_value::<MessageWithFieldMask>(input)?;
        let want = MessageWithFieldMask::default();
        assert_eq!(got, want);
        let output = serde_json::to_value(&got)?;
        assert_eq!(output, json!({}));
        Ok(())
    }
}
