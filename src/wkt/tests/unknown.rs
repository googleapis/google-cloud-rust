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

//! Test serialization for unknown fields.
//!
//! This shows how the generator could handle unknown fields in the future.

#[cfg(test)]
mod tests {
    use serde_json::json;
    use std::collections::HashMap;

    #[test]
    fn roundtrip_from_json() -> anyhow::Result<()> {
        let input = json!({
            "field0": "abc123",
            "field1": 42,
            "field2": {
                "child0": "c0",
                "child1": 7,
                "uc1": "abc",
            },
            "field3": [
                {"child0": "c1", "uc2": "def"},
            ],
            "field4": {
                "name0": {
                    "child0": "c2",
                    "uc1": "abc",
                    "uc2": "def"
                }
            },
            "extraField0": "field2-value",
            "extraField1": {
                "a": 7,
                "b": "8",
            }
        });

        let message = serde_json::from_value::<MessageWithUnknown>(input.clone())?;
        assert_eq!(message.field_0, "abc123");
        assert_eq!(message.field_1, Some(42));
        let got = serde_json::to_value(message)?;
        assert_eq!(input, got);
        Ok(())
    }

    #[test]
    fn to_json() -> anyhow::Result<()> {
        let message = MessageWithUnknown::new()
            .set_field_0("abc123")
            .set_field_1(42)
            .set_field_2(ChildMessage::new().set_child_0("c0").set_child_1(7))
            .set_field_3([ChildMessage::new().set_child_0("c1")])
            .set_field_4([("name", ChildMessage::new().set_child_0("c2"))]);
        let got = serde_json::to_value(message)?;
        let want = json!({
            "field0": "abc123",
            "field1": 42,
            "field2": {
                "child0": "c0",
                "child1": 7
            },
            "field3": [{"child0": "c1"}],
            "field4": {
                "name": {"child0": "c2"},
            },
        });
        assert_eq!(got, want);
        Ok(())
    }

    #[serde_with::serde_as]
    #[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]
    #[serde(default, rename_all = "camelCase")]
    #[non_exhaustive]
    pub struct MessageWithUnknown {
        #[serde(skip_serializing_if = "String::is_empty")]
        pub field_0: String,

        #[serde(skip_serializing_if = "Option::is_none")]
        pub field_1: Option<i32>,

        #[serde(skip_serializing_if = "Option::is_none")]
        pub field_2: Option<ChildMessage>,

        #[serde(skip_serializing_if = "Vec::is_empty")]
        pub field_3: Vec<ChildMessage>,

        #[serde(skip_serializing_if = "HashMap::is_empty")]
        pub field_4: HashMap<String, ChildMessage>,

        #[serde(flatten)]
        _unknown_fields: Option<serde_json::Map<String, serde_json::Value>>,
    }

    impl MessageWithUnknown {
        pub fn new() -> Self {
            Self::default()
        }

        pub fn set_field_0<V: Into<String>>(mut self, v: V) -> Self {
            self.field_0 = v.into();
            self
        }

        pub fn set_field_1<V: Into<i32>>(mut self, v: V) -> Self {
            self.field_1 = Some(v.into());
            self
        }

        pub fn set_field_2<V: Into<ChildMessage>>(mut self, v: V) -> Self {
            self.field_2 = Some(v.into());
            self
        }

        pub fn set_field_3<I, V>(mut self, i: I) -> Self
        where
            I: IntoIterator<Item = V>,
            V: Into<ChildMessage>,
        {
            self.field_3 = i.into_iter().map(|v| v.into()).collect();
            self
        }

        pub fn set_field_4<I, K, V>(mut self, i: I) -> Self
        where
            I: IntoIterator<Item = (K, V)>,
            K: Into<String>,
            V: Into<ChildMessage>,
        {
            self.field_4 = i.into_iter().map(|(k, v)| (k.into(), v.into())).collect();
            self
        }
    }

    #[serde_with::serde_as]
    #[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]
    #[serde(default, rename_all = "camelCase")]
    #[non_exhaustive]
    pub struct ChildMessage {
        #[serde(skip_serializing_if = "String::is_empty")]
        pub child_0: String,

        #[serde(skip_serializing_if = "Option::is_none")]
        pub child_1: Option<i32>,

        #[serde(flatten)]
        _unknown_fields: Option<serde_json::Map<String, serde_json::Value>>,
    }

    impl ChildMessage {
        pub fn new() -> Self {
            Self::default()
        }

        pub fn set_child_0<V: Into<String>>(mut self, v: V) -> Self {
            self.child_0 = v.into();
            self
        }

        pub fn set_child_1<V: Into<i32>>(mut self, v: V) -> Self {
            self.child_1 = Some(v.into());
            self
        }
    }
}
