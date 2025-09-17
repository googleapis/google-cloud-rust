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

impl<'de> serde::de::Deserialize<'de> for crate::model::ExternalAccountKey {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[allow(non_camel_case_types)]
        #[doc(hidden)]
        #[derive(PartialEq, Eq, Hash)]
        enum __FieldTag {
            __name,
            __key_id,
            __b64_mac_key,
            Unknown(std::string::String),
        }
        impl<'de> serde::de::Deserialize<'de> for __FieldTag {
            fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct Visitor;
                impl<'de> serde::de::Visitor<'de> for Visitor {
                    type Value = __FieldTag;
                    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                        formatter.write_str("a field name for ExternalAccountKey")
                    }
                    fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
                    where
                        E: serde::de::Error,
                    {
                        use std::result::Result::Ok;
                        use std::string::ToString;
                        match value {
                            "name" => Ok(__FieldTag::__name),
                            "keyId" => Ok(__FieldTag::__key_id),
                            "key_id" => Ok(__FieldTag::__key_id),
                            "b64MacKey" => Ok(__FieldTag::__b64_mac_key),
                            "b64_mac_key" => Ok(__FieldTag::__b64_mac_key),
                            _ => Ok(__FieldTag::Unknown(value.to_string())),
                        }
                    }
                }
                deserializer.deserialize_identifier(Visitor)
            }
        }
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = crate::model::ExternalAccountKey;
            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("struct ExternalAccountKey")
            }
            fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                #[allow(unused_imports)]
                use serde::de::Error;
                use std::option::Option::Some;
                let mut fields = std::collections::HashSet::new();
                let mut result = Self::Value::new();
                while let Some(tag) = map.next_key::<__FieldTag>()? {
                    #[allow(clippy::match_single_binding)]
                    match tag {
                        __FieldTag::__name => {
                            if !fields.insert(__FieldTag::__name) {
                                return std::result::Result::Err(A::Error::duplicate_field(
                                    "multiple values for name",
                                ));
                            }
                            result.name = map
                                .next_value::<std::option::Option<std::string::String>>()?
                                .unwrap_or_default();
                        }
                        __FieldTag::__key_id => {
                            if !fields.insert(__FieldTag::__key_id) {
                                return std::result::Result::Err(A::Error::duplicate_field(
                                    "multiple values for key_id",
                                ));
                            }
                            result.key_id = map
                                .next_value::<std::option::Option<std::string::String>>()?
                                .unwrap_or_default();
                        }
                        __FieldTag::__b64_mac_key => {
                            if !fields.insert(__FieldTag::__b64_mac_key) {
                                return std::result::Result::Err(A::Error::duplicate_field(
                                    "multiple values for b64_mac_key",
                                ));
                            }
                            struct __With(std::option::Option<::bytes::Bytes>);
                            impl<'de> serde::de::Deserialize<'de> for __With {
                                fn deserialize<D>(
                                    deserializer: D,
                                ) -> std::result::Result<Self, D::Error>
                                where
                                    D: serde::de::Deserializer<'de>,
                                {
                                    serde_with::As::< std::option::Option<serde_with::base64::Base64> >::deserialize(deserializer).map(__With)
                                }
                            }
                            result.b64_mac_key = map.next_value::<__With>()?.0.unwrap_or_default();
                        }
                        __FieldTag::Unknown(key) => {
                            let value = map.next_value::<serde_json::Value>()?;
                            result._unknown_fields.insert(key, value);
                        }
                    }
                }
                std::result::Result::Ok(result)
            }
        }
        deserializer.deserialize_any(Visitor)
    }
}

impl serde::ser::Serialize for crate::model::ExternalAccountKey {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        use serde::ser::SerializeMap;
        #[allow(unused_imports)]
        use std::option::Option::Some;
        let mut state = serializer.serialize_map(std::option::Option::None)?;
        if !self.name.is_empty() {
            state.serialize_entry("name", &self.name)?;
        }
        if !self.key_id.is_empty() {
            state.serialize_entry("keyId", &self.key_id)?;
        }
        if !self.b64_mac_key.is_empty() {
            struct __With<'a>(&'a ::bytes::Bytes);
            impl<'a> serde::ser::Serialize for __With<'a> {
                fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
                where
                    S: serde::ser::Serializer,
                {
                    serde_with::As::<serde_with::base64::Base64>::serialize(self.0, serializer)
                }
            }
            state.serialize_entry("b64MacKey", &__With(&self.b64_mac_key))?;
        }
        if !self._unknown_fields.is_empty() {
            for (key, value) in self._unknown_fields.iter() {
                state.serialize_entry(key, &value)?;
            }
        }
        state.end()
    }
}

impl std::fmt::Debug for crate::model::ExternalAccountKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug_struct = f.debug_struct("ExternalAccountKey");
        debug_struct.field("name", &self.name);
        debug_struct.field("key_id", &self.key_id);
        debug_struct.field("b64_mac_key", &self.b64_mac_key);
        if !self._unknown_fields.is_empty() {
            debug_struct.field("_unknown_fields", &self._unknown_fields);
        }
        debug_struct.finish()
    }
}

impl<'de> serde::de::Deserialize<'de> for crate::model::CreateExternalAccountKeyRequest {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[allow(non_camel_case_types)]
        #[doc(hidden)]
        #[derive(PartialEq, Eq, Hash)]
        enum __FieldTag {
            __parent,
            __external_account_key,
            Unknown(std::string::String),
        }
        impl<'de> serde::de::Deserialize<'de> for __FieldTag {
            fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct Visitor;
                impl<'de> serde::de::Visitor<'de> for Visitor {
                    type Value = __FieldTag;
                    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                        formatter.write_str("a field name for CreateExternalAccountKeyRequest")
                    }
                    fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
                    where
                        E: serde::de::Error,
                    {
                        use std::result::Result::Ok;
                        use std::string::ToString;
                        match value {
                            "parent" => Ok(__FieldTag::__parent),
                            "externalAccountKey" => Ok(__FieldTag::__external_account_key),
                            "external_account_key" => Ok(__FieldTag::__external_account_key),
                            _ => Ok(__FieldTag::Unknown(value.to_string())),
                        }
                    }
                }
                deserializer.deserialize_identifier(Visitor)
            }
        }
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = crate::model::CreateExternalAccountKeyRequest;
            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("struct CreateExternalAccountKeyRequest")
            }
            fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                #[allow(unused_imports)]
                use serde::de::Error;
                use std::option::Option::Some;
                let mut fields = std::collections::HashSet::new();
                let mut result = Self::Value::new();
                while let Some(tag) = map.next_key::<__FieldTag>()? {
                    #[allow(clippy::match_single_binding)]
                    match tag {
                        __FieldTag::__parent => {
                            if !fields.insert(__FieldTag::__parent) {
                                return std::result::Result::Err(A::Error::duplicate_field(
                                    "multiple values for parent",
                                ));
                            }
                            result.parent = map
                                .next_value::<std::option::Option<std::string::String>>()?
                                .unwrap_or_default();
                        }
                        __FieldTag::__external_account_key => {
                            if !fields.insert(__FieldTag::__external_account_key) {
                                return std::result::Result::Err(A::Error::duplicate_field(
                                    "multiple values for external_account_key",
                                ));
                            }
                            result.external_account_key = map.next_value::<std::option::Option<crate::model::ExternalAccountKey>>()?
                                ;
                        }
                        __FieldTag::Unknown(key) => {
                            let value = map.next_value::<serde_json::Value>()?;
                            result._unknown_fields.insert(key, value);
                        }
                    }
                }
                std::result::Result::Ok(result)
            }
        }
        deserializer.deserialize_any(Visitor)
    }
}

impl serde::ser::Serialize for crate::model::CreateExternalAccountKeyRequest {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        use serde::ser::SerializeMap;
        #[allow(unused_imports)]
        use std::option::Option::Some;
        let mut state = serializer.serialize_map(std::option::Option::None)?;
        if !self.parent.is_empty() {
            state.serialize_entry("parent", &self.parent)?;
        }
        if self.external_account_key.is_some() {
            state.serialize_entry("externalAccountKey", &self.external_account_key)?;
        }
        if !self._unknown_fields.is_empty() {
            for (key, value) in self._unknown_fields.iter() {
                state.serialize_entry(key, &value)?;
            }
        }
        state.end()
    }
}

impl std::fmt::Debug for crate::model::CreateExternalAccountKeyRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug_struct = f.debug_struct("CreateExternalAccountKeyRequest");
        debug_struct.field("parent", &self.parent);
        debug_struct.field("external_account_key", &self.external_account_key);
        if !self._unknown_fields.is_empty() {
            debug_struct.field("_unknown_fields", &self._unknown_fields);
        }
        debug_struct.finish()
    }
}
