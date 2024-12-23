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

/// `Any` contains an arbitrary serialized protocol buffer message along with a
/// URL that describes the type of the serialized message.
///
/// Protobuf library provides support to pack/unpack Any values in the form
/// of utility functions or additional generated methods of the Any type.
///
///
/// # JSON
///
/// The JSON representation of an `Any` value uses the regular
/// representation of the deserialized, embedded message, with an
/// additional field `@type` which contains the type URL. Example:
///
/// ```norust
///     package google.profile;
///     message Person {
///       string first_name = 1;
///       string last_name = 2;
///     }
///
///     {
///       "@type": "type.googleapis.com/google.profile.Person",
///       "firstName": <string>,
///       "lastName": <string>
///     }
/// ```
///
/// If the embedded message type is well-known and has a custom JSON
/// representation, that representation will be embedded adding a field
/// `value` which holds the custom JSON in addition to the `@type`
/// field. Example (for message [google.protobuf.Duration][]):
///
/// ```norust
///     {
///       "@type": "type.googleapis.com/google.protobuf.Duration",
///       "value": "1.212s"
///     }
/// ```
#[derive(Clone, Debug, Default, PartialEq)]
#[non_exhaustive]
pub struct Any(serde_json::Map<String, serde_json::Value>);

/// Indicates a problem trying to use an [Any].
#[derive(thiserror::Error, Debug)]
pub enum AnyError {
    /// Problem serializing an object into an [Any].
    #[error("cannot serialize object into an Any, source={0:?}")]
    SerializationError(#[source] Box<dyn std::error::Error>),

    /// Problem deserializing an object from an [Any].
    #[error("cannot deserialize from an Any, source={0:?}")]
    DeserializationError(#[source] Box<dyn std::error::Error>),

    /// Mismatched type, the [Any] does not contain the desired type.
    #[error("expected type mismatch in Any deserialization type={0}")]
    TypeMismatchError(String),
}

type Error = AnyError;

impl Any {
    /// Creates a new [Any] from any object that supports serialization to JSON.
    // TODO(#98) - each message should have a type value
    pub fn from<T>(message: &T) -> Result<Self, Error>
    where
        T: serde::ser::Serialize,
    {
        use serde_json::Value;

        let value =
            serde_json::to_value(message).map_err(|e| Error::SerializationError(e.into()))?;
        let value = match value {
            Value::Object(mut map) => {
                map.insert("@type".to_string(), serde_json::json!(""));
                Ok(map)
            }
            Value::String(s) => {
                // Only a handful of well-known messages are serialized into
                // something other than a object. In all cases, they are
                // serialized using a `value` field.
                // TODO(#98) - each message should have a type value
                let mut map = serde_json::Map::new();
                map.insert(
                    "@type".to_string(),
                    serde_json::Value::String("type.googleapis.com/google.protobuf.".into()),
                );
                map.insert("value".to_string(), serde_json::Value::String(s));
                Ok(map)
            }
            _ => Err(Error::SerializationError(Box::from(
                "unexpected JSON type, only Object and String are supported",
            ))),
        }?;
        Ok(Any(value))
    }

    fn map_de_err(e: Box<dyn std::error::Error>) -> Error {
        Error::DeserializationError(e)
    }

    fn map_de_str(s: String) -> Error {
        Error::DeserializationError(Box::from(s))
    }

    /// Extracts (if possible) a `T` value from the [Any].
    pub fn try_into_message<T>(&self) -> Result<T, Error>
    where
        T: serde::de::DeserializeOwned,
    {
        let map = &self.0;
        let r#type = map
            .get("@type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "@type field is missing or is not a string".to_string())
            .map_err(Self::map_de_str)?;
        if r#type.starts_with("type.googleapis.com/google.protobuf.") {
            return map
                .get("value")
                .map(|v| serde_json::from_value::<T>(v.clone()))
                .ok_or_else(|| Self::map_de_str("value field is missing".to_string()))?
                .map_err(|e| Self::map_de_err(e.into()));
        }
        serde_json::from_value::<T>(serde_json::Value::Object(map.clone()))
            .map_err(|e| Self::map_de_err(e.into()))
    }
}

/// Implement [`serde`](::serde) serialization for [Any].
impl serde::ser::Serialize for Any {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        self.0.serialize(serializer)
    }
}

/// Implement [`serde`](::serde) deserialization for [Any].
impl<'de> serde::de::Deserialize<'de> for Any {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = serde_json::Map::<String, serde_json::Value>::deserialize(deserializer)?;
        Ok(Any(value))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::duration::*;
    use serde_json::json;
    type Result = std::result::Result<(), Box<dyn std::error::Error>>;

    #[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]
    #[serde(rename_all = "camelCase")]
    #[non_exhaustive]
    pub struct Stored {
        #[serde(skip_serializing_if = "String::is_empty")]
        pub parent: String,
        #[serde(skip_serializing_if = "String::is_empty")]
        pub id: String,
    }

    #[test]
    fn serialize_duration() -> Result {
        let d = Duration::clamp(60, 0);
        let any = Any::from(&d)?;
        let got = serde_json::to_value(any)?;
        let want = json!({"@type": "type.googleapis.com/google.protobuf.", "value": "60s"});
        assert_eq!(got, want);
        Ok(())
    }

    #[test]
    fn deserialize_duration() -> Result {
        let input = json!({"@type": "type.googleapis.com/google.protobuf.", "value": "60s"});
        let any = Any(input.as_object().unwrap().clone());
        let d = any.try_into_message::<Duration>()?;
        assert_eq!(d, Duration::clamp(60, 0));
        Ok(())
    }

    #[test]
    fn serialize_generic() -> Result {
        let d = Stored {
            parent: "parent".to_string(),
            id: "id".to_string(),
        };
        let any = Any::from(&d)?;
        let got = serde_json::to_value(any)?;
        let want = json!({"@type": "", "parent": "parent", "id": "id"});
        assert_eq!(got, want);
        Ok(())
    }

    #[test]
    fn deserialize_generic() -> Result {
        let input = json!({"@type": "", "parent": "parent", "id": "id"});
        let any = Any(input.as_object().unwrap().clone());
        let d = any.try_into_message::<Stored>()?;
        assert_eq!(
            d,
            Stored {
                parent: "parent".to_string(),
                id: "id".to_string()
            }
        );
        Ok(())
    }

    #[test]
    fn serialize_error() -> Result {
        use std::collections::BTreeMap;
        let mut input = BTreeMap::new();
        input.insert(vec![2, 3], "unused");
        let got = Any::from(&input);
        assert!(got.is_err());
        match got.as_ref().err().unwrap() {
            Error::SerializationError(_) => assert!(true),
            _ => assert!(false, "unexpected error {got:?}"),
        };

        let input = vec![2, 3, 4];
        let got = Any::from(&input);
        assert!(got.is_err());
        match got.as_ref().err().unwrap() {
            Error::SerializationError(_) => assert!(true),
            _ => assert!(false, "unexpected error {got:?}"),
        };
        Ok(())
    }

    #[test]
    fn deserialize_error() -> Result {
        let input = json!({"@type-is-missing": ""});
        let any = serde_json::from_value::<Any>(input)?;
        let got = any.try_into_message::<Stored>();
        assert!(got.is_err());

        let input = json!({"@type": [1, 2, 3]});
        let any = serde_json::from_value::<Any>(input)?;
        let got = any.try_into_message::<Stored>();
        assert!(got.is_err());

        let input = json!({"@type": "type.googleapis.com/google.protobuf.Duration", "value-is-missing": "1.2s"});
        let any = serde_json::from_value::<Any>(input)?;
        let got = any.try_into_message::<Stored>();
        assert!(got.is_err());

        let input =
            json!({"@type": "type.googleapis.com/google.protobuf.Duration", "value": ["1.2s"]});
        let any = serde_json::from_value::<Any>(input)?;
        let got = any.try_into_message::<Stored>();
        assert!(got.is_err());

        Ok(())
    }
}
