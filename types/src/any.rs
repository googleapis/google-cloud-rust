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
pub struct Any(serde_json::Value);

#[derive(Debug)]
pub enum Error {
    SerializationError(Box<dyn std::error::Error>),
    DeserializationError(Box<dyn std::error::Error>),
    TypeMismatchError(),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        match self {
            Error::SerializationError(e) => write!(f, "serialization error {:?}", e),
            Error::DeserializationError(e) => write!(f, "deserialization error {:?}", e),
            Self::TypeMismatchError() => write!(f, "type mismatch"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::SerializationError(e) => Some(e.as_ref()),
            Error::DeserializationError(e) => Some(e.as_ref()),
            Self::TypeMismatchError() => None,
        }
    }
}

impl Any {
    // TODO(#98) - each message should have a type value
    pub fn from<T>(message: &T) -> Result<Self, Error>
    where
        T: serde::ser::Serialize,
    {
        use serde_json::{json, Value};

        let value =
            serde_json::to_value(message).map_err(|e| Error::SerializationError(e.into()))?;
        let value = match value {
            Value::Object(mut map) => {
                map.insert("@type".to_string(), serde_json::json!(""));
                Ok(Value::Object(map))
            }
            Value::String(s) => {
                // Only a handful of well-known messages are serialized into
                // something other than a object. In all cases, they are
                // serialized using a `value` field.
                // TODO(#98) - each message should have a type value
                Ok(json!({"@type": "type.googleapis.com/google.protobuf.", "value": s}))
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

    pub fn try_into_message<T>(&self) -> Result<T, Error>
    where
        T: serde::de::DeserializeOwned,
    {
        let value = &self.0;
        let object = value
            .as_object()
            .ok_or_else(|| "expected Object value inside Any".to_string())
            .map_err(Self::map_de_str)?;
        let r#type = object["@type"]
            .as_str()
            .ok_or_else(|| "@type field is missing or is not a string".to_string())
            .map_err(Self::map_de_str)?;
        if r#type.starts_with("type.googleapis.com/google.protobuf.") {
            let value = &object["value"];
            let t = serde_json::from_value::<T>(value.clone())
                .map_err(|e| Self::map_de_err(e.into()))?;
            return Ok(t);
        }
        let t =
            serde_json::from_value::<T>(value.clone()).map_err(|e| Self::map_de_err(e.into()))?;
        Ok(t)
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
        let value = serde_json::Value::deserialize(deserializer)?;
        Ok(Any(value))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::duration::*;
    use serde_json::json;
    type Result = std::result::Result<(), Box<dyn std::error::Error>>;

    #[serde_with::skip_serializing_none]
    #[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]
    #[serde(rename_all = "camelCase")]
    #[non_exhaustive]
    pub struct Stored {
        pub parent: String,
        pub id: String,
    }

    #[test]
    fn serialize_duration() -> Result {
        let d = Duration::from_seconds(60);
        let any = Any::from(&d)?;
        let got = serde_json::to_value(any)?;
        let want = json!({"@type": "type.googleapis.com/google.protobuf.", "value": "60s"});
        assert_eq!(got, want);
        Ok(())
    }

    #[test]
    fn deserialize_duration() -> Result {
        let input = json!({"@type": "type.googleapis.com/google.protobuf.", "value": "60s"});
        let any = Any(input);
        let d = any.try_into_message::<Duration>()?;
        assert_eq!(d, Duration::from_seconds(60));
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
        let any = Any(input);
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
}
