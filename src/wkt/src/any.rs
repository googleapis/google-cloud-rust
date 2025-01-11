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
    SerializationError(#[source] BoxedError),

    /// Problem deserializing an object from an [Any].
    #[error("cannot deserialize from an Any, source={0:?}")]
    DeserializationError(#[source] BoxedError),

    /// Mismatched type, the [Any] does not contain the desired type.
    #[error("expected type mismatch in Any deserialization type={0}")]
    TypeMismatchError(String),
}

impl AnyError {
    pub(crate) fn ser<T: Into<BoxedError>>(v: T) -> Self {
        Self::SerializationError(v.into())
    }

    pub(crate) fn deser<T: Into<BoxedError>>(v: T) -> Self {
        Self::DeserializationError(v.into())
    }
}

type BoxedError = Box<dyn std::error::Error + Send + Sync>;
type Error = AnyError;

impl Any {
    /// Creates a new [Any] from any [Message][crate::message::Message] that
    /// also supports serialization to JSON.
    pub fn try_from<T>(message: &T) -> Result<Self, Error>
    where
        T: serde::ser::Serialize + crate::message::Message,
    {
        use serde_json::{Map, Value};

        let value = serde_json::to_value(message).map_err(Error::ser)?;
        let value = match value {
            Value::Object(mut map) => {
                map.insert(
                    "@type".to_string(),
                    Value::String(T::typename().to_string()),
                );
                map
            }
            Value::String(s) => {
                // Only a few well-known messages are serialized into something
                // other than a object. In all cases, they are serialized using
                // a small JSON object, with the string in the `value` field.
                let map: Map<String, serde_json::Value> =
                    [("@type", T::typename().to_string()), ("value", s)]
                        .into_iter()
                        .map(|(k, v)| (k.to_string(), Value::String(v)))
                        .collect();
                map
            }
            _ => {
                return Err(Self::unexpected_json_type());
            }
        };
        Ok(Any(value))
    }

    /// Extracts (if possible) a `T` value from the [Any].
    pub fn try_into_message<T>(&self) -> Result<T, Error>
    where
        T: serde::de::DeserializeOwned + crate::message::Message,
    {
        let map = &self.0;
        let r#type = map
            .get("@type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "@type field is missing or is not a string".to_string())
            .map_err(Error::deser)?;
        Self::check_typename(r#type, T::typename())?;
        if r#type.starts_with("type.googleapis.com/google.protobuf.")
            && r#type != "type.googleapis.com/google.protobuf.Empty"
            && r#type != "type.googleapis.com/google.protobuf.FieldMask"
        {
            return map
                .get("value")
                .map(|v| serde_json::from_value::<T>(v.clone()))
                .ok_or_else(Self::missing_value_field)?
                .map_err(Error::deser);
        }
        serde_json::from_value::<T>(serde_json::Value::Object(map.clone())).map_err(Error::deser)
    }

    fn missing_value_field() -> Error {
        Error::deser("value field is missing")
    }

    fn unexpected_json_type() -> Error {
        Error::ser("unexpected JSON type, only Object and String are supported")
    }

    fn check_typename(got: &str, want: &str) -> Result<(), Error> {
        if got == want {
            return Ok(());
        }
        Err(Error::deser(format!("mismatched typenames extracting from Any, the any has {got}, the target type is {want}")))
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
    use crate::empty::Empty;
    use crate::field_mask::*;
    use crate::timestamp::*;
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

    impl crate::message::Message for Stored {
        fn typename() -> &'static str {
            "type.googleapis.com/wkt.test.Stored"
        }
    }

    #[test]
    fn serialize_duration() -> Result {
        let d = Duration::clamp(60, 0);
        let any = Any::try_from(&d)?;
        let got = serde_json::to_value(any)?;
        let want = json!({"@type": "type.googleapis.com/google.protobuf.Duration", "value": "60s"});
        assert_eq!(got, want);
        Ok(())
    }

    #[test]
    fn deserialize_duration() -> Result {
        let input =
            json!({"@type": "type.googleapis.com/google.protobuf.Duration", "value": "60s"});
        let any = Any(input.as_object().unwrap().clone());
        let d = any.try_into_message::<Duration>()?;
        assert_eq!(d, Duration::clamp(60, 0));
        Ok(())
    }

    #[test]
    fn serialize_empty() -> Result {
        let empty = Empty::default();
        let any = Any::try_from(&empty)?;
        let got = serde_json::to_value(any)?;
        let want = json!({"@type": "type.googleapis.com/google.protobuf.Empty"});
        assert_eq!(got, want);
        Ok(())
    }

    #[test]
    fn deserialize_empty() -> Result {
        let input = json!({"@type": "type.googleapis.com/google.protobuf.Empty"});
        let any = Any(input.as_object().unwrap().clone());
        let empty = any.try_into_message::<Empty>()?;
        assert_eq!(empty, Empty::default());
        Ok(())
    }

    #[test]
    fn serialize_field_mask() -> Result {
        let d = FieldMask::default().set_paths(["a", "b"].map(str::to_string).to_vec());
        let any = Any::try_from(&d)?;
        let got = serde_json::to_value(any)?;
        let want =
            json!({"@type": "type.googleapis.com/google.protobuf.FieldMask", "paths": "a,b"});
        assert_eq!(got, want);
        Ok(())
    }

    #[test]
    fn deserialize_field_mask() -> Result {
        let input =
            json!({"@type": "type.googleapis.com/google.protobuf.FieldMask", "paths": "a,b"});
        let any = Any(input.as_object().unwrap().clone());
        let d = any.try_into_message::<FieldMask>()?;
        assert_eq!(
            d,
            FieldMask::default().set_paths(["a", "b"].map(str::to_string).to_vec())
        );
        Ok(())
    }

    #[test]
    fn serialize_timestamp() -> Result {
        let d = Timestamp::clamp(123, 0);
        let any = Any::try_from(&d)?;
        let got = serde_json::to_value(any)?;
        let want = json!({"@type": "type.googleapis.com/google.protobuf.Timestamp", "value": "1970-01-01T00:02:03Z"});
        assert_eq!(got, want);
        Ok(())
    }

    #[test]
    fn deserialize_timestamp() -> Result {
        let input = json!({"@type": "type.googleapis.com/google.protobuf.Timestamp", "value": "1970-01-01T00:02:03Z"});
        let any = Any(input.as_object().unwrap().clone());
        let d = any.try_into_message::<Timestamp>()?;
        assert_eq!(d, Timestamp::clamp(123, 0));
        Ok(())
    }

    #[test]
    fn serialize_generic() -> Result {
        let d = Stored {
            parent: "parent".to_string(),
            id: "id".to_string(),
        };
        let any = Any::try_from(&d)?;
        let got = serde_json::to_value(any)?;
        let want =
            json!({"@type": "type.googleapis.com/wkt.test.Stored", "parent": "parent", "id": "id"});
        assert_eq!(got, want);
        Ok(())
    }

    #[test]
    fn deserialize_generic() -> Result {
        let input =
            json!({"@type": "type.googleapis.com/wkt.test.Stored", "parent": "parent", "id": "id"});
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

    #[derive(Default, serde::Serialize)]
    struct DetectBadMessages(serde_json::Value);
    impl crate::message::Message for DetectBadMessages {
        fn typename() -> &'static str {
            "not used"
        }
    }

    #[test]
    fn try_from_error() -> Result {
        let input = DetectBadMessages(json!([2, 3]));
        let got = Any::try_from(&input);
        assert!(got.is_err(), "{got:?}");

        Ok(())
    }

    #[test]
    fn deserialize_missing_type_field() -> Result {
        let input = json!({"@type-is-missing": ""});
        let any = serde_json::from_value::<Any>(input)?;
        let got = any.try_into_message::<Stored>();
        assert!(got.is_err());
        Ok(())
    }

    #[test]
    fn deserialize_invalid_type_field() -> Result {
        let input = json!({"@type": [1, 2, 3]});
        let any = serde_json::from_value::<Any>(input)?;
        let got = any.try_into_message::<Stored>();
        assert!(got.is_err());
        Ok(())
    }

    #[test]
    fn deserialize_missing_value_field() -> Result {
        let input = json!({"@type": "type.googleapis.com/google.protobuf.Duration", "value-is-missing": "1.2s"});
        let any = serde_json::from_value::<Any>(input)?;
        let got = any.try_into_message::<Duration>();
        assert!(got.is_err());
        Ok(())
    }

    #[test]
    fn deserialize_invalid_value_field() -> Result {
        let input =
            json!({"@type": "type.googleapis.com/google.protobuf.Duration", "value": ["1.2s"]});
        let any = serde_json::from_value::<Any>(input)?;
        let got = any.try_into_message::<Duration>();
        assert!(got.is_err());
        Ok(())
    }

    #[test]
    fn deserialize_type_mismatch() -> Result {
        let input =
            json!({"@type": "type.googleapis.com/google.protobuf.Duration", "value": "1.2s"});
        let any = serde_json::from_value::<Any>(input)?;
        let got = any.try_into_message::<Timestamp>();
        assert!(got.is_err());
        let error = got.err().unwrap();
        assert!(
            format!("{error}").contains("type.googleapis.com/google.protobuf.Duration"),
            "{error}"
        );
        assert!(
            format!("{error}").contains("type.googleapis.com/google.protobuf.Timestamp"),
            "{error}"
        );
        Ok(())
    }
}
