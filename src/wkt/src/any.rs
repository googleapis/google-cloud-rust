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

use crate::message::MessageSerializer;

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
///
/// # Example
/// ```rust
/// # use google_cloud_wkt::{Any, AnyError, Duration, Timestamp};
/// use serde_json::json;
/// let any = serde_json::from_value::<Any>(json!({
///     "@type": "type.googleapis.com/google.protobuf.Duration",
///     "value": "123.5" // missing `s` suffix
/// }))?;
/// let extracted = any.try_into_message::<Duration>();
/// assert!(matches!(extracted, Err(AnyError::Deserialization(_))));
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// # Example
/// ```rust
/// # use google_cloud_wkt::{Any, AnyError, Duration, Timestamp};
/// let duration = Duration::clamp(60, 0);
/// let any = Any::try_from(&duration)?;
/// let extracted = any.try_into_message::<Timestamp>();
/// assert!(matches!(extracted, Err(AnyError::TypeMismatch(_))));
/// # Ok::<(), AnyError>(())
/// ```
#[derive(thiserror::Error, Debug)]
pub enum AnyError {
    /// Problem serializing an object into an [Any].
    #[error("cannot serialize object into an Any, source={0:?}")]
    Serialization(#[source] BoxedError),

    /// Problem deserializing an object from an [Any].
    #[error("cannot deserialize from an Any, source={0:?}")]
    Deserialization(#[source] BoxedError),

    /// Mismatched type, the [Any] does not contain the desired type.
    #[error("expected type mismatch in Any deserialization type={0}")]
    TypeMismatch(String),
}

impl AnyError {
    pub(crate) fn ser<T: Into<BoxedError>>(v: T) -> Self {
        Self::Serialization(v.into())
    }

    pub(crate) fn deser<T: Into<BoxedError>>(v: T) -> Self {
        Self::Deserialization(v.into())
    }

    pub(crate) fn mismatch<T: Into<String>>(v: T) -> Self {
        Self::TypeMismatch(v.into())
    }
}

type BoxedError = Box<dyn std::error::Error + Send + Sync>;
type Error = AnyError;

impl Any {
    /// Returns the name of the contained type.
    ///
    /// An any may contain any message type. The name of the message is a URL,
    /// usually with the `https://` scheme elided. All types in Google Cloud
    /// APIs are of the form
    /// `https://type.googleapis.com/${fully-qualified-name}`.
    ///
    /// Note that this is not an available URL where you can download data (such
    /// as the message schema) from.
    pub fn type_url(&self) -> Option<&str> {
        self.0.get("@type").and_then(serde_json::Value::as_str)
    }

    /// Creates a new [Any] from any [Message][crate::message::Message] that
    /// also supports serialization to JSON.
    pub fn try_from<T>(message: &T) -> Result<Self, Error>
    where
        T: crate::message::Message + serde::ser::Serialize + serde::de::DeserializeOwned,
    {
        let serializer = T::serializer();
        let value = serializer.serialize_to_map(message)?;
        Ok(Any(value))
    }

    /// Extracts (if possible) a `T` value from the [Any].
    pub fn try_into_message<T>(&self) -> Result<T, Error>
    where
        T: crate::message::Message + serde::ser::Serialize + serde::de::DeserializeOwned,
    {
        let map = &self.0;
        let r#type = map
            .get("@type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "@type field is missing or is not a string".to_string())
            .map_err(Error::deser)?;
        Self::check_typename(r#type, T::typename())?;

        let serializer = T::serializer();
        serializer.deserialize_from_map(map)
    }

    fn check_typename(got: &str, want: &str) -> Result<(), Error> {
        if got == want {
            return Ok(());
        }
        Err(Error::mismatch(format!(
            "mismatched typenames extracting from Any, the any has {got}, the target type is {want}"
        )))
    }
}

impl crate::message::Message for Any {
    fn typename() -> &'static str {
        "type.googleapis.com/google.protobuf.Any"
    }

    #[allow(private_interfaces)]
    fn serializer() -> impl crate::message::MessageSerializer<Self> {
        crate::message::ValueSerializer::<Self>::new()
    }
}

/// Implement [`serde`](::serde) serialization for [Any].
impl serde::ser::Serialize for Any {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
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
    fn serialize_any() -> Result {
        let d = Duration::clamp(60, 0);
        let any = Any::try_from(&d)?;
        let any = Any::try_from(&any)?;
        assert_eq!(
            any.type_url(),
            Some("type.googleapis.com/google.protobuf.Any")
        );
        let got = serde_json::to_value(any)?;
        let want = json!({
            "@type": "type.googleapis.com/google.protobuf.Any",
            "value": {
                "@type": "type.googleapis.com/google.protobuf.Duration",
                "value": "60s"
            }
        });
        assert_eq!(got, want);
        Ok(())
    }

    #[test]
    fn deserialize_any() -> Result {
        let input = json!({
            "@type": "type.googleapis.com/google.protobuf.Any",
            "value": {
                "@type": "type.googleapis.com/google.protobuf.Duration",
                "value": "60s"
            }
        });
        let any = Any(input.as_object().unwrap().clone());
        assert_eq!(
            any.type_url(),
            Some("type.googleapis.com/google.protobuf.Any")
        );
        let any = any.try_into_message::<Any>()?;
        assert_eq!(
            any.type_url(),
            Some("type.googleapis.com/google.protobuf.Duration")
        );
        let d = any.try_into_message::<Duration>()?;
        assert_eq!(d, Duration::clamp(60, 0));
        Ok(())
    }

    #[test]
    fn serialize_duration() -> Result {
        let d = Duration::clamp(60, 0);
        let any = Any::try_from(&d)?;
        assert_eq!(
            any.type_url(),
            Some("type.googleapis.com/google.protobuf.Duration")
        );
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
        assert_eq!(
            any.type_url(),
            Some("type.googleapis.com/google.protobuf.Duration")
        );
        let d = any.try_into_message::<Duration>()?;
        assert_eq!(d, Duration::clamp(60, 0));
        Ok(())
    }

    #[test]
    fn serialize_empty() -> Result {
        let empty = Empty::default();
        let any = Any::try_from(&empty)?;
        assert_eq!(
            any.type_url(),
            Some("type.googleapis.com/google.protobuf.Empty")
        );
        let got = serde_json::to_value(any)?;
        let want = json!({"@type": "type.googleapis.com/google.protobuf.Empty"});
        assert_eq!(got, want);
        Ok(())
    }

    #[test]
    fn deserialize_empty() -> Result {
        let input = json!({"@type": "type.googleapis.com/google.protobuf.Empty"});
        let any = Any(input.as_object().unwrap().clone());
        assert_eq!(
            any.type_url(),
            Some("type.googleapis.com/google.protobuf.Empty")
        );
        let empty = any.try_into_message::<Empty>()?;
        assert_eq!(empty, Empty::default());
        Ok(())
    }

    #[test]
    fn serialize_field_mask() -> Result {
        let d = FieldMask::default().set_paths(["a", "b"].map(str::to_string).to_vec());
        let any = Any::try_from(&d)?;
        assert_eq!(
            any.type_url(),
            Some("type.googleapis.com/google.protobuf.FieldMask")
        );
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
        assert_eq!(
            any.type_url(),
            Some("type.googleapis.com/google.protobuf.FieldMask")
        );
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
        assert_eq!(
            any.type_url(),
            Some("type.googleapis.com/google.protobuf.Timestamp")
        );
        let got = serde_json::to_value(any)?;
        let want = json!({"@type": "type.googleapis.com/google.protobuf.Timestamp", "value": "1970-01-01T00:02:03Z"});
        assert_eq!(got, want);
        Ok(())
    }

    #[test]
    fn deserialize_timestamp() -> Result {
        let input = json!({"@type": "type.googleapis.com/google.protobuf.Timestamp", "value": "1970-01-01T00:02:03Z"});
        let any = Any(input.as_object().unwrap().clone());
        assert_eq!(
            any.type_url(),
            Some("type.googleapis.com/google.protobuf.Timestamp")
        );
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
        assert_eq!(any.type_url(), Some("type.googleapis.com/wkt.test.Stored"));
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
        assert_eq!(any.type_url(), Some("type.googleapis.com/wkt.test.Stored"));
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

    #[derive(Default, serde::Serialize, serde::Deserialize)]
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
