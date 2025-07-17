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

//! Define traits required of all messages.

pub(crate) type Map = serde_json::Map<String, serde_json::Value>;
use crate::AnyError as Error;

/// A trait that must be implemented by all messages.
///
/// Messages sent to and received from Google Cloud services may be wrapped in
/// [Any][crate::any::Any]. `Any` uses a `@type` field to encode the type
/// name and then validates extraction and insertion against this type.
pub trait Message: serde::ser::Serialize + serde::de::DeserializeOwned {
    /// The typename of this message.
    fn typename() -> &'static str;

    /// Returns the serializer for this message type.
    #[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
    fn serializer() -> impl MessageSerializer<Self> {
        DefaultSerializer::<Self>::new()
    }
}

pub(crate) mod sealed {
    pub trait MessageSerializer {}
}

#[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
/// Internal API for message serialization.
/// This is not intended for direct use by consumers of this crate.
pub trait MessageSerializer<T>: sealed::MessageSerializer {
    /// Store the value into a JSON object.
    fn serialize_to_map(&self, message: &T) -> Result<Map, Error>;

    /// Extract the value from a JSON object.
    fn deserialize_from_map(&self, map: &Map) -> Result<T, Error>;
}

// Default serializer that most types can use.
pub(crate) struct DefaultSerializer<T> {
    _phantom: std::marker::PhantomData<T>,
}

impl<T> DefaultSerializer<T> {
    pub fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
}

#[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
impl<T> sealed::MessageSerializer for DefaultSerializer<T> {}

impl<T> MessageSerializer<T> for DefaultSerializer<T>
where
    T: Message,
{
    fn serialize_to_map(&self, message: &T) -> Result<Map, Error> {
        to_json_object(message)
    }

    fn deserialize_from_map(&self, map: &Map) -> Result<T, Error> {
        from_object(map)
    }
}

// Serializes the type `T` into the `value` field.
pub(crate) struct ValueSerializer<T> {
    _phantom: std::marker::PhantomData<T>,
}

impl<T> ValueSerializer<T> {
    pub fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
}

#[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
impl<T> sealed::MessageSerializer for ValueSerializer<T> {}

impl<T> MessageSerializer<T> for ValueSerializer<T>
where
    T: Message,
{
    fn serialize_to_map(&self, message: &T) -> Result<Map, Error> {
        to_json_other(message)
    }

    fn deserialize_from_map(&self, map: &Map) -> Result<T, Error> {
        from_other(map)
    }
}

/// Write the serialization of `T` flatly into a map.
///
/// We use this for types that do not have special encodings, as defined in:
/// https://protobuf.dev/programming-guides/json/
///
/// That typically means that `T` is an object.
pub(crate) fn to_json_object<T>(message: &T) -> Result<Map, Error>
where
    T: Message,
{
    use serde_json::Value;

    let value = serde_json::to_value(message).map_err(Error::ser)?;
    match value {
        Value::Object(mut map) => {
            map.insert(
                "@type".to_string(),
                Value::String(T::typename().to_string()),
            );
            Ok(map)
        }
        _ => Err(unexpected_json_type()),
    }
}

/// Write the serialization of `T` into the `value` field of a map.
///
/// We use this for types that have special encodings, as defined in:
/// https://protobuf.dev/programming-guides/json/
///
/// Typically this means that the JSON serialization of `T` is not an object. It
/// is also used for `Any`, as flatly serializing an `Any` would have
/// conflicting `@type` fields.
pub(crate) fn to_json_other<T>(message: &T) -> Result<Map, Error>
where
    T: Message,
{
    let value = serde_json::to_value(message).map_err(Error::ser)?;
    let mut map = crate::message::Map::new();
    map.insert("@type".to_string(), T::typename().into());
    map.insert("value".to_string(), value);
    Ok(map)
}

/// The analog of `to_json_object()`
pub(crate) fn from_object<T>(map: &Map) -> Result<T, Error>
where
    T: Message,
{
    let map = map
        .iter()
        .filter_map(|(k, v)| {
            if k == "@type" {
                return None;
            }
            Some((k.clone(), v.clone()))
        })
        .collect();
    serde_json::from_value::<T>(serde_json::Value::Object(map)).map_err(Error::deser)
}

/// The analog of `to_json_other()`
pub(crate) fn from_other<T>(map: &Map) -> Result<T, Error>
where
    T: Message,
{
    map.get("value")
        .map(|v| serde_json::from_value::<T>(v.clone()))
        .ok_or_else(missing_value_field)?
        .map_err(Error::deser)
}

pub(crate) fn missing_value_field() -> Error {
    Error::deser("value field is missing")
}

fn unexpected_json_type() -> Error {
    Error::ser("unexpected JSON type, only Object and String are supported")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    struct TestMessage {
        #[serde(flatten)]
        _unknown_fields: serde_json::Map<String, serde_json::Value>,
    }

    impl Message for TestMessage {
        fn typename() -> &'static str {
            "TestMessage"
        }
    }

    #[test]
    fn drop_type_field() {
        let input = json!({
            "@type": "TestMessage",
            "a": 1,
            "b": 2,
        });
        let map = input.as_object().cloned().unwrap();

        let serializer = TestMessage::serializer();
        let test = serializer.deserialize_from_map(&map).unwrap();

        assert!(test._unknown_fields.get("@type").is_none(), "{test:?}");
    }
}
