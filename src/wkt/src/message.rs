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
/// [Any][crate::any::Any]. `Any` uses a `@type` field to encoding the type
/// name and then validates extraction and insertion against this type.
pub trait Message {
    /// The typename of this message.
    fn typename() -> &'static str;

    #[doc(hidden)]
    /// Store the value into a JSON object.
    fn to_map(&self) -> Result<Map, Error>
    where
        Self: serde::ser::Serialize + Sized,
    {
        to_json_object(self)
    }

    #[doc(hidden)]
    /// Extract the value from a JSON object.
    fn from_map(map: &Map) -> Result<Self, Error>
    where
        Self: serde::de::DeserializeOwned,
    {
        from_object(map)
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
    T: Message + serde::ser::Serialize,
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
    T: Message + serde::ser::Serialize,
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
    T: serde::de::DeserializeOwned,
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
    T: serde::de::DeserializeOwned,
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
mod test {
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
        let test = TestMessage::from_map(&map).unwrap();
        assert!(test._unknown_fields.get("@type").is_none(), "{test:?}");
    }
}
