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
pub trait Message: serde::ser::Serialize + serde::de::DeserializeOwned {
    /// The typename of this message.
    fn typename() -> &'static str;

    /// Store the value into a JSON object.
    fn to_map(&self) -> Result<Map, Error> {
        to_json_object(self)
    }

    /// Extract the value from a JSON object.
    fn from_map(map: &Map) -> Result<Self, Error> {
        serde_json::from_value::<Self>(serde_json::Value::Object(map.clone())).map_err(Error::deser)
    }
}

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

pub(crate) fn to_json_string<T>(message: &T) -> Result<Map, Error>
where
    T: Message,
{
    use serde_json::Value;
    let value = serde_json::to_value(message).map_err(Error::ser)?;
    match value {
        Value::String(s) => {
            // Only a few well-known messages are serialized into something
            // other than a object. In all cases, they are serialized using
            // a small JSON object, with the string in the `value` field.
            let map: Map = [("@type", T::typename().to_string()), ("value", s)]
                .into_iter()
                .map(|(k, v)| (k.to_string(), Value::String(v)))
                .collect();
            Ok(map)
        }
        _ => Err(unexpected_json_type()),
    }
}

pub(crate) fn from_value<T>(map: &Map) -> Result<T, Error>
where
    T: serde::de::DeserializeOwned,
{
     map
        .get("value")
        .map(|v| serde_json::from_value::<T>(v.clone()))
        .ok_or_else(missing_value_field)?
        .map_err(Error::deser)
}

fn missing_value_field() -> Error {
    Error::deser("value field is missing")
}

fn unexpected_json_type() -> Error {
    Error::ser("unexpected JSON type, only Object and String are supported")
}
