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

/// `FieldMask` represents a set of symbolic field paths, for example:
///
/// ```norust
///     paths: "f.a"
///     paths: "f.b.d"
/// ```
///
/// Here `f` represents a field in some root message, `a` and `b`
/// fields in the message found in `f`, and `d` a field found in the
/// message in `f.b`.
///
/// Field masks are used to specify a subset of fields that should be
/// returned by a get operation or modified by an update operation.
/// Field masks also have a custom JSON encoding (see below).
///
/// # Field Masks in Projections
///
/// When used in the context of a projection, a response message or
/// sub-message is filtered by the API to only contain those fields as
/// specified in the mask. For example, if the mask in the previous
/// example is applied to a response message as follows:
///
/// ```norust
///     f {
///       a : 22
///       b {
///         d : 1
///         x : 2
///       }
///       y : 13
///     }
///     z: 8
/// ```
///
/// The result will not contain specific values for fields x,y and z
/// (their value will be set to the default, and omitted in proto text
/// output):
///
///
/// ```norust
///     f {
///       a : 22
///       b {
///         d : 1
///       }
///     }
/// ```
///
/// A repeated field is not allowed except at the last position of a
/// paths string.
///
/// If a FieldMask object is not present in a get operation, the
/// operation applies to all fields (as if a FieldMask of all fields
/// had been specified).
///
/// Note that a field mask does not necessarily apply to the
/// top-level response message. In case of a REST get operation, the
/// field mask applies directly to the response, but in case of a REST
/// list operation, the mask instead applies to each individual message
/// in the returned resource list. In case of a REST custom method,
/// other definitions may be used. Where the mask applies will be
/// clearly documented together with its declaration in the API.  In
/// any case, the effect on the returned resource/resources is required
/// behavior for APIs.
///
/// # Field Masks in Update Operations
///
/// A field mask in update operations specifies which fields of the
/// targeted resource are going to be updated. The API is required
/// to only change the values of the fields as specified in the mask
/// and leave the others untouched. If a resource is passed in to
/// describe the updated values, the API ignores the values of all
/// fields not covered by the mask.
///
/// If a repeated field is specified for an update operation, new values will
/// be appended to the existing repeated field in the target resource. Note that
/// a repeated field is only allowed in the last position of a `paths` string.
///
/// If a sub-message is specified in the last position of the field mask for an
/// update operation, then new value will be merged into the existing sub-message
/// in the target resource.
///
/// For example, given the target message:
///
/// ```norust
///     f {
///       b {
///         d: 1
///         x: 2
///       }
///       c: [1]
///     }
/// ```
///
/// And an update message:
///
/// ```norust
///     f {
///       b {
///         d: 10
///       }
///       c: [2]
///     }
/// ```
///
/// then if the field mask is:
///
/// ```norust
///  paths: ["f.b", "f.c"]
/// ```
///
/// then the result will be:
///
/// ```norust
///     f {
///       b {
///         d: 10
///         x: 2
///       }
///       c: [1, 2]
///     }
/// ```
///
/// An implementation may provide options to override this default behavior for
/// repeated and message fields.
///
/// In order to reset a field's value to the default, the field must
/// be in the mask and set to the default value in the provided resource.
/// Hence, in order to reset all fields of a resource, provide a default
/// instance of the resource and set all fields in the mask, or do
/// not provide a mask as described below.
///
/// If a field mask is not present on update, the operation applies to
/// all fields (as if a field mask of all fields has been specified).
/// Note that in the presence of schema evolution, this may mean that
/// fields the client does not know and has therefore not filled into
/// the request will be reset to their default. If this is unwanted
/// behavior, a specific service may require a client to always specify
/// a field mask, producing an error if not.
///
/// As with get operations, the location of the resource which
/// describes the updated values in the request message depends on the
/// operation kind. In any case, the effect of the field mask is
/// required to be honored by the API.
///
/// ## Considerations for HTTP REST
///
/// The HTTP kind of an update operation which uses a field mask must
/// be set to PATCH instead of PUT in order to satisfy HTTP semantics
/// (PUT must only be used for full updates).
///
/// # JSON Encoding of Field Masks
///
/// In JSON, a field mask is encoded as a single string where paths are
/// separated by a comma. Fields name in each path are converted
/// to/from lower-camel naming conventions.
///
/// As an example, consider the following message declarations:
///
/// ```norust
///     message Profile {
///       User user = 1;
///       Photo photo = 2;
///     }
///     message User {
///       string display_name = 1;
///       string address = 2;
///     }
/// ```
///
/// In proto a field mask for `Profile` may look as such:
///
/// ```norust
///     mask {
///       paths: "user.display_name"
///       paths: "photo"
///     }
/// ```
///
/// In JSON, the same mask is represented as below:
///
/// ```norust
///     {
///       mask: "user.displayName,photo"
///     }
/// ```
///
/// # Field Masks and Oneof Fields
///
/// Field masks treat fields in oneofs just as regular fields. Consider the
/// following message:
///
/// ```norust
///     message SampleMessage {
///       oneof test_oneof {
///         string name = 4;
///         SubMessage sub_message = 9;
///       }
///     }
/// ```
///
/// The field mask can be:
///
/// ```norust
///     mask {
///       paths: "name"
///     }
/// ```
///
/// Or:
///
/// ```norust
///     mask {
///       paths: "sub_message"
///     }
/// ```
///
/// Note that oneof type names ("test_oneof" in this case) cannot be used in
/// paths.
///
/// ## Field Mask Verification
///
/// The implementation of any API method which has a FieldMask type field in the
/// request should verify the included field paths, and return an
/// `INVALID_ARGUMENT` error if any path is unmappable.
#[derive(Clone, Debug, Default, PartialEq, serde::Deserialize)]
#[non_exhaustive]
pub struct FieldMask {
    /// The set of field mask paths.
    #[serde(deserialize_with = "crate::field_mask::deserialize_paths")]
    pub paths: Vec<String>,
}

impl FieldMask {
    /// Set the paths.
    pub fn set_paths(mut self, paths: Vec<String>) -> Self {
        self.paths = paths;
        self
    }
}

impl crate::message::Message for FieldMask {
    fn typename() -> &'static str {
        "type.googleapis.com/google.protobuf.FieldMask"
    }
}

/// Implement [`serde`](::serde) serialization for [FieldMask]
impl serde::ser::Serialize for FieldMask {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("FieldMask", 1)?;
        state.serialize_field("paths", &self.paths.join(","))?;
        state.end()
    }
}

struct PathVisitor;

fn deserialize_paths<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    deserializer.deserialize_str(PathVisitor)
}

impl serde::de::Visitor<'_> for PathVisitor {
    type Value = Vec<String>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a string with comma-separated field mask paths)")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if value.is_empty() {
            Ok(Vec::new())
        } else {
            Ok(value.split(',').map(str::to_string).collect())
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::json;
    use test_case::test_case;

    type Result = std::result::Result<(), Box<dyn std::error::Error>>;

    #[test_case(vec![], ""; "Serialize empty")]
    #[test_case(vec!["field1"], "field1"; "Serialize single")]
    #[test_case(vec!["field1", "field2", "field3"], "field1,field2,field3"; "Serialize multiple")]
    fn test_serialize(paths: Vec<&str>, want: &str) -> Result {
        let value = serde_json::to_value(FieldMask {
            paths: paths.into_iter().map(str::to_string).collect(),
        })?;
        let got = value
            .get("paths")
            .ok_or("missing paths")?
            .as_str()
            .ok_or("paths is not str")?;
        assert_eq!(want, got);
        Ok(())
    }

    #[test_case("", vec![]; "Deserialize empty")]
    #[test_case("field1", vec!["field1"]; "Deserialize single")]
    #[test_case("field1,field2,field3", vec!["field1" ,"field2", "field3"]; "Deserialize multiple")]
    fn test_deserialize(paths: &str, mut want: Vec<&str>) -> Result {
        let value = json!({ "paths": paths });
        let mut got = serde_json::from_value::<FieldMask>(value)?;
        want.sort();
        got.paths.sort();
        assert_eq!(got.paths, want);
        Ok(())
    }

    #[test]
    fn deserialize_unexpected_input_type() -> Result {
        let got = serde_json::from_value::<FieldMask>(serde_json::json!({"paths": {"a": "b"}}));
        assert!(got.is_err());
        let msg = format!("{got:?}");
        assert!(msg.contains("field mask paths"), "message={}", msg);
        Ok(())
    }
}
