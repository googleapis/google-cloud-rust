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

#[cfg(google_cloud_unstable_storage_bidi)]
#[cfg_attr(docsrs, doc(cfg(feature = "unstable-stream")))]
/// Represents the parameters of a request to open a new object for exclusive appends.
///
/// Consumers of the `google-cloud-storage` crate rarely have a need to use this type directly, the most common exception is when mocking of the `Storage` client.
#[derive(Clone, Debug, Default, PartialEq)]
#[non_exhaustive]
pub struct OpenAppendableObjectRequest {
    /// The object attributes and pre-conditions for the open operation.
    pub spec: crate::model::WriteObjectSpec,
    /// Additional request parameters.
    pub params: Option<crate::model::CommonObjectRequestParams>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_open_appendable_object_request() {
        let req = OpenAppendableObjectRequest {
            spec: crate::model::WriteObjectSpec::default(),
            params: None,
        };
        assert_eq!(req.spec.resource, None);
        assert_eq!(req.params, None);
    }
}
