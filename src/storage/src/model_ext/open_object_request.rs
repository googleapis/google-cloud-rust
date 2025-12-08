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

use crate::{google::storage::v2::BidiReadObjectSpec, model::CommonObjectRequestParams};
use gaxi::prost::ToProto;

/// The request type for a bidi read object streaming RPC.
///
/// This type is used in the Storage [stub][crate::stub::Storage] to represent
/// a request for [open_object][crate::client::Storage::open_object].
/// Applications rarely use this type directly, but it might be used in tests
/// where the client library is being mocked.
// Implementation note: this type exclude some fields from the proto, such as
// the read handle and routing token, as these cannot be set by the application.
#[derive(Clone, Debug, Default, PartialEq)]
#[non_exhaustive]
pub struct OpenObjectRequest {
    /// The bucket containing the target object.
    pub bucket: String,
    /// The target object name.
    pub object: String,
    /// The target object generation. If zero, then target the latest version of the object.
    pub generation: i64,
    /// If set, return an error if the current generation does not match the value.
    pub if_generation_match: Option<i64>,
    /// If set, return an error if the current generation matches the value.
    pub if_generation_not_match: Option<i64>,
    /// If set, return an error if the current metageneration does not match the value.
    pub if_metageneration_match: Option<i64>,
    /// If set, return an error if the current metageneration matches the value.
    pub if_metageneration_not_match: Option<i64>,
    /// Parameters that can be passed to any object request.
    ///
    /// At the moment, these are only encryption parameters for
    /// [Customer-Supplied Encryption Keys].
    ///
    /// [Customer-Supplied Encryption Keys]: https://docs.cloud.google.com/storage/docs/encryption/customer-supplied-keys
    pub common_object_request_params: Option<CommonObjectRequestParams>,
}

impl OpenObjectRequest {
    /// Sets the [bucket][OpenObjectRequest::bucket] field.
    pub fn set_bucket<T>(mut self, v: T) -> Self
    where
        T: Into<String>,
    {
        self.bucket = v.into();
        self
    }

    /// Sets the [object][OpenObjectRequest::object] field.
    pub fn set_object<T>(mut self, v: T) -> Self
    where
        T: Into<String>,
    {
        self.object = v.into();
        self
    }

    /// Sets the [generation][OpenObjectRequest::generation] field.
    pub fn set_generation(mut self, v: i64) -> Self {
        self.generation = v;
        self
    }

    /// Sets the [if_generation_match][OpenObjectRequest::if_generation_match] field.
    pub fn set_if_generation_match(mut self, v: i64) -> Self {
        self.if_generation_match = Some(v);
        self
    }

    /// Sets the [if_generation_match][OpenObjectRequest::if_generation_match] field.
    pub fn set_or_clear_if_generation_match(mut self, v: Option<i64>) -> Self {
        self.if_generation_match = v;
        self
    }

    /// Sets the [if_generation_not_match][OpenObjectRequest::if_generation_not_match] field.
    pub fn set_if_generation_not_match(mut self, v: i64) -> Self {
        self.if_generation_not_match = Some(v);
        self
    }

    /// Sets the [if_generation_not_match][OpenObjectRequest::if_generation_not_match] field.
    pub fn set_or_clear_if_generation_not_match(mut self, v: Option<i64>) -> Self {
        self.if_generation_not_match = v;
        self
    }

    /// Sets the [if_metageneration_match][OpenObjectRequest::if_metageneration_match] field.
    pub fn set_if_metageneration_match(mut self, v: i64) -> Self {
        self.if_metageneration_match = Some(v);
        self
    }

    /// Sets the [if_metageneration_match][OpenObjectRequest::if_metageneration_match] field.
    pub fn set_or_clear_if_metageneration_match(mut self, v: Option<i64>) -> Self {
        self.if_metageneration_match = v;
        self
    }

    /// Sets the [if_metageneration_not_match][OpenObjectRequest::if_metageneration_not_match] field.
    pub fn set_if_metageneration_not_match(mut self, v: i64) -> Self {
        self.if_metageneration_not_match = Some(v);
        self
    }

    /// Sets the [if_metageneration_not_match][OpenObjectRequest::if_metageneration_not_match] field.
    pub fn set_or_clear_if_metageneration_not_match(mut self, v: Option<i64>) -> Self {
        self.if_metageneration_not_match = v;
        self
    }

    /// Sets the [common_object_request_params][OpenObjectRequest::common_object_request_params] field.
    pub fn set_common_object_request_params(mut self, v: CommonObjectRequestParams) -> Self {
        self.common_object_request_params = Some(v);
        self
    }

    /// Sets the [common_object_request_params][OpenObjectRequest::common_object_request_params] field.
    pub fn set_or_clear_common_object_request_params(
        mut self,
        v: Option<CommonObjectRequestParams>,
    ) -> Self {
        self.common_object_request_params = v;
        self
    }
}

impl From<OpenObjectRequest> for BidiReadObjectSpec {
    fn from(value: OpenObjectRequest) -> Self {
        let proto = value
            .common_object_request_params
            .map(ToProto::to_proto)
            .transpose()
            .expect("CommonObjectRequestParams to proto never fails");
        Self {
            bucket: value.bucket,
            object: value.object,
            generation: value.generation,
            if_generation_match: value.if_generation_match,
            if_generation_not_match: value.if_generation_not_match,
            if_metageneration_match: value.if_metageneration_match,
            if_metageneration_not_match: value.if_metageneration_not_match,
            common_object_request_params: proto,
            ..BidiReadObjectSpec::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::google::storage::v2::CommonObjectRequestParams as ProtoParams;
    use pastey::paste;

    #[test]
    fn bucket() {
        let got = OpenObjectRequest::default().set_bucket("bucket");
        assert_eq!(got.bucket, "bucket");
        let got = got.set_bucket("");
        assert_eq!(got, OpenObjectRequest::default());
    }

    #[test]
    fn object() {
        let got = OpenObjectRequest::default().set_object("object");
        assert_eq!(got.object, "object");
        let got = got.set_object("");
        assert_eq!(got, OpenObjectRequest::default());
    }

    #[test]
    fn generation() {
        let got = OpenObjectRequest::default().set_generation(42);
        assert_eq!(got.generation, 42);
        let got = got.set_generation(0);
        assert_eq!(got, OpenObjectRequest::default());
    }

    macro_rules! setter {
        ($field:ident) => {
            paste! {
                #[test]
                fn $field() {
                    let got = OpenObjectRequest::default().[<set_$field>](42);
                    assert_eq!(got.$field, Some(42));
                    let got = got.[<set_or_clear_$field>](Some(7));
                    assert_eq!(got.$field, Some(7));
                    let got = got.[<set_or_clear_$field>](None);
                    assert_eq!(got.$field, None);
                    assert_eq!(got, OpenObjectRequest::default());
                }
            }
        };
    }

    setter!(if_generation_match);
    setter!(if_generation_not_match);
    setter!(if_metageneration_match);
    setter!(if_metageneration_not_match);

    #[test]
    fn common_object_request_params() {
        let got = OpenObjectRequest::default().set_common_object_request_params(
            CommonObjectRequestParams::new().set_encryption_algorithm("abc"),
        );
        assert_eq!(
            got.common_object_request_params,
            Some(CommonObjectRequestParams::new().set_encryption_algorithm("abc"))
        );
        let got = got.set_or_clear_common_object_request_params(None);
        assert_eq!(got, OpenObjectRequest::default());
    }

    #[test]
    fn from() {
        let got = BidiReadObjectSpec::from(
            OpenObjectRequest::default()
                .set_bucket("bucket")
                .set_object("object")
                .set_generation(123)
                .set_if_generation_match(234)
                .set_if_generation_not_match(345)
                .set_if_metageneration_match(456)
                .set_if_metageneration_not_match(567)
                .set_common_object_request_params(
                    CommonObjectRequestParams::new().set_encryption_algorithm("test-abc"),
                ),
        );
        let want = BidiReadObjectSpec {
            bucket: "bucket".into(),
            object: "object".into(),
            generation: 123,
            if_generation_match: Some(234),
            if_generation_not_match: Some(345),
            if_metageneration_match: Some(456),
            if_metageneration_not_match: Some(567),
            common_object_request_params: Some(ProtoParams {
                encryption_algorithm: "test-abc".into(),
                ..ProtoParams::default()
            }),
            ..BidiReadObjectSpec::default()
        };
        assert_eq!(got, want);
    }
}
