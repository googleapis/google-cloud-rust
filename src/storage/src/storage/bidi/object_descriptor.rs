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

use super::stub::dynamic::ObjectDescriptor as ObjectDescriptorStub;
use crate::model::Object;
use crate::model_ext::ReadRange;
use crate::read_object::ReadObjectResponse;

#[derive(Debug)]
pub struct ObjectDescriptor {
    inner: Box<dyn ObjectDescriptorStub>,
}

impl ObjectDescriptor {
    pub fn new<T>(inner: T) -> Self
    where
        T: ObjectDescriptorStub + 'static,
    {
        Self {
            inner: Box::new(inner),
        }
    }

    pub fn object(&self) -> &Object {
        self.inner.object()
    }

    pub async fn read_range(&self, range: ReadRange) -> ReadObjectResponse {
        let inner = self.inner.read_range(range).await;
        ReadObjectResponse::from_dyn(inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model_ext::ObjectHighlights;
    use crate::read_object::dynamic::ReadObjectResponse;
    use mockall::mock;

    #[tokio::test]
    async fn can_be_mocked() -> anyhow::Result<()> {
        let object = Object::new().set_name("test-object").set_generation(123456);
        let mut mock = MockDescriptor::new();
        mock.expect_object().times(1).return_const(object.clone());
        mock.expect_read_range()
            .times(1)
            .withf(|range| range.0 == ReadRange::segment(100, 200).0)
            .returning(|_| Box::new(MockResponse::new()));

        let descriptor = ObjectDescriptor::new(mock);
        assert_eq!(descriptor.object(), &object);

        let _reader = descriptor.read_range(ReadRange::segment(100, 200)).await;
        Ok(())
    }

    mock! {
        #[derive(Debug)]
        Descriptor {}

        impl super::super::stub::ObjectDescriptor for Descriptor {
            fn object(&self) -> &Object;
            async fn read_range(&self, range: ReadRange) -> Box<dyn ReadObjectResponse + Send>;
        }
    }

    mock! {
        #[derive(Debug)]
        Response {}

        #[async_trait::async_trait]
        impl crate::read_object::dynamic::ReadObjectResponse for Response {
            fn object(&self) -> ObjectHighlights;
            async fn next(&mut self) -> Option<crate::Result<bytes::Bytes>>;
        }
    }
}
