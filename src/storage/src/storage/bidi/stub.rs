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

use crate::model::Object;
use crate::model_ext::ReadRange;
use crate::read_object::ReadObjectResponse;

pub(crate) mod dynamic {
    use super::{Object, ReadObjectResponse, ReadRange};
    use http::HeaderMap;

    #[async_trait::async_trait]
    pub trait ObjectDescriptor: std::fmt::Debug + Send + Sync {
        fn object(&self) -> Object;
        async fn read_range(&self, range: ReadRange) -> ReadObjectResponse;
        fn headers(&self) -> HeaderMap;
    }

    #[async_trait::async_trait]
    impl<T: crate::stub::ObjectDescriptor> ObjectDescriptor for T {
        fn object(&self) -> Object {
            T::object(self)
        }

        async fn read_range(&self, range: ReadRange) -> ReadObjectResponse {
            T::read_range(self, range).await
        }

        fn headers(&self) -> HeaderMap {
            T::headers(self)
        }
    }
}
