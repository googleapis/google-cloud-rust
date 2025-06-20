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

use crate::credentials::errors::SubjectTokenProviderError;

pub struct Builder {
    token: String,
}

impl Builder {
    pub fn new(token: String) -> Self {
        Self { token }
    }

    pub fn build(self) -> SubjectToken {
        SubjectToken { token: self.token }
    }
}

#[derive(Debug)]
pub struct SubjectToken {
    pub(crate) token: String,
}

pub trait SubjectTokenProvider: std::fmt::Debug + Send + Sync {
    type Error: SubjectTokenProviderError;
    fn subject_token(&self) -> impl Future<Output = Result<SubjectToken, Self::Error>> + Send;
}
