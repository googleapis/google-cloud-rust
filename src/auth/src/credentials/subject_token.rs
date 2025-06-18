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

pub struct Builder {
  token: String,
}

impl Builder {
    fn new(token: String) -> Self {
        Self { token }
    }
    
    fn build(self) -> SubjectToken {
        SubjectToken { token: self.token }
    }
}

pub struct SubjectToken {
  token: String,
}

pub trait SubjectTokenProviderError: std::error::Error {
    /// Return true if the error is transient and the call may succeed in the future.
    ///
    /// Applicatiosn should only return true if the error automatically 
    /// recovers, without the need for any human action.
    ///
    /// Timeouts and network problems are good candidates for `is_transient() == true`.
    /// Configuration errors that require changing a file, or installing an executable are not.
    fn is_transient(&self) -> bool;
}


pub trait SubjectTokenProvider: std::fmt::Debug + Send + Sync {
       type Error: SubjectTokenProviderError;
fn subject_token(&self) -> impl Future<Output = Result<SubjectToken, Self::Error>> + Send;
}

pub(crate) mod dynamic {
    use super::*;
    #[async_trait::async_trait]
    pub trait SubjectTokenProvider<E>: std::fmt::Debug + Send + Sync {
        async fn subject_token(&self) -> Result<SubjectToken, E>;
    }

    #[async_trait::async_trait]
    impl<T> SubjectTokenProvider<T::Error> for T
    where
        T: super::SubjectTokenProvider,
    {
        async fn subject_token(&self) -> Result<SubjectToken, T::Error> {
            T::subject_token(self).await
        }
    }
}
