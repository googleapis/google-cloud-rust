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

//! Per request options.

/// A set of options configuring a single request.
///
/// Application only use this class directly in mocks, where they may want to
/// verify their application has configured all the right request parameters and
/// options.
///
/// All other code uses this type indirectly, via the per-request builders.
#[derive(Clone, Debug, Default)]
pub struct RequestOptions {
    user_agent: Option<String>,
}

impl RequestOptions {
    /// Prepends this prefix to the user agent header value.
    pub fn set_user_agent<T: Into<String>>(&mut self, v: T) {
        self.user_agent = Some(v.into());
    }

    /// Gets the current user-agent prefix
    pub fn user_agent_prefix(&self) -> &Option<String> {
        &self.user_agent
    }
}

/// Implementations of this trait provide setters to configure request options.
/// 
/// The Google Cloud Client Libraries for Rust provide a builder for each RPC.
/// These builders can be used to set the request parameters, e.g., the name of
/// the resource targeted by the RPC, as well as any options affecting the
/// request, such as additional headers or timeouts.
pub trait RequestOptionsBuilder {
    /// Set the user agent header.
    fn with_user_agent<T: Into<String>>(self, v: T) -> Self;
}

/// Simplify implementation of the `RequestOptionsBuilder` trait in generated
/// code.
/// 
/// This is an implementation detail, most applications have little need to
/// worry about or use this trait.
pub trait RequestBuilder {
    fn request_options(&mut self) -> &mut RequestOptions;
}

/// Implements the [RequestOptionsBuilder] trait for any [RequestBuilder]
/// implementation.
impl<T> RequestOptionsBuilder for T
where
    T: RequestBuilder,
{
    fn with_user_agent<V: Into<String>>(mut self, v: V) -> Self {
        self.request_options().set_user_agent(v);
        self
    }
}
