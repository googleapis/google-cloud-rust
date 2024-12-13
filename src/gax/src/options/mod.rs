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

use std::any::{Any, TypeId};
use std::collections::HashMap;

// The number of options tends to grow rather quickly. Keep them in separate
// modules to avoid overly large files and make these easier to find.
mod request_timeout;
pub use request_timeout::*;
mod user_agent_prefix;
pub use user_agent_prefix::*;

pub trait RequestOption: 'static
where
    Self::Type: Any + Clone + Send,
{
    /// The type of the option.
    type Type;
}

/// A set of options configuring a single request.
///
/// Some applications need to override the default settings on a per-request
/// basis. For example, they may want to change the RPC timeout, or the retry
/// policy on different requests.
///
/// The client RPCs accept an (optional) set of options applicable to the
/// request.
///
/// Example:
/// ```
/// fn WithDefaults(client: &Client, req: ListFoosRequest) {
///     client.list_foos(req, None)
/// }
///
/// fn WithOptions(client: &Client, req: ListFoosRequest) {
///     use std::time::Duration;
///     let options = RequestOptions::new().set::<RequestTimeout>(Duration::from_secs(60));
///     client.list_foos(req, options.into())
/// }
///
/// fn Get(options: &RequestOptions) {
///     let value = options.get::<RequestTimeout>();
///     println!("current timeout set to {value:?}");
/// }
/// # struct ListFoosRequest;
/// # struct Client {};
/// # impl Client { pub fn list_foos(&self, req: ListFoosRequest, options: Option<RequestOptions>) {} }
/// # use gcp_sdk_gax::options::RequestOptions;
/// # use gcp_sdk_gax::options::RequestTimeout;
/// ```
#[derive(Debug, Default)]
pub struct RequestOptions {
    options: HashMap<TypeId, Box<dyn Any + Send>>,
}

impl RequestOptions {
    /// Create a new (empty) set of options.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the option `O` to the value in `value`.
    ///
    /// # Parameters
    /// * `value` - the new value for the option.
    /// * `O` - the name of the option being set.
    pub fn set<O>(mut self, value: impl Into<O::Type>) -> Self
    where
        O: RequestOption,
    {
        self.options
            .insert(TypeId::of::<O>(), Box::new(value.into()));
        self
    }

    /// Adds all the options from `values` to this collection of options.
    ///
    /// If both collections have the same option set, the value from `new` takes
    /// precedence.
    ///
    /// # Parameters
    /// * `new` - the new options values.
    pub fn extend(mut self, new: RequestOptions) -> Self {
        self.options.extend(new.options);
        self
    }

    /// Retrieves the value for option `O`, or `None`if it is not set.
    ///
    /// #Parameters
    /// * `O` - the name of the option to retrieve.
    pub fn get<O: RequestOption>(&self) -> Option<O::Type> {
        self.options
            .get(&TypeId::of::<O>())
            .and_then(|v| v.downcast_ref::<O::Type>())
            .cloned()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_set_get() {
        let options = RequestOptions::new()
            .set::<RequestTimeout>(Duration::from_secs(123))
            .set::<UserAgentPrefix>("myapp/3.4.5");
        assert_eq!(
            options.get::<RequestTimeout>(),
            Some(Duration::from_secs(123))
        );
        assert_eq!(
            options.get::<UserAgentPrefix>(),
            Some("myapp/3.4.5".to_string())
        );
    }
}
