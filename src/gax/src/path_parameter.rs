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

//! Defines traits and helpers to serialize path parameters.
//!
//! Path parameters in the Google APIs are always required, but they may need
//! to be source from fields that are `Option<T>`, or they may be nested fields.
//! In the later case, the containing submessage is always `Option<M>`.
//! 
//! We could change the generator to issue different code depending on whether
//! the parameter is `Option<T>` or not. In the first case we would have the
//! generator write:
//! 
//! ```norust
//! format!("/v1/foos/{}"
//!     request.field.unwrap_or_else(|| gax::path_parameter::missing("field"))?
//! )
//! ```
//! 
//! while if the field is not optional we could write:
//! 
//! ```norust
//! format!("/v1/foos/{}"
//!     request.field
//! )
//! ```
//! 
//! But that requires more cleverness in the generator than we wanted to
//! implement.
//!
//! This module defines some traits and helpers to simplify the code generator.
//! They automatically convert `Option<T>` to `Result<T, Error>`, so the
//! generator always writes:
//!
//! gax::path_parameter::required(req.field, name)?.sub_field
//! 
//! and for non-nested fields:
//! 
//! gax::path_parameter::required(req.field, name)?
//!
//! If accessing deeply nested fields that can results in multiple calls to
//! `required`.


/// Defines how to handle a path parameter.
/// 
/// Path parameters are always required, but sometimes the field in the request
/// is an Option<T>. We want to simplify the code generator, and  and just send
/// , when their field is an option 
pub trait PathParameter {
    type P: Sized;
    fn required<'a>(&'a self, name: &str) -> Result<&'a Self::P>;
}

impl<T> PathParameter for Option<T> {
    type P = T;
    fn required<'a>(&'a self, name: &str) -> Result<&'a Self::P> {
        self.as_ref()
            .ok_or_else(|| Error::MissingRequiredParameter(name.into()))
    }
}

impl<T> PathParameter for T
where
    T: crate::request_parameter::RequestParameter,
{
    type P = T;
    fn required<'a>(&'a self, _: &str) -> Result<&'a Self::P,> {
        Ok(self)
    }
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("missing required parameter {0}")]
    MissingRequiredParameter(String),
}

/// 
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;
    type Result = std::result::Result<(), Error>;

    #[test]
    fn optional_with_value() -> Result {
        let v = Some("abc".to_string());
        let got = PathParameter::required(&v, "name")?;
        assert_eq!("abc", got);
        Ok(())
    }

    #[test]
    fn optional_without_value() -> Result {
        let v = None::<String>;
        let got = PathParameter::required(&v, "name");
        assert!(got.is_err(), "expected error {:?}", got);
        Ok(())
    }

    #[test]
    fn required() -> Result {
        let v = "value".to_string();
        let got = PathParameter::required(&v, "name")?;
        assert_eq!("value", got);
        Ok(())
    }
}
