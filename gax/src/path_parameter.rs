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

pub trait PathParameter {
    type P: Sized;
    fn required<'a>(&'a self, name: &str) -> std::result::Result<&'a Self::P, Error>;
}

impl<T> PathParameter for Option<T> {
    type P = T;
    fn required<'a>(&'a self, name: &str) -> std::result::Result<&'a Self::P, Error> {
        self.as_ref()
            .ok_or_else(|| Error::MissingRequiredParameter(name.into()))
    }
}

impl<T> PathParameter for T
where
    T: crate::request_parameter::RequestParameter,
{
    type P = T;
    fn required<'a>(&'a self, _: &str) -> std::result::Result<&'a Self::P, Error> {
        Ok(self)
    }
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("missing required parameter {0}")]
    MissingRequiredParameter(String),
}

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
