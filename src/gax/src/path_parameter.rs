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

//! Handling of missing path parameters.
//!
//! Parameters used to build the request path (aka 'path parameters') are
//! required. But for complicated reasons they may appear in optional fields.
//! The generator needs to return an error when the parameter is missing, and
//! a small helper function makes the generated code easier to read.

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("missing required parameter {0}")]
    MissingRequiredParameter(String),
}

pub fn missing(name: &str) -> crate::error::Error {
    crate::error::Error::other(Error::MissingRequiredParameter(name.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing() {
        let e = super::missing("abc123");
        let fmt = format!("{e}");
        assert!(fmt.contains("abc123"), "{e:?}");
        let inner = e.as_inner::<super::Error>().unwrap();
        match inner {
            Error::MissingRequiredParameter(s) => {
                assert_eq!(s.as_str(), "abc123");
            }
        }
    }
}
