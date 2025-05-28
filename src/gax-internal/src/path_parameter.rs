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
#[non_exhaustive]
pub enum Error {
    #[error("missing required parameter {0}")]
    MissingRequiredParameter(String),
}

pub fn missing(name: &str) -> gax::error::Error {
    gax::error::Error::binding(Error::MissingRequiredParameter(name.to_string()))
}

#[cfg(test)]
mod tests {
    use super::Error;
    use std::error::Error as _;

    #[test]
    fn missing() {
        let e = super::missing("abc123");
        let fmt = format!("{e}");
        assert!(fmt.contains("abc123"), "{e:?}");
        let source = e.source().and_then(|e| e.downcast_ref::<Error>());
        assert!(
            matches!(source, Some(Error::MissingRequiredParameter(p)) if p == "abc123"),
            "{e:?}"
        );
    }
}
