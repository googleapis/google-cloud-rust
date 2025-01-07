// Copyright 2021 Google LLC
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

use super::{Error, ErrorKind, Result};
use chrono::Utc;
use serde::Serialize;

/// JSON Web Signature for a token.
#[derive(Serialize)]
pub struct JwsClaims<'a> {
    pub iss: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<&'a str>,
    pub aud: &'a str,
    pub exp: Option<i64>,
    pub iat: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub typ: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sub: Option<&'a str>,
}

impl JwsClaims<'_> {
    pub fn encode(&mut self) -> Result<String> {
        let now = Utc::now() - chrono::Duration::seconds(10);
        self.iat = self.iat.or_else(|| Some(now.timestamp()));
        self.exp = self
            .iat
            .or_else(|| Some((now + chrono::Duration::hours(1)).timestamp()));
        if self.exp.unwrap() < self.iat.unwrap() {
            return Err(Error::new(
                "exp must be later than iat",
                ErrorKind::Validation,
            ));
        }

        use base64::prelude::{Engine as _, BASE64_URL_SAFE_NO_PAD};
        let json = serde_json::to_string(&self).map_err(Error::wrap_serialization)?;
        Ok(BASE64_URL_SAFE_NO_PAD.encode(json.as_bytes()))
    }
}

/// The header that describes who, what, how a token was created.
#[derive(Serialize)]
pub struct JwsHeader<'a> {
    pub alg: &'a str,
    pub typ: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kid: Option<&'a str>,
}

impl JwsHeader<'_> {
    pub fn encode(&self) -> Result<String> {
        use base64::prelude::{Engine as _, BASE64_URL_SAFE_NO_PAD};
        let json = serde_json::to_string(&self).map_err(Error::wrap_serialization)?;
        Ok(BASE64_URL_SAFE_NO_PAD.encode(json.as_bytes()))
    }
}