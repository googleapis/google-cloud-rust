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

use super::{Error, Result};
use chrono::Utc;
use serde::Serialize;

/// JSON Web Signature for a token.
#[derive(Serialize)]
pub(crate) struct JwsClaims<'a> {
    pub(crate) iss: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) scope: Option<&'a str>,
    pub(crate) aud: &'a str,
    pub(crate) exp: Option<i64>,
    pub(crate) iat: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) typ: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) sub: Option<&'a str>,
}

impl JwsClaims<'_> {
    pub(crate) fn encode(&mut self) -> Result<String> {
        let now = Utc::now() - chrono::Duration::seconds(10);
        self.iat = self.iat.or_else(|| Some(now.timestamp()));
        self.exp = self
            .iat
            .or_else(|| Some((now + chrono::Duration::hours(1)).timestamp()));
        if self.exp.unwrap() < self.iat.unwrap() {
            return Err(Error::Other("exp must be later than iat".into()));
        }
        let json = serde_json::to_string(&self)?;
        Ok(base64::encode_config(json, base64::URL_SAFE_NO_PAD))
    }
}

/// The header that describes who, what, how a token was created.
#[derive(Serialize)]
pub(crate) struct JwsHeader<'a> {
    pub(crate) alg: &'a str,
    pub(crate) typ: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) kid: Option<&'a str>,
}

impl JwsHeader<'_> {
    pub(crate) fn encode(&self) -> Result<String> {
        let json = serde_json::to_string(&self)?;
        Ok(base64::encode_config(json, base64::URL_SAFE_NO_PAD))
    }
}
