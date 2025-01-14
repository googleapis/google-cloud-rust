// Copyright 2025 Google LLC
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

use crate::credentials::CredentialError;
use crate::credentials::Result;
use derive_builder::Builder;
use serde::Serialize;
use std::time::Duration;
use time::OffsetDateTime;

const DEFAULT_TOKEN_TIMEOUT: Duration = Duration::from_secs(3600);

/// JSON Web Signature for a token.
#[derive(Clone, Serialize, Default, Builder)]
#[builder(setter(into, strip_option), default)]
pub struct JwsClaims<'a> {
    pub iss: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<&'a str>,
    pub aud: Option<&'a str>,
    #[serde(with = "time::serde::timestamp::option")]
    pub exp: Option<OffsetDateTime>,
    #[serde(with = "time::serde::timestamp::option")]
    pub iat: Option<OffsetDateTime>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub typ: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sub: Option<&'a str>,
}

impl JwsClaims<'_> {
    pub fn encode(&self) -> Result<String> {
        // Services reject assertions with `iat` in the future. Unfortunately all
        // machines have some amount of clock skew, and it is possible that
        // the machine creating this assertion has a clock a few milliseconds
        // or seconds ahead of the machines receiving the assertion.
        // Create the assertion with a 10 second margin to avoid most clock
        // skew problems.
        let now = OffsetDateTime::now_utc() - Duration::from_secs(10);
        let iat = self.iat.unwrap_or(now);
        let exp = self.exp.unwrap_or_else(|| now + DEFAULT_TOKEN_TIMEOUT);
        if exp < iat {
            return Err(CredentialError::non_retryable(format!(
                "expiration time {:?}, must be later than issued time {:?}",
                exp, iat
            )));
        }
        let updated_jws_claim = JwsClaims {
            iat: Some(iat),
            exp: Some(exp),
            ..self.clone()
        };
        use base64::prelude::{Engine as _, BASE64_URL_SAFE_NO_PAD};
        let json =
            serde_json::to_string(&updated_jws_claim).map_err(CredentialError::non_retryable)?;
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
        let json = serde_json::to_string(&self).map_err(CredentialError::non_retryable)?;
        Ok(BASE64_URL_SAFE_NO_PAD.encode(json.as_bytes()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use serde_json::Value;

    #[test]
    fn test_jws_claims_encode_defaults() {
        let claims = JwsClaimsBuilder::default()
            .iss("test_iss")
            .aud("test_aud")
            .build()
            .unwrap();

        let encoded = claims.encode().unwrap();
        let decoded = String::from_utf8(
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(encoded)
                .unwrap(),
        )
        .unwrap();

        let now = OffsetDateTime::now_utc() - Duration::from_secs(10);
        let expected_iat = now.unix_timestamp();
        let expected_exp = (now + DEFAULT_TOKEN_TIMEOUT).unix_timestamp();

        let v: Value = serde_json::from_str(&decoded).unwrap();
        assert_eq!(v["iss"], "test_iss");
        assert_eq!(v.get("scope"), None);
        assert_eq!(v["aud"], "test_aud");
        assert_eq!(v["iat"], expected_iat);
        assert_eq!(v["exp"], expected_exp);
        assert_eq!(v.get("typ"), None);
        assert_eq!(v.get("sub"), None);
    }

    #[test]
    fn test_jws_claims_encode_custom() {
        let iat_custom = OffsetDateTime::now_utc() - DEFAULT_TOKEN_TIMEOUT;
        let exp_custom = OffsetDateTime::now_utc() + DEFAULT_TOKEN_TIMEOUT;

        let claims = JwsClaimsBuilder::default()
            .iss("test_iss")
            .aud("test_aud")
            .iat(iat_custom)
            .exp(exp_custom)
            .typ("test_typ")
            .sub("test_sub")
            .scope("test_scope")
            .build()
            .unwrap();

        let encoded = claims.encode().unwrap();
        let decoded = String::from_utf8(
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(encoded)
                .unwrap(),
        )
        .unwrap();
        let v: Value = serde_json::from_str(&decoded).unwrap();

        assert_eq!(v["iss"], "test_iss");
        assert_eq!(v["scope"], "test_scope");
        assert_eq!(v["aud"], "test_aud");

        assert_eq!(v["iat"], iat_custom.unix_timestamp());
        assert_eq!(v["exp"], exp_custom.unix_timestamp());
        assert_eq!(v["typ"], "test_typ");
        assert_eq!(v["sub"], "test_sub");
    }

    #[test]
    fn test_jws_claims_encode_error() {
        let claims = JwsClaimsBuilder::default()
            .iss("test_iss")
            .exp(OffsetDateTime::now_utc() - DEFAULT_TOKEN_TIMEOUT)
            .build()
            .unwrap();
        assert!(claims.encode().is_err());
    }

    #[test]
    fn test_jws_header_encode() {
        let header = JwsHeader {
            alg: "RS256",
            typ: "JWT",
            kid: Some("some_key_id"),
        };
        let encoded = header.encode().unwrap();
        let decoded = String::from_utf8(
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(encoded)
                .unwrap(),
        )
        .unwrap();
        let v: Value = serde_json::from_str(&decoded).unwrap();

        assert_eq!(v["alg"], "RS256");
        assert_eq!(v["typ"], "JWT");
        assert_eq!(v["kid"], "some_key_id");
    }

    #[test]
    fn test_jws_header_encode_no_kid() {
        let header = JwsHeader {
            alg: "RS256",
            typ: "JWT",
            kid: None,
        };
        let encoded = header.encode().unwrap();
        let decoded = String::from_utf8(
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(encoded)
                .unwrap(),
        )
        .unwrap();
        let v: Value = serde_json::from_str(&decoded).unwrap();

        assert_eq!(v["alg"], "RS256");
        assert_eq!(v["typ"], "JWT");
        assert_eq!(v.get("kid"), None);
    }
}
