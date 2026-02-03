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

use crate::credentials::Result;
use crate::errors;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use time::OffsetDateTime;

// Services reject assertions with `iat` in the future. Unfortunately all
// machines have some amount of clock skew, and it is possible that
// the machine creating this assertion has a clock a few milliseconds
// or seconds ahead of the machines receiving the assertion.
// Create the assertion with a 10 second margin to avoid most clock
// skew problems.
pub const CLOCK_SKEW_FUDGE: Duration = Duration::from_secs(10);
pub const DEFAULT_TOKEN_TIMEOUT: Duration = Duration::from_secs(3600);

/// JSON Web Signature for a token.
#[derive(Serialize)]
pub struct JwsClaims {
    pub iss: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    pub aud: Option<String>,
    #[serde(with = "time::serde::timestamp")]
    pub exp: OffsetDateTime,
    #[serde(with = "time::serde::timestamp")]
    pub iat: OffsetDateTime,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub typ: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sub: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_audience: Option<String>,
}

impl JwsClaims {
    pub fn encode(&self) -> Result<String> {
        if self.exp < self.iat {
            return Err(errors::non_retryable_from_str(format!(
                "expiration time {:?}, must be later than issued time {:?}",
                self.exp, self.iat
            )));
        }

        if self.aud.is_some() && self.scope.is_some() {
            return Err(errors::non_retryable_from_str(format!(
                "Found {:?} for audience and {:?} for scope, however expecting only 1 of them to be set.",
                self.aud, self.scope
            )));
        }

        use base64::prelude::{BASE64_URL_SAFE_NO_PAD, Engine as _};
        let json = serde_json::to_string(&self).map_err(errors::non_retryable)?;
        Ok(BASE64_URL_SAFE_NO_PAD.encode(json.as_bytes()))
    }
}

/// The header that describes who, what, and how a token was created.
#[derive(Serialize, Deserialize, Debug)]
pub struct JwsHeader<'a> {
    pub alg: &'a str,
    pub typ: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kid: Option<String>,
}

impl JwsHeader<'_> {
    pub fn encode(&self) -> Result<String> {
        use base64::prelude::{BASE64_URL_SAFE_NO_PAD, Engine as _};
        let json = serde_json::to_string(&self).map_err(errors::non_retryable)?;
        Ok(BASE64_URL_SAFE_NO_PAD.encode(json.as_bytes()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use serde_json::Value;

    #[test]
    fn test_jws_claims_encode_partial() {
        let now = OffsetDateTime::now_utc();
        let then = now + Duration::from_secs(4200);

        let claims = JwsClaims {
            iss: "test_iss".to_string(),
            scope: None,
            aud: Some("test_aud".to_string()),
            exp: then,
            iat: now,
            typ: None,
            sub: None,
            target_audience: None,
        };

        let encoded = claims.encode().unwrap();
        let decoded = String::from_utf8(
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(encoded)
                .unwrap(),
        )
        .unwrap();

        let v: Value = serde_json::from_str(&decoded).unwrap();
        assert_eq!(v["iss"], "test_iss");
        assert_eq!(v.get("scope"), None);
        assert_eq!(v["aud"], "test_aud");
        assert_eq!(v["iat"], now.unix_timestamp());
        assert_eq!(v["exp"], then.unix_timestamp());
        assert_eq!(v.get("typ"), None);
        assert_eq!(v.get("sub"), None);
    }

    #[test]
    fn test_jws_claims_encode_full() {
        let now = OffsetDateTime::now_utc();
        let then = now + Duration::from_secs(4200);

        let claims = JwsClaims {
            iss: "test_iss".to_string(),
            scope: Some("scope1 scope2".to_string()),
            aud: None,
            exp: then,
            iat: now,
            typ: Some("test_typ".to_string()),
            sub: Some("test_sub".to_string()),
            target_audience: None,
        };

        let encoded = claims.encode().unwrap();
        let decoded = String::from_utf8(
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(encoded)
                .unwrap(),
        )
        .unwrap();
        let v: Value = serde_json::from_str(&decoded).unwrap();

        assert_eq!(v["iss"], "test_iss");
        assert_eq!(v["scope"], "scope1 scope2");

        assert_eq!(v["iat"], now.unix_timestamp());
        assert_eq!(v["exp"], then.unix_timestamp());
        assert_eq!(v["typ"], "test_typ");
        assert_eq!(v["sub"], "test_sub");
    }

    #[test]
    fn test_jws_claims_encode_error_exp_before_iat() {
        let now = OffsetDateTime::now_utc();
        let then = now - Duration::from_secs(4200);

        let claims = JwsClaims {
            iss: "test_iss".to_string(),
            scope: None,
            aud: None,
            exp: then,
            iat: now,
            typ: None,
            sub: None,
            target_audience: None,
        };
        let expected_error_message = "must be later than issued time";
        assert!(
            claims
                .encode()
                .is_err_and(|e| e.to_string().contains(expected_error_message))
        );
    }

    #[test]
    fn test_jws_claims_encode_error_set_scope_and_aud() {
        let now = OffsetDateTime::now_utc();
        let then = now + Duration::from_secs(4200);

        let claims = JwsClaims {
            iss: "test_iss".to_string(),
            scope: Some("scope".to_string()),
            aud: Some("test-aud".to_string()),
            exp: then,
            iat: now,
            typ: None,
            sub: None,
            target_audience: None,
        };
        let expected_error_message = "expecting only 1 of them to be set";
        assert!(
            claims
                .encode()
                .is_err_and(|e| e.to_string().contains(expected_error_message))
        );
    }

    #[test]
    fn test_jws_header_encode() {
        let header = JwsHeader {
            alg: "RS256",
            typ: "JWT",
            kid: Some("some_key_id".to_string()),
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
