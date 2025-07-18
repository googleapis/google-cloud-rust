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

use google_cloud_auth::credentials::*;

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;
    use http::Extensions;
    use rustls::crypto::{CryptoProvider, KeyProvider};
    use scoped_env::ScopedEnv;

    // TODO(#1442) : We should use auth's factory function specifically for
    // service account credentials when it is available, instead of using the
    // generic ADC factory function with delicately crafted json.
    async fn test_service_account_credentials() -> Credentials {
        let contents = r#"{
            "type": "service_account",
            "project_id": "test-project-id",
            "private_key_id": "test-private-key-id",
            "private_key": "-----BEGIN PRIVATE KEY-----\nBLAHBLAHBLAH\n-----END PRIVATE KEY-----\n",
            "client_email": "test-client-email",
            "universe_domain": "test-universe-domain"
        }"#;

        let file = tempfile::NamedTempFile::new().unwrap();
        let path = file.into_temp_path();
        std::fs::write(&path, contents).expect("Unable to write to temporary file.");
        let _e = ScopedEnv::set("GOOGLE_APPLICATION_CREDENTIALS", path.to_str().unwrap());

        let creds = Builder::default().build().unwrap();
        let fmt = format!("{creds:?}");
        assert!(fmt.contains("ServiceAccountCredentials"));

        creds
    }

    const CUSTOM_ERROR: &str = "Custom error for the `uses_installed_crypto_provider` unit test.";

    #[derive(Debug)]
    struct FakeKeyProvider {}

    impl KeyProvider for FakeKeyProvider {
        fn load_private_key(
            &self,
            _key_der: rustls::pki_types::PrivateKeyDer<'static>,
        ) -> std::result::Result<std::sync::Arc<dyn rustls::sign::SigningKey>, rustls::Error>
        {
            Err(rustls::Error::General(CUSTOM_ERROR.to_string()))
        }
        fn fips(&self) -> bool {
            true
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn uses_installed_crypto_provider() {
        // We need a type with a static lifetime because of the constraints on
        // `PrivateKeyDer`.
        static FAKE_KEY_PROVIDER: FakeKeyProvider = FakeKeyProvider {};

        // It is easier to grab some `CryptoProvider` and replace its
        // `key_provider` than construct a fake `CryptoProvider` from scratch.
        let mut cp = rustls::crypto::ring::default_provider();
        cp.key_provider = &FAKE_KEY_PROVIDER;

        // Install our custom `CryptoProvider`.
        //
        // Note that this can only be called once **per process**. That is why
        // we isolate this test into its own binary. Adding other tests to this
        // binary will use the fake (and faulty!) provider we just installed.
        CryptoProvider::install_default(cp).unwrap();

        // Try to use the service account credentials. This calls into the
        // custom crypto provider.
        let creds = test_service_account_credentials().await;
        let err = creds.headers(Extensions::new()).await.unwrap_err();
        assert!(!err.is_transient(), "{err:?}");
        let source = err.source().and_then(|e| e.downcast_ref::<rustls::Error>());
        assert!(
            matches!(source, Some(rustls::Error::General(m)) if m == CUSTOM_ERROR),
            "display={err}, debug={err:?}"
        );
    }
}
