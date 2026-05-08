// Copyright 2026 Google LLC
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

use rustls::crypto::CryptoProvider;

pub(crate) fn get_key_provider() -> &'static dyn rustls::crypto::KeyProvider {
    let key_provider = CryptoProvider::get_default().map(|p| p.key_provider);
    #[cfg(feature = "default-rustls-provider")]
    let key_provider =
        key_provider.unwrap_or_else(|| rustls::crypto::aws_lc_rs::default_provider().key_provider);

    #[cfg(not(feature = "default-rustls-provider"))]
    let key_provider = key_provider.expect(
        r###"
The default rustls::CryptoProvider should be configured by the application. The
`google-cloud-auth` crate was compiled without the `default-rustls-provider`
feature. Without this feature the crate expects the application to initialize
the rustls crypto provider using `rustls::CryptoProvider::install_default()`.

Note that the application must use the exact same version of `rustls` as the
`google-cloud-auth` crate does. Otherwise `install_default()` has no effect."###,
    );

    key_provider
}
