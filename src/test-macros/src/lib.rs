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

use proc_macro::TokenStream;
use quote::quote;
use syn::{ItemFn, parse_macro_input};

/// A shorthand for tests that should fail on background panics.
///
/// In our tests, we commonly pass mocks into background tasks. While the mocks panic on unmet
/// expectations, by default, the panics are consumed by the JoinHandle.
///
/// Tokio can be configured to shutdown the runtime on panics in background
/// tasks. But this is an unstable feature, and requires compiling with
/// `--cfg tokio_unstable`.
///
/// See: <https://github.com/googleapis/google-cloud-rust/issues/4733>
#[proc_macro_attribute]
pub fn tokio_test_no_panics(args: TokenStream, input: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(input as ItemFn);
    let args = proc_macro2::TokenStream::from(args);

    let comma = if !args.is_empty() {
        quote! { , }
    } else {
        quote! {}
    };

    let expanded = quote! {
        #[cfg_attr(
            tokio_unstable,
            tokio::test(#args #comma unhandled_panic = "shutdown_runtime")
        )]
        #[cfg_attr(
            not(tokio_unstable),
            tokio::test(#args)
        )]
        #input_fn
    };

    TokenStream::from(expanded)
}
