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

use gcp_sdk_gax::options::{RequestOptions, RequestTimeout, UserAgentPrefix};
use std::time::Duration;

#[test]
fn test_setall() {
    let options = RequestOptions::new().set::<UserAgentPrefix>("myapp/4.5.6");
    assert_eq!(
        options.get::<UserAgentPrefix>(),
        Some("myapp/4.5.6".to_string())
    );
    assert_eq!(options.get::<RequestTimeout>(), None);

    let options = options.extend(
        RequestOptions::new()
            .set::<RequestTimeout>(Duration::from_secs(123))
            .set::<UserAgentPrefix>("myapp/3.4.5"),
    );
    assert_eq!(
        options.get::<RequestTimeout>(),
        Some(Duration::from_secs(123))
    );
    assert_eq!(
        options.get::<UserAgentPrefix>(),
        Some("myapp/3.4.5".to_string())
    );
}
