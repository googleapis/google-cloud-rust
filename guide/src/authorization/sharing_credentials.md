<!-- 
Copyright 2025 Google LLC
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    https://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Sharing Credentials

In concurrent applications, it is often necessary to share credentials across
multiple asynchronous tasks or threads.

The guide shows you how to construct the `Credentials` object and then clone it
for each concurrent task. The `Credentials` object is designed for efficient
cloning and handles the background refreshing of authentication tokens
automatically.

```rust
let credentials = Builder::default().build()?;
tokio::spawn(async move {
    do_some_work_with_apis(credentials.clone(), endpoint, ...).await;
});
```

Cloning the `Credentials` object is more performant than repeatedly creating new
credentials, as it avoids the overhead associated with credential construction.
This approach also ensures consistency and simplifies credential management
within the application.
