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

#[cfg(test)]
mod tests {
    use gax::error::binding::*;
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
    use google_cloud_storage as gcs;
    use std::error::Error as _;

    #[tokio::test]
    async fn useful_binding_error() -> anyhow::Result<()> {
        let client = gcs::client::StorageControl::builder()
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        // ```
        // option (google.api.routing) = {
        //   routing_parameters {
        //     field: "name"
        //     path_template: "{bucket=**}"
        //   }
        // };
        // ```
        let e = client
            .delete_bucket()
            .send()
            .await
            .expect_err("Should fail locally with a binding error.");
        assert!(e.is_binding(), "{e:?}");
        let got = e
            .source()
            .and_then(|e| e.downcast_ref::<BindingError>())
            .expect("should be a BindingError");
        let want = PathMismatch {
            subs: vec![SubstitutionMismatch {
                field_name: "name",
                problem: SubstitutionFail::UnsetExpecting("**"),
            }],
        };
        assert!(got.paths.contains(&want), "got: {got:?}, want: {want:?}");

        // ```
        // option (google.api.routing) = {
        //   routing_parameters {
        //     field: "name"
        //     path_template: "{bucket=projects/*/buckets/*}/**"
        //   }
        // };
        // ```
        let e = client
            .delete_folder()
            .send()
            .await
            .expect_err("Should fail locally with a binding error.");
        assert!(e.is_binding(), "{e:?}");
        assert!(e.source().is_some(), "{e:?}");
        let got = e
            .source()
            .and_then(|e| e.downcast_ref::<BindingError>())
            .expect("should be a BindingError");
        let want = PathMismatch {
            subs: vec![SubstitutionMismatch {
                field_name: "name",
                problem: SubstitutionFail::UnsetExpecting("projects/*/buckets/*/**"),
            }],
        };
        assert!(got.paths.contains(&want), "got: {got:?}, want: {want:?}");

        Ok(())
    }

    #[tokio::test]
    async fn binding_error_or() -> anyhow::Result<()> {
        let client = gcs::client::StorageControl::builder()
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        // ```
        // option (google.api.routing) = {
        //   routing_parameters {
        //     field: "resource"
        //     path_template: "{bucket=**}"
        //   }
        //   routing_parameters {
        //     field: "resource"
        //     path_template: "{bucket=projects/*/buckets/*}/**"
        //   }
        // };
        // ```
        let e = client
            .set_iam_policy()
            .send()
            .await
            .expect_err("Should fail locally with a binding error.");
        assert!(e.is_binding(), "{e:?}");
        let got = e
            .source()
            .and_then(|e| e.downcast_ref::<BindingError>())
            .expect("should be a BindingError");

        // Note that the routing key ("bucket") is the same. for the two
        // `routing_parameters`. We should report the errors in separate paths.
        let want1 = PathMismatch {
            subs: vec![SubstitutionMismatch {
                field_name: "resource",
                problem: SubstitutionFail::UnsetExpecting("projects/*/buckets/*/**"),
            }],
        };
        let want2 = PathMismatch {
            subs: vec![SubstitutionMismatch {
                field_name: "resource",
                problem: SubstitutionFail::UnsetExpecting("**"),
            }],
        };
        assert!(got.paths.contains(&want1), "got: {got:?}, want: {want1:?}");
        assert!(got.paths.contains(&want2), "got: {got:?}, want: {want2:?}");
        Ok(())
    }

    #[tokio::test]
    async fn binding_error_and() -> anyhow::Result<()> {
        let client = gcs::client::StorageControl::builder()
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        // ```
        // option (google.api.routing) = {
        //   routing_parameters {
        //     field: "source_bucket"
        //   }
        //   routing_parameters {
        //     field: "destination_bucket"
        //     path_template: "{bucket=**}"
        //   }
        // };
        // ```
        let e = client
            .rewrite_object()
            .send()
            .await
            .expect_err("Should fail locally with a binding error.");
        assert!(e.is_binding(), "{e:?}");
        let got = e
            .source()
            .and_then(|e| e.downcast_ref::<BindingError>())
            .expect("should be a BindingError");

        // Note that the routing key differs for the two `routing_parameters`.
        // We should report substitution errors in a single path.
        let want = PathMismatch {
            subs: vec![
                SubstitutionMismatch {
                    field_name: "destination_bucket",
                    problem: SubstitutionFail::UnsetExpecting("**"),
                },
                SubstitutionMismatch {
                    field_name: "source_bucket",
                    problem: SubstitutionFail::UnsetExpecting("**"),
                },
            ],
        };
        assert!(got.paths.contains(&want), "got: {got:?}, want: {want:?}");
        Ok(())
    }
}
