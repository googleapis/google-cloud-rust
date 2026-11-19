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
    use gcs::client::StorageControl;
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
    use google_cloud_gax::error::binding::*;
    use google_cloud_gax::error::rpc::Code;
    use google_cloud_gax::retry_policy::NeverRetry;
    use google_cloud_storage as gcs;
    use httptest::{Expectation, Server, all_of, matchers::*, responders::status_code};
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

    async fn control_client(server: &Server) -> anyhow::Result<StorageControl> {
        Ok(StorageControl::builder()
            .with_endpoint(format!("http://{}", server.addr()))
            .with_credentials(Anonymous::new().build())
            .with_retry_policy(NeverRetry)
            .build()
            .await?)
    }

    fn assert_folder_mismatch(err: google_cloud_gax::error::Error, input: &str) {
        assert!(err.is_binding(), "{err:?}");
        let got = err
            .source()
            .and_then(|e| e.downcast_ref::<BindingError>())
            .expect("should be a BindingError");
        let want = PathMismatch {
            subs: vec![SubstitutionMismatch {
                field_name: "name",
                problem: SubstitutionFail::MismatchExpecting(
                    input.into(),
                    "projects/*/buckets/*/**",
                ),
            }],
        };
        assert!(got.paths.contains(&want), "got: {got:?}, want: {want:?}");
    }

    #[tokio::test]
    async fn explicit_routing_boundary_error() -> anyhow::Result<()> {
        let server = Server::run();
        let client = control_client(&server).await?;

        for input in ["projects/p/buckets/", "projects/p/folders/f"] {
            let err = client
                .get_folder()
                .set_name(input)
                .send()
                .await
                .expect_err("Should fail locally with a binding error.");
            assert_folder_mismatch(err, input);

            let err = client
                .delete_folder()
                .set_name(input)
                .send()
                .await
                .expect_err("Should fail locally with a binding error.");
            assert_folder_mismatch(err, input);
        }

        Ok(())
    }

    fn expect_routing_header(server: &Server, path: &'static str, request_params: &'static str) {
        server.expect(
            Expectation::matching(all_of![
                request::method_path("POST", path),
                request::headers(contains(("x-goog-request-params", request_params))),
            ])
            .times(1)
            .respond_with(
                status_code(200)
                    .append_header("content-type", "application/grpc")
                    .append_header("grpc-status", "12")
                    .append_header("grpc-message", "unimplemented"),
            ),
        );
    }

    fn assert_unimplemented<T: std::fmt::Debug>(result: google_cloud_gax::Result<T>) {
        let err = result.expect_err("mock gRPC server should return Unimplemented");
        assert_eq!(
            err.status().map(|s| s.code),
            Some(Code::Unimplemented),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn explicit_routing_wire_headers() -> anyhow::Result<()> {
        let server = Server::run();
        let client = control_client(&server).await?;

        let folder_cases = [
            (
                "projects/p/buckets/b-folder/folders/f",
                "bucket=projects%2Fp%2Fbuckets%2Fb-folder",
            ),
            (
                "projects/p/buckets/b-exact",
                "bucket=projects%2Fp%2Fbuckets%2Fb-exact",
            ),
        ];
        for (name, want_header) in folder_cases {
            expect_routing_header(
                &server,
                "/google.storage.control.v2.StorageControl/GetFolder",
                want_header,
            );
            assert_unimplemented(client.get_folder().set_name(name).send().await);
        }

        let iam_perm_cases = [
            (
                "projects/p/buckets/b-obj/objects/dir/obj",
                "bucket=projects%2Fp%2Fbuckets%2Fb-obj",
            ),
            (
                "projects/p/buckets/b-mf/managedFolders/mf",
                "bucket=projects%2Fp%2Fbuckets%2Fb-mf",
            ),
            (
                "projects/p/buckets/b-colon/objects:custom",
                "bucket=projects%2Fp%2Fbuckets%2Fb-colon",
            ),
            (
                "projects/p/buckets/b-obj/objects-extra",
                "bucket=projects%2Fp%2Fbuckets%2Fb-obj%2Fobjects-extra",
            ),
        ];
        for (resource, want_header) in iam_perm_cases {
            expect_routing_header(
                &server,
                "/google.storage.control.v2.StorageControl/TestIamPermissions",
                want_header,
            );
            assert_unimplemented(
                client
                    .test_iam_permissions()
                    .set_resource(resource)
                    .send()
                    .await,
            );
        }

        expect_routing_header(
            &server,
            "/google.storage.control.v2.StorageControl/GetIamPolicy",
            "bucket=buckets%2Flegacy-b",
        );
        assert_unimplemented(
            client
                .get_iam_policy()
                .set_resource("buckets/legacy-b")
                .send()
                .await,
        );

        Ok(())
    }
}
