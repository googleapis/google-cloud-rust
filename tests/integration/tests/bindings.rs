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

/// This module contains "generated" code for testing additional binding logic.
///
/// The code was generated off of this source proto:
///
/// ```norust
/// syntax = "proto3";
/// package binding.dev;
///
/// import "google/api/annotations.proto";
///
/// service TestService {
///   rpc DoFoo(Request) returns (Response) {
///     option (google.api.http) = {
///       post: "/v1/{name=projects/*/locations/*}:first"
///       additional_bindings {
///         post: "/v1/projects/{project}/locations/{location}/ids/{id}:additionalBinding"
///       }
///       additional_bindings {
///         get: "/v1/projects/{child.project}/locations/{child.location}/ids/{child.id}:additionalBindingOnChild"
///       }
///     };
///   }
/// }
///
/// message Request {
///     string name = 1;
///     string project = 2;
///     string location = 3;
///     uint64 id = 4;
///     optional string optional = 5;
///     Request child = 6;
/// }
///
/// message Response {}
/// ```
mod bindings {
    use gax::error::{Error, binding::BindingError};
    use gaxi::path_parameter::{PathMismatchBuilder, try_match};
    use gaxi::routing_parameter::Segment;
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;

    /// A stand in for a generated request message.
    #[derive(Default, serde::Serialize)]
    pub struct Request {
        pub name: String,
        pub project: String,
        pub location: String,
        pub id: u64,
        pub optional: Option<String>,
        pub child: Option<Box<Request>>,
    }

    /// A stand in for a generated service stub.
    pub struct TestService {
        inner: gaxi::http::ReqwestClient,
    }

    impl TestService {
        pub async fn new() -> Self {
            let mut config = gaxi::options::ClientConfig::default();
            config.cred = Anonymous::new().build().into();
            let inner = gaxi::http::ReqwestClient::new(config, "https://test.googleapis.com")
                .await
                .expect("test credentials can never fail");
            Self { inner }
        }

        /// Once-generated code that produces a `reqwest::RequestBuilder`
        ///
        /// The code was copied exactly from `transport.rs`.
        ///
        /// TODO(#2523) - have the generator own this code, so it stays in sync.
        pub fn builder(&self, req: Request) -> gax::Result<reqwest::RequestBuilder> {
            let builder = None
                .or_else(|| {
                    let path = format!(
                        "/v1/{}:first",
                        try_match(
                            Some(&req).map(|m| &m.name).map(|s| s.as_str()),
                            &[
                                Segment::Literal("projects/"),
                                Segment::SingleWildcard,
                                Segment::Literal("/locations/"),
                                Segment::SingleWildcard
                            ]
                        )?,
                    );

                    let builder = (|| {
                        let builder = self.inner.builder(reqwest::Method::POST, path);
                        let builder = builder.query(&[("project", &req.project)]);
                        let builder = builder.query(&[("location", &req.location)]);
                        let builder = builder.query(&[("id", &req.id)]);
                        let builder = req
                            .optional
                            .iter()
                            .fold(builder, |builder, p| builder.query(&[("optional", p)]));
                        let builder = req
                            .child
                            .as_ref()
                            .map(|p| serde_json::to_value(p).map_err(Error::ser))
                            .transpose()?
                            .into_iter()
                            .fold(builder, |builder, v| {
                                use gaxi::query_parameter::QueryParameter;
                                v.add(builder, "child")
                            });
                        Ok(builder)
                    })();
                    Some(builder)
                })
                .or_else(|| {
                    let path = format!(
                        "/v1/projects/{}/locations/{}/ids/{}:additionalBinding",
                        try_match(
                            Some(&req).map(|m| &m.project).map(|s| s.as_str()),
                            &[Segment::SingleWildcard]
                        )?,
                        try_match(
                            Some(&req).map(|m| &m.location).map(|s| s.as_str()),
                            &[Segment::SingleWildcard]
                        )?,
                        try_match(Some(&req).map(|m| &m.id), &[Segment::SingleWildcard])?,
                    );

                    let builder = (|| {
                        let builder = self.inner.builder(reqwest::Method::POST, path);
                        let builder = builder.query(&[("name", &req.name)]);
                        let builder = req
                            .optional
                            .iter()
                            .fold(builder, |builder, p| builder.query(&[("optional", p)]));
                        let builder = req
                            .child
                            .as_ref()
                            .map(|p| serde_json::to_value(p).map_err(Error::ser))
                            .transpose()?
                            .into_iter()
                            .fold(builder, |builder, v| {
                                use gaxi::query_parameter::QueryParameter;
                                v.add(builder, "child")
                            });
                        Ok(builder)
                    })();
                    Some(builder)
                })
                .or_else(|| {
                    let path = format!(
                        "/v1/projects/{}/locations/{}/ids/{}:additionalBindingOnChild",
                        try_match(
                            Some(&req)
                                .and_then(|m| m.child.as_ref())
                                .map(|m| &m.project)
                                .map(|s| s.as_str()),
                            &[Segment::SingleWildcard]
                        )?,
                        try_match(
                            Some(&req)
                                .and_then(|m| m.child.as_ref())
                                .map(|m| &m.location)
                                .map(|s| s.as_str()),
                            &[Segment::SingleWildcard]
                        )?,
                        try_match(
                            Some(&req).and_then(|m| m.child.as_ref()).map(|m| &m.id),
                            &[Segment::SingleWildcard]
                        )?,
                    );

                    let builder = (|| {
                        let builder = self.inner.builder(reqwest::Method::GET, path);
                        let builder = builder.query(&[("name", &req.name)]);
                        let builder = builder.query(&[("project", &req.project)]);
                        let builder = builder.query(&[("location", &req.location)]);
                        let builder = builder.query(&[("id", &req.id)]);
                        let builder = req
                            .optional
                            .iter()
                            .fold(builder, |builder, p| builder.query(&[("optional", p)]));
                        let builder = req
                            .child
                            .as_ref()
                            .map(|p| serde_json::to_value(p).map_err(Error::ser))
                            .transpose()?
                            .into_iter()
                            .fold(builder, |builder, v| {
                                use gaxi::query_parameter::QueryParameter;
                                v.add(builder, "child")
                            });
                        Ok(builder)
                    })();
                    Some(builder)
                })
                .ok_or_else(|| {
                    let mut paths = Vec::new();
                    {
                        let builder = PathMismatchBuilder::default();
                        let builder = builder.maybe_add(
                            Some(&req).map(|m| &m.name).map(|s| s.as_str()),
                            &[
                                Segment::Literal("projects/"),
                                Segment::SingleWildcard,
                                Segment::Literal("/locations/"),
                                Segment::SingleWildcard,
                            ],
                            "name",
                            "projects/*/locations/*",
                        );
                        paths.push(builder.build());
                    }
                    {
                        let builder = PathMismatchBuilder::default();
                        let builder = builder.maybe_add(
                            Some(&req).map(|m| &m.project).map(|s| s.as_str()),
                            &[Segment::SingleWildcard],
                            "project",
                            "*",
                        );
                        let builder = builder.maybe_add(
                            Some(&req).map(|m| &m.location).map(|s| s.as_str()),
                            &[Segment::SingleWildcard],
                            "location",
                            "*",
                        );
                        let builder = builder.maybe_add(
                            Some(&req).map(|m| &m.id),
                            &[Segment::SingleWildcard],
                            "id",
                            "*",
                        );
                        paths.push(builder.build());
                    }
                    {
                        let builder = PathMismatchBuilder::default();
                        let builder = builder.maybe_add(
                            Some(&req)
                                .and_then(|m| m.child.as_ref())
                                .map(|m| &m.project)
                                .map(|s| s.as_str()),
                            &[Segment::SingleWildcard],
                            "child.project",
                            "*",
                        );
                        let builder = builder.maybe_add(
                            Some(&req)
                                .and_then(|m| m.child.as_ref())
                                .map(|m| &m.location)
                                .map(|s| s.as_str()),
                            &[Segment::SingleWildcard],
                            "child.location",
                            "*",
                        );
                        let builder = builder.maybe_add(
                            Some(&req).and_then(|m| m.child.as_ref()).map(|m| &m.id),
                            &[Segment::SingleWildcard],
                            "child.id",
                            "*",
                        );
                        paths.push(builder.build());
                    }
                    gax::error::Error::binding(BindingError { paths })
                })??;

            Ok(builder)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::bindings::*;
    use anyhow::Result;
    use gax::error::binding::{BindingError, PathMismatch, SubstitutionFail, SubstitutionMismatch};
    use std::collections::HashSet;
    use std::error::Error as _;

    #[tokio::test]
    async fn first_match_wins() -> Result<()> {
        let stub = TestService::new().await;
        let request = Request {
            name: "projects/p/locations/l".to_string(),
            project: "ignored-p".to_string(),
            location: "ignored-l".to_string(),
            id: 12345,
            ..Default::default()
        };
        let builder = stub.builder(request)?;

        let reqwest = builder.build()?;
        assert_eq!(reqwest.method(), reqwest::Method::POST);

        let url = reqwest.url();
        assert_eq!(url.path(), "/v1/projects/p/locations/l:first");

        let actual_qps: HashSet<String> =
            url.query_pairs().map(|(key, _)| key.into_owned()).collect();
        let want_qps: HashSet<String> = ["project", "location", "id"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert_eq!(actual_qps, want_qps);

        Ok(())
    }

    #[tokio::test]
    async fn additional_binding_used() -> Result<()> {
        let stub = TestService::new().await;
        let request = Request {
            name: "does-not-match-the-template".to_string(),
            project: "p".to_string(),
            location: "l".to_string(),
            id: 12345,
            ..Default::default()
        };
        let builder = stub.builder(request)?;

        let reqwest = builder.build()?;
        assert_eq!(reqwest.method(), reqwest::Method::POST);

        let url = reqwest.url();
        assert_eq!(
            url.path(),
            "/v1/projects/p/locations/l/ids/12345:additionalBinding"
        );

        // Verify we use the query parameters associated with
        // `:additionalBinding`, not `:first`
        let actual_qps: HashSet<String> =
            url.query_pairs().map(|(key, _)| key.into_owned()).collect();
        let want_qps: HashSet<String> = ["name".to_string()].into();
        assert_eq!(actual_qps, want_qps);

        Ok(())
    }

    #[tokio::test]
    async fn additional_binding_on_child() -> Result<()> {
        let stub = TestService::new().await;
        let request = Request {
            child: Some(Box::new(Request {
                project: "p".to_string(),
                location: "l".to_string(),
                id: 12345,
                ..Default::default()
            })),
            ..Default::default()
        };
        let builder = stub.builder(request)?;

        let reqwest = builder.build()?;
        assert_eq!(reqwest.method(), reqwest::Method::GET);

        let url = reqwest.url();
        assert_eq!(
            url.path(),
            "/v1/projects/p/locations/l/ids/12345:additionalBindingOnChild"
        );

        Ok(())
    }

    #[tokio::test]
    async fn no_bindings() -> Result<()> {
        let stub = TestService::new().await;
        let request = Request {
            // name: unset!!!
            project: "does/not/match/the/template".to_string(),
            location: "l".to_string(),
            // child: also unset!!!
            ..Default::default()
        };
        let e = stub
            .builder(request)
            .expect_err("Binding validation should fail");

        assert!(e.is_binding(), "{e:?}");
        assert!(e.source().is_some(), "{e:?}");
        let got = e
            .source()
            .and_then(|e| e.downcast_ref::<BindingError>())
            .expect("should be a BindingError");

        let want = BindingError {
            paths: vec![
                PathMismatch {
                    subs: vec![SubstitutionMismatch {
                        field_name: "name",
                        problem: SubstitutionFail::UnsetExpecting("projects/*/locations/*"),
                    }],
                },
                PathMismatch {
                    subs: vec![SubstitutionMismatch {
                        field_name: "project",
                        problem: SubstitutionFail::MismatchExpecting(
                            "does/not/match/the/template".to_string(),
                            "*",
                        ),
                    }],
                },
                PathMismatch {
                    subs: vec![
                        SubstitutionMismatch {
                            field_name: "child.project",
                            problem: SubstitutionFail::UnsetExpecting("*"),
                        },
                        SubstitutionMismatch {
                            field_name: "child.location",
                            problem: SubstitutionFail::UnsetExpecting("*"),
                        },
                        SubstitutionMismatch {
                            field_name: "child.id",
                            problem: SubstitutionFail::Unset,
                        },
                    ],
                },
            ],
        };
        assert_eq!(got, &want);
        Ok(())
    }
}
