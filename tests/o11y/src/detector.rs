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

//! A Google Cloud resource detector.
//!
//! This module contains a [resource detector] for Google Cloud. Use this
//! early in your application to determine if your application is deployed to a
//! Google Cloud environment, and the detailed information of this deployment.
//!
//! # Example
//! ```
//! use integration_tests_o11y::detector::GoogleCloudResourceDetector;
//! use integration_tests_o11y::otlp::trace::Builder;
//! # async fn sample() -> anyhow::Result<()> {
//! let detector = GoogleCloudResourceDetector::builder().build().await?;
//! // Use `resource` to initialize the exporters. For example:
//! let provider = Builder::new("my-project", "my-service")
//!     .with_detector(detector)
//!     .build()
//!     .await?;
//! # Ok(()) }
//! ```
//!
//! [resource detector]: https://opentelemetry.io/docs/concepts/resources/#resource-detectors

use opentelemetry::KeyValue;
use opentelemetry_sdk::resource::{Resource, ResourceBuilder, ResourceDetector};
use std::collections::BTreeMap;
use std::time::Duration;

const METADATA_ROOT: &str = "http://metadata.google.internal";
const GCE_METADATA_HOST_ENV_VAR: &str = "GCE_METADATA_HOST";
const DEFAULT_ATTEMPT_TIMEOUT: Duration = Duration::from_millis(100);
const DEFAULT_ATTEMPT_COUNT: u32 = 5;
const INSTANCE_METADATA_PATH: &str = "/computeMetadata/v1/instance/";

/// Detects if the application is running in a Google Cloud environment.
///
/// Detects if the application is running on [Google Compute Engine] (GCE),
/// [Google Kubernetes Engine] (GKE), [Cloud Run], or
/// [Google Application Engine] (GAE).
///
/// GAE is unlikely, as there is no Rust runtime for it. Though applications
/// could deploy with Rust embedded in a language supported by the runtime.
#[derive(Clone, Debug)]
pub struct GoogleCloudResourceDetector(Resource);

impl GoogleCloudResourceDetector {
    pub fn builder() -> GoogleCloudResourceDetectorBuilder {
        GoogleCloudResourceDetectorBuilder::new()
    }
}

impl ResourceDetector for GoogleCloudResourceDetector {
    fn detect(&self) -> Resource {
        self.0.clone()
    }
}

#[derive(Debug, Default)]
pub struct GoogleCloudResourceDetectorBuilder {
    endpoint: String,
    attempt_timeout: Duration,
    attempt_count: u32,
    fallback: Option<Resource>,
}

impl GoogleCloudResourceDetectorBuilder {
    fn new() -> Self {
        Self {
            endpoint: std::env::var(GCE_METADATA_HOST_ENV_VAR)
                .unwrap_or_else(|_| METADATA_ROOT.to_string()),
            attempt_count: DEFAULT_ATTEMPT_COUNT,
            attempt_timeout: DEFAULT_ATTEMPT_TIMEOUT,
            fallback: None,
        }
    }

    pub async fn build(self) -> Result<GoogleCloudResourceDetector, Error> {
        let resource = self.detect_async().await;
        let resource = match (resource, self.fallback) {
            (Ok(r), _) => r,
            (Err(_), Some(r)) => r,
            (Err(e), None) => return Err(e),
        };
        Ok(GoogleCloudResourceDetector(resource))
    }

    pub async fn detect_async(&self) -> Result<Resource, Error> {
        let builder =
            Resource::builder_empty().with_attribute(KeyValue::new("cloud.provider", "gcp"));
        let builder = if std::env::var("KUBERNETES_SERVICE_HOST").is_ok() {
            self.gke_resource(builder).await?
        } else if std::env::var("GAE_SERVICE").is_ok() {
            self.gae_resource(builder)
        } else if std::env::var("K_SERVICE").is_ok() {
            self.gcr_resource(builder)
        } else {
            self.gce_resource(builder).await?
        };
        Ok(builder.build())
    }

    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = endpoint.into();
        self
    }

    pub fn with_attempt_timeout(mut self, timeout: Duration) -> Self {
        self.attempt_timeout = timeout;
        self
    }

    pub fn with_attempt_count(mut self, count: u32) -> Self {
        self.attempt_count = count;
        self
    }

    pub fn with_fallback(mut self, fallback: Resource) -> Self {
        self.fallback = Some(fallback);
        self
    }

    async fn gke_resource(&self, builder: ResourceBuilder) -> Result<ResourceBuilder, Error> {
        let text = self.fetch_instance_metadata().await?;
        let instance = serde_json::from_str::<InstanceMetadata>(&text).map_err(Error::mds)?;
        let cluster_name = instance
            .attributes
            .get("cluster-name")
            .map(|v| v.to_string());

        let mut builder = self.gce_resource_impl(builder, instance)?;
        if let Some(v) = cluster_name {
            builder = builder.with_attribute(KeyValue::new("k8s.cluster.name", v));
        }

        Ok(Self::attributes_from_env(
            builder,
            &[
                ("POD_NAME", "k8s.pod.name"),
                ("HOSTNAME", "k8s.pod.name"),
                ("CONTAINER_NAME", "k8s.container.name"),
                ("NAMESPACE_NAME", "k8s.namespace.name"),
            ],
        ))
    }

    async fn gce_resource(&self, builder: ResourceBuilder) -> Result<ResourceBuilder, Error> {
        let text = self.fetch_instance_metadata().await?;
        let instance = serde_json::from_str::<InstanceMetadata>(&text).map_err(Error::mds)?;
        self.gce_resource_impl(builder, instance)
    }

    fn gce_resource_impl<'a>(
        &self,
        builder: ResourceBuilder,
        instance: InstanceMetadata<'a>,
    ) -> Result<ResourceBuilder, Error> {
        // The zone is is projects/{project_id}/zones/{zone_id} format.
        let (region, zone) = if let Some(name) = instance.zone {
            parse_zone(name)
        } else {
            (None, None)
        };

        let builder = builder.with_attributes(
            [
                ("cloud.availability_zone", zone),
                ("cloud.region", region),
                ("gce.instance_id", instance.id),
                ("gce.instance_name", instance.name),
                ("gce.machine_type", instance.machine_type),
            ]
            .into_iter()
            .filter_map(|(k, option)| option.map(|v| (k, v)))
            .map(|(k, v)| KeyValue::new(k, v.to_string())),
        );
        Ok(builder)
    }

    fn gae_resource(&self, builder: ResourceBuilder) -> ResourceBuilder {
        Self::attributes_from_env(
            builder,
            &[
                ("GAE_SERVICE", "gae.service"),
                ("GAE_VERSION", "gae.version"),
                ("GAE_INSTANCE", "gae.instance"),
            ],
        )
    }

    fn gcr_resource(&self, builder: ResourceBuilder) -> ResourceBuilder {
        Self::attributes_from_env(
            builder,
            &[
                ("K_SERVICE", "gcr.service"),
                ("K_REVISION", "gcr.revision"),
                ("K_CONFIGURATION", "gcr.configuration"),
            ],
        )
    }

    fn attributes_from_env(
        builder: ResourceBuilder,
        list: &[(&'static str, &'static str)],
    ) -> ResourceBuilder {
        list.iter().fold(builder, |builder, (var, name)| {
            if let Ok(value) = std::env::var(var) {
                builder.with_attribute(KeyValue::new(*name, value))
            } else {
                builder
            }
        })
    }

    async fn fetch_instance_metadata(&self) -> Result<String, Error> {
        let url = reqwest::Url::parse(&self.endpoint)
            .map_err(Error::url)?
            .join(INSTANCE_METADATA_PATH)
            .map_err(Error::url)?;
        let mut last_error = None;
        for iteration in 0..self.attempt_count {
            if iteration != 0 {
                tokio::time::sleep(self.attempt_timeout).await;
            }
            match self.fetch_metadata_attempt(url.clone()).await {
                Ok(s) => return Ok(s),
                Err(e) => {
                    last_error = Some(e);
                }
            }
        }
        Err(last_error.unwrap())
    }

    async fn fetch_metadata_attempt(&self, url: reqwest::Url) -> Result<String, Error> {
        // Use a new client on each attempt, we want to discard any connections
        // that failed with 429 or similar errors.
        reqwest::Client::new()
            .request(reqwest::Method::GET, url)
            .header("Metadata-Flavor", "Google")
            .query(&[("recursive", "true")])
            .timeout(self.attempt_timeout)
            .send()
            .await
            .map_err(Error::mds)?
            .error_for_status()
            .map_err(Error::mds)?
            .text()
            .await
            .map_err(Error::mds)
    }
}

#[derive(Debug, serde::Deserialize)]
struct InstanceMetadata<'a> {
    zone: Option<&'a str>,
    id: Option<&'a str>,
    name: Option<&'a str>,
    #[serde(rename = "machine-type")]
    machine_type: Option<&'a str>,
    attributes: BTreeMap<&'a str, &'a str>,
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error("cannot parse endpoint: {0:?}")]
    Url(#[source] BoxedError),
    #[error("cannot retrieve data from metadata server: {0:?}")]
    Mds(#[source] BoxedError),
    #[error("cannot parse data received from metadata server: {0:?}")]
    Parse(#[source] BoxedError),
}

impl Error {
    fn url<E>(error: E) -> Self
    where
        E: Into<BoxedError>,
    {
        Error::Url(error.into())
    }

    fn mds<E>(error: E) -> Self
    where
        E: Into<BoxedError>,
    {
        Error::Mds(error.into())
    }
}

type BoxedError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Parses a zone in `projects/{project_id}/zones/{zone_id}` format into the
/// region and zone.
fn parse_zone(zone: &str) -> (Option<&str>, Option<&str>) {
    let parts: Vec<&str> = zone.split('/').collect();
    let id = match &parts[..] {
        ["projects", _, "zones", zone_id] => *zone_id,
        _ => return (None, None),
    };
    let parts: Vec<&str> = id.split('-').collect();
    match &parts[..] {
        [_geo, _region, letter] if !letter.is_empty() => {
            (Some(&id[0..(id.len() - letter.len() - 1)]), Some(id))
        }
        _ => (None, Some(id)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use httptest::{Expectation, Server, matchers::*, responders::*};
    use scoped_env::ScopedEnv;
    use serial_test::{parallel, serial};
    use std::borrow::Cow;
    use std::collections::BTreeMap;
    use test_case::test_case;

    const MOCK_METADATA: &str = r#"{
        "zone": "projects/p/zones/us-central1-c",
        "id": "test-id",
        "name": "test-name",
        "machine-type": "c4-standard-192",
        "attributes": {
            "cluster-name": "test-cluster"
        }
    }"#;

    const GCE_WANT: [(&str, &str); 5] = [
        ("cloud.availability_zone", "us-central1-c"),
        ("cloud.region", "us-central1"),
        ("gce.instance_id", "test-id"),
        ("gce.instance_name", "test-name"),
        ("gce.machine_type", "c4-standard-192"),
    ];

    #[tokio::test]
    #[serial]
    async fn detect() -> anyhow::Result<()> {
        let _k = ScopedEnv::remove("KUBERNETES_SERVICE_HOST");
        let _g = ScopedEnv::remove("GAE_SERVICE");
        let _r = ScopedEnv::remove("K_SERVICE");
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::method_path("GET", INSTANCE_METADATA_PATH),
                request::query(url_decoded(contains(("recursive", "true")))),
            ])
            .respond_with(status_code(200).body(MOCK_METADATA)),
        );
        let detector = GoogleCloudResourceDetector::builder()
            .with_endpoint(server.url("").to_string())
            .build()
            .await?;
        let resource = detector.detect();
        let got = BTreeMap::from_iter(resource.iter().map(|(k, v)| (k.as_str(), v.as_str())));
        let want = BTreeMap::from_iter(
            GCE_WANT
                .iter()
                .chain([("cloud.provider", "gcp")].iter())
                .map(|(k, v)| (*k, Cow::from(*v))),
        );
        assert_eq!(got, want, "{resource:?}");
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn detect_async_gke_pod_name() -> anyhow::Result<()> {
        let _k = ScopedEnv::set("KUBERNETES_SERVICE_HOST", "--test-only--");
        let _g = ScopedEnv::remove("GAE_SERVICE");
        let _k = ScopedEnv::remove("K_SERVICE");
        let _h = ScopedEnv::remove("HOSTNAME");
        let _p = ScopedEnv::set("POD_NAME", "test-pod-name");
        let _c = ScopedEnv::set("CONTAINER_NAME", "test-container-name");
        let _n = ScopedEnv::set("NAMESPACE_NAME", "test-namespace-name");
        let (_server, detector) = success_setup();
        let resource = detector.detect_async().await?;
        let got = BTreeMap::from_iter(resource.iter().map(|(k, v)| (k.as_str(), v.as_str())));
        let want = BTreeMap::from_iter(
            GCE_WANT
                .iter()
                .chain(
                    [
                        ("cloud.provider", "gcp"),
                        ("k8s.cluster.name", "test-cluster"),
                        ("k8s.pod.name", "test-pod-name"),
                        ("k8s.container.name", "test-container-name"),
                        ("k8s.namespace.name", "test-namespace-name"),
                    ]
                    .iter(),
                )
                .map(|(k, v)| (*k, Cow::from(*v))),
        );
        assert_eq!(got, want, "{resource:?}");
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn detect_async_gke_hostname() -> anyhow::Result<()> {
        let _k = ScopedEnv::set("KUBERNETES_SERVICE_HOST", "--test-only--");
        let _g = ScopedEnv::remove("GAE_SERVICE");
        let _r = ScopedEnv::remove("K_SERVICE");
        let _p = ScopedEnv::remove("POD_NAME");
        let _h = ScopedEnv::set("HOSTNAME", "test-hostname");
        let _n = ScopedEnv::remove("NAMESPACE_NAME");
        let _c = ScopedEnv::remove("CONTAINER_NAME");
        let (_server, detector) = success_setup();
        let resource = detector.detect_async().await?;
        let got = BTreeMap::from_iter(resource.iter().map(|(k, v)| (k.as_str(), v.as_str())));
        let want = BTreeMap::from_iter(
            GCE_WANT
                .iter()
                .chain(
                    [
                        ("cloud.provider", "gcp"),
                        ("k8s.cluster.name", "test-cluster"),
                        ("k8s.pod.name", "test-hostname"),
                    ]
                    .iter(),
                )
                .map(|(k, v)| (*k, Cow::from(*v))),
        );
        assert_eq!(got, want, "{resource:?}");
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn detect_async_gae() -> anyhow::Result<()> {
        let _k = ScopedEnv::remove("KUBERNETES_SERVICE_HOST");
        let _g = ScopedEnv::set("GAE_SERVICE", "test-only");
        let _r = ScopedEnv::remove("K_SERVICE");
        let detector =
            GoogleCloudResourceDetectorBuilder::new().with_endpoint("http://localhost:1");
        let resource = detector.detect_async().await?;
        let got = BTreeMap::from_iter(resource.iter().map(|(k, v)| (k.as_str(), v.as_str())));
        let want = BTreeMap::from_iter(
            [("cloud.provider", "gcp"), ("gae.service", "test-only")]
                .map(|(k, v)| (k, Cow::from(v))),
        );
        assert_eq!(got, want, "{resource:?}");
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn detect_async_gcr() -> anyhow::Result<()> {
        let _k = ScopedEnv::remove("KUBERNETES_SERVICE_HOST");
        let _g = ScopedEnv::remove("GAE_SERVICE");
        let _r = ScopedEnv::set("K_SERVICE", "test-only");
        let detector =
            GoogleCloudResourceDetectorBuilder::new().with_endpoint("http://localhost:1");
        let resource = detector.detect_async().await?;
        let got = BTreeMap::from_iter(resource.iter().map(|(k, v)| (k.as_str(), v.as_str())));
        let want = BTreeMap::from_iter(
            [("cloud.provider", "gcp"), ("gcr.service", "test-only")]
                .map(|(k, v)| (k, Cow::from(v))),
        );
        assert_eq!(got, want, "{resource:?}");
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn detect_async_gce() -> anyhow::Result<()> {
        let _k = ScopedEnv::remove("KUBERNETES_SERVICE_HOST");
        let _g = ScopedEnv::remove("GAE_SERVICE");
        let _r = ScopedEnv::remove("K_SERVICE");
        let (_server, detector) = success_setup();
        let resource = detector.detect_async().await?;
        let got = BTreeMap::from_iter(resource.iter().map(|(k, v)| (k.as_str(), v.as_str())));
        let want = BTreeMap::from_iter(
            GCE_WANT
                .iter()
                .chain([("cloud.provider", "gcp")].iter())
                .map(|(k, v)| (*k, Cow::from(*v))),
        );
        assert_eq!(got, want, "{resource:?}");
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn gke() -> anyhow::Result<()> {
        let _h = ScopedEnv::remove("HOSTNAME");
        let _p = ScopedEnv::remove("POD_NAME");
        let _c = ScopedEnv::remove("CONTAINER_NAME");
        let _n = ScopedEnv::remove("NAMESPACE_NAME");
        let (_server, detector) = success_setup();
        let builder = Resource::builder_empty();
        let resource = detector.gke_resource(builder).await?.build();
        let got = BTreeMap::from_iter(resource.iter().map(|(k, v)| (k.as_str(), v.as_str())));
        let want = BTreeMap::from_iter(
            GCE_WANT
                .iter()
                .chain([("k8s.cluster.name", "test-cluster")].iter())
                .map(|(k, v)| (*k, Cow::from(*v))),
        );
        assert_eq!(got, want, "{resource:?}");
        Ok(())
    }

    #[tokio::test]
    #[parallel]
    async fn gce() -> anyhow::Result<()> {
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::method_path("GET", INSTANCE_METADATA_PATH),
                request::query(url_decoded(contains(("recursive", "true")))),
            ])
            .respond_with(status_code(200).body(MOCK_METADATA)),
        );

        let detector = GoogleCloudResourceDetectorBuilder::new()
            .with_endpoint(server.url("").to_string())
            .with_attempt_count(3);

        let builder = Resource::builder_empty();
        let resource = detector.gce_resource(builder).await?.build();
        let got = BTreeMap::from_iter(resource.iter().map(|(k, v)| (k.as_str(), v.as_str())));
        let want = BTreeMap::from_iter(GCE_WANT.map(|(k, v)| (k, Cow::from(v))));
        assert_eq!(got, want, "{resource:?}");
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn gae() -> anyhow::Result<()> {
        let _s = ScopedEnv::set("GAE_SERVICE", "test-service");
        let _v = ScopedEnv::set("GAE_VERSION", "test-version");
        let _i = ScopedEnv::set("GAE_INSTANCE", "test-instance");

        let detector =
            GoogleCloudResourceDetectorBuilder::new().with_endpoint("http://localhost:1");
        let builder = Resource::builder_empty();
        let resource = detector.gae_resource(builder).build();
        let got = BTreeMap::from_iter(resource.iter().map(|(k, v)| (k.as_str(), v.as_str())));
        let want = BTreeMap::from_iter(
            [
                ("gae.service", "test-service"),
                ("gae.version", "test-version"),
                ("gae.instance", "test-instance"),
            ]
            .map(|(k, v)| (k, Cow::from(v))),
        );
        assert_eq!(got, want, "{resource:?}");
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn gcr() -> anyhow::Result<()> {
        let _s = ScopedEnv::set("K_SERVICE", "test-service");
        let _r = ScopedEnv::set("K_REVISION", "test-revision");
        let _c = ScopedEnv::set("K_CONFIGURATION", "test-configuration");

        let detector =
            GoogleCloudResourceDetectorBuilder::new().with_endpoint("http://localhost:1");
        let builder = Resource::builder_empty();
        let resource = detector.gcr_resource(builder).build();
        let got = BTreeMap::from_iter(resource.iter().map(|(k, v)| (k.as_str(), v.as_str())));
        let want = BTreeMap::from_iter(
            [
                ("gcr.service", "test-service"),
                ("gcr.revision", "test-revision"),
                ("gcr.configuration", "test-configuration"),
            ]
            .map(|(k, v)| (k, Cow::from(v))),
        );
        assert_eq!(got, want, "{resource:?}");
        Ok(())
    }

    #[tokio::test]
    #[parallel]
    async fn retry_on_429() -> anyhow::Result<()> {
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::method_path("GET", INSTANCE_METADATA_PATH),
                request::query(url_decoded(contains(("recursive", "true")))),
            ])
            .times(3)
            .respond_with(cycle![
                status_code(429),
                status_code(429),
                status_code(200).body(MOCK_METADATA),
            ]),
        );

        let detector = GoogleCloudResourceDetectorBuilder::new()
            .with_endpoint(server.url("").to_string())
            .with_attempt_count(3);

        let result = detector.fetch_instance_metadata().await?;
        assert_eq!(result, MOCK_METADATA);
        Ok(())
    }

    #[tokio::test]
    #[parallel]
    async fn retry_on_timeout() -> anyhow::Result<()> {
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::method_path("GET", INSTANCE_METADATA_PATH),
                request::query(url_decoded(contains(("recursive", "true")))),
            ])
            .times(2)
            .respond_with(cycle![
                delay_and_then(
                    Duration::from_millis(200),
                    status_code(200).body(MOCK_METADATA),
                ),
                status_code(200).body(MOCK_METADATA)
            ]),
        );

        let detector = GoogleCloudResourceDetectorBuilder::new()
            .with_endpoint(server.url("").to_string())
            .with_attempt_timeout(Duration::from_millis(100))
            .with_attempt_count(2);

        let result = detector.fetch_instance_metadata().await?;
        assert_eq!(result, MOCK_METADATA);
        Ok(())
    }

    #[tokio::test]
    #[parallel]
    async fn too_many_transients() -> anyhow::Result<()> {
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::method_path("GET", INSTANCE_METADATA_PATH),
                request::query(url_decoded(contains(("recursive", "true")))),
            ])
            .times(3)
            .respond_with(status_code(404)),
        );

        let detector = GoogleCloudResourceDetectorBuilder::new()
            .with_endpoint(server.url("").to_string())
            .with_attempt_count(3);

        let result = detector.fetch_instance_metadata().await;
        assert!(matches!(result, Err(Error::Mds(_))), "{result:?}");
        Ok(())
    }

    fn success_setup() -> (Server, GoogleCloudResourceDetectorBuilder) {
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::method_path("GET", INSTANCE_METADATA_PATH),
                request::query(url_decoded(contains(("recursive", "true")))),
            ])
            .respond_with(status_code(200).body(MOCK_METADATA)),
        );
        let detector =
            GoogleCloudResourceDetectorBuilder::new().with_endpoint(server.url("").to_string());
        (server, detector)
    }

    #[test_case("", None, None)]
    #[test_case("projects/p/zones", None, None)]
    #[test_case("projects/p/zones/z", None, Some("z"))]
    #[test_case("projects/p/zones/z-b", None, Some("z-b"))]
    #[test_case("projects/p/zones/z-c-", None, Some("z-c-"))]
    #[test_case("projects/p/zones/us-central1x", None, Some("us-central1x"))]
    #[test_case(
        "projects/p/zones/us-central1-c",
        Some("us-central1"),
        Some("us-central1-c")
    )]
    #[test_case(
        "projects/p/zones/us-central1-aaa",
        Some("us-central1"),
        Some("us-central1-aaa")
    )]
    #[parallel]
    fn region_and_zone(input: &str, want_region: Option<&str>, want_zone: Option<&str>) {
        let (got_region, got_zone) = parse_zone(input);
        assert_eq!(got_region, want_region);
        assert_eq!(got_zone, want_zone);
    }
}
