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

use crate::constants::TRUST_BOUNDARY_HEADER;
use crate::credentials::EntityTag;
use crate::credentials::{
    AccessToken, AccessTokenCredentialsProvider, CacheableResource, CredentialsProvider, dynamic,
};
use crate::mds::client::Client as MDSClient;
use crate::universe_domain::is_default_universe_domain;
use crate::{Result, errors};
use google_cloud_gax::Result as GaxResult;
use google_cloud_gax::backoff_policy::BackoffPolicy;
use google_cloud_gax::error::Error as GaxError;
use google_cloud_gax::exponential_backoff::ExponentialBackoff;
use google_cloud_gax::retry_loop_internal::retry_loop;
use google_cloud_gax::retry_policy::{Aip194Strict, RetryPolicy, RetryPolicyExt};
use google_cloud_gax::retry_throttler::{AdaptiveThrottler, RetryThrottlerArg};
use http::{Extensions, HeaderMap, HeaderValue};
use reqwest::Client;
use std::clone::Clone;
use std::fmt::Debug;
use std::sync::{Arc, OnceLock};
use tokio::sync::{Mutex, watch};
use tokio::time::{Duration, Instant, sleep};

const NO_OP_ENCODED_LOCATIONS: &str = "0x0";

// TTL: 6 hours
const DEFAULT_TTL: Duration = Duration::from_secs(6 * 60 * 60);
// Refresh slack: an hour before the TTL expires
const REFRESH_SLACK: Duration = Duration::from_secs(60 * 60);
// Period to wait after an error: 15 minutes
const COOLDOWN_INTERVAL: Duration = Duration::from_secs(15 * 60);

#[derive(Debug)]
struct AccessBoundary {
    /// A channel to keep track of the boundary state.
    /// - `None`: We haven't fetched anything yet (uninitialized).
    /// - `Some(...)`: We successfully talked to the IAM service or have a customer provided override.
    ///   These values come with a TTL so we know how long to keep them around.
    rx_header: watch::Receiver<(Option<BoundaryValue>, EntityTag)>,
}

#[derive(Debug, Clone)]
struct BoundaryValue {
    /// This is an `Option` because the IAM service can signal that the
    /// given credential has no access boundary. In that case, we save it as `None`
    /// (along with the TTL in `expires_at`) so we don't repeatedly
    /// fetch a non-existent boundary.
    value: Option<String>,
    expires_at: Instant,
}

impl BoundaryValue {
    fn new(value: Option<String>) -> Self {
        Self {
            value,
            expires_at: Instant::now() + DEFAULT_TTL,
        }
    }
}

impl AccessBoundary {
    fn new<T>(provider: T) -> Self
    where
        T: AccessBoundaryProvider + 'static,
    {
        let (tx_header, rx_header) = watch::channel((None, EntityTag::new()));

        if Self::is_enabled() {
            tokio::spawn(refresh_task(provider, tx_header));
        }

        Self { rx_header }
    }

    pub(crate) fn new_no_op() -> Self {
        let (_, rx_header) = watch::channel((None, EntityTag::new()));
        Self { rx_header }
    }
    fn is_enabled() -> bool {
        #[cfg(google_cloud_unstable_trusted_boundaries)]
        {
            true
        }
        #[cfg(not(google_cloud_unstable_trusted_boundaries))]
        {
            false
        }
    }

    fn latest_header_value_and_entity_tag(&self) -> (Option<String>, EntityTag) {
        let (val, tag) = self.rx_header.borrow().clone();
        let val = val
            .filter(|b| b.expires_at >= Instant::now()) // fail open if expired
            .and_then(|b| b.value)
            .filter(|v| v != NO_OP_ENCODED_LOCATIONS);
        (val, tag)
    }
}

/// A decorator for [crate::credentials::AccessTokenCredentialsProvider] with access boundary information.
#[derive(Clone, Debug)]
pub(crate) struct CredentialsWithAccessBoundary<T>
where
    T: dynamic::AccessTokenCredentialsProvider + 'static,
{
    credentials: Arc<T>,
    access_boundary: Arc<AccessBoundary>,
    cache: Arc<Mutex<EntityTagCache>>,
}

/// A cache designed to track cache validity for composite resources.
/// It maps a dynamically requested "outer" `EntityTag` to the "inner" `EntityTag`
/// provided by the underlying credentials layer, alongside a value that represents
/// the current properties injected by the composite layer. This is not generic
/// and is instead tailored to the access boundary use case, but later can be
/// reused.
#[derive(Debug)]
struct EntityTagCache {
    /// The `EntityTag` exposed to callers of this composite provider.
    tag: Option<EntityTag>,

    /// The `EntityTag` representing the state of the underlying inner credentials provider.
    creds_tag: Option<EntityTag>,
    /// The cached headers from the underlying provider.
    creds_data: Option<HeaderMap>,
    /// The `EntityTag` representing the state of the access boundary provider.
    boundary_tag: Option<EntityTag>,
    /// The injected state (in this case the current access boundary) tied to this cache entry.
    boundary_data: Option<Option<String>>,
}

impl EntityTagCache {
    fn new() -> Self {
        Self {
            tag: None,
            creds_data: None,
            creds_tag: None,
            boundary_tag: None,
            boundary_data: None,
        }
    }

    fn update_credentials(
        &mut self,
        tag: EntityTag,
        data: HeaderMap,
    ) -> Result<CacheableResource<HeaderMap>> {
        self.creds_tag = Some(tag);
        self.creds_data = Some(data);
        self.update_resource()
    }

    fn update_boundary(
        &mut self,
        tag: EntityTag,
        data: Option<String>,
    ) -> Result<CacheableResource<HeaderMap>> {
        self.boundary_tag = Some(tag);
        self.boundary_data = Some(data);
        self.update_resource()
    }

    fn update_both(
        &mut self,
        creds_tag: EntityTag,
        creds_data: HeaderMap,
        boundary_tag: EntityTag,
        boundary_data: Option<String>,
    ) -> Result<CacheableResource<HeaderMap>> {
        self.creds_tag = Some(creds_tag);
        self.creds_data = Some(creds_data);
        self.boundary_tag = Some(boundary_tag);
        self.boundary_data = Some(boundary_data);
        self.update_resource()
    }

    fn update_resource(&mut self) -> Result<CacheableResource<HeaderMap>> {
        let new = EntityTag::new();
        self.tag = Some(new.clone());
        Ok(CacheableResource::New {
            entity_tag: new,
            data: self.combine()?,
        })
    }

    fn combine(&self) -> Result<HeaderMap> {
        let mut headers = self
            .creds_data
            .clone()
            .expect("credentials returned NotModified when no data was cached");
        if let Some(Some(value)) = &self.boundary_data {
            headers.insert(
                TRUST_BOUNDARY_HEADER,
                HeaderValue::from_str(value).map_err(errors::non_retryable)?,
            );
        }
        Ok(headers)
    }
}

impl<T> CredentialsWithAccessBoundary<T>
where
    T: dynamic::AccessTokenCredentialsProvider + 'static,
{
    pub(crate) fn new(credentials: T, access_boundary_url: Option<String>) -> Self {
        let credentials = Arc::new(credentials);

        let provider = IAMAccessBoundaryProvider {
            credentials: credentials.clone(),
            url: access_boundary_url,
        };
        let access_boundary = Arc::new(AccessBoundary::new(provider));
        Self {
            credentials,
            access_boundary,
            cache: Arc::new(Mutex::new(EntityTagCache::new())),
        }
    }

    pub(crate) fn new_for_mds(
        credentials: T,
        mds_client: MDSClient,
        iam_endpoint_override: Option<String>,
    ) -> Self {
        let credentials = Arc::new(credentials);
        let provider = MDSAccessBoundaryProvider {
            credentials: credentials.clone(),
            mds_client,
            iam_endpoint_override,
            url: OnceLock::new(),
        };
        let access_boundary = Arc::new(AccessBoundary::new(provider));
        Self {
            credentials,
            access_boundary,
            cache: Arc::new(Mutex::new(EntityTagCache::new())),
        }
    }

    pub(crate) fn new_no_op(credentials: T) -> Self {
        Self {
            credentials: Arc::new(credentials),
            access_boundary: Arc::new(AccessBoundary::new_no_op()),
            cache: Arc::new(Mutex::new(EntityTagCache::new())),
        }
    }

    #[cfg(all(test, google_cloud_unstable_trusted_boundaries))]
    pub(crate) async fn wait_for_boundary(&self) {
        let mut rx = self.access_boundary.rx_header.clone();
        if rx.borrow().0.is_some() {
            return;
        }
        let _ = rx.changed().await;
    }
}

impl<T> CredentialsWithAccessBoundary<T>
where
    T: dynamic::AccessTokenCredentialsProvider + 'static,
{
    async fn query_credentials(
        &self,
        inner_tag: &Option<EntityTag>,
        extensions: Extensions,
    ) -> Result<CacheableResource<HeaderMap>> {
        let mut extensions = extensions;
        if let Some(tag) = inner_tag {
            extensions.insert(tag.clone());
        } else {
            extensions.remove::<EntityTag>();
        }

        self.credentials.headers(extensions).await
    }

    fn query_boundary(&self, inner_tag: &Option<EntityTag>) -> CacheableResource<Option<String>> {
        let (boundary_value, boundary_tag) =
            self.access_boundary.latest_header_value_and_entity_tag();
        match inner_tag {
            Some(tag) if tag.eq(&boundary_tag) => CacheableResource::NotModified,
            _ => CacheableResource::New {
                entity_tag: boundary_tag,
                data: boundary_value,
            },
        }
    }
}

/// Decorates Credentials and AccessTokenCredentials with access boundary information.
impl<T> CredentialsProvider for CredentialsWithAccessBoundary<T>
where
    T: dynamic::AccessTokenCredentialsProvider + 'static,
{
    async fn headers(&self, extensions: Extensions) -> Result<CacheableResource<HeaderMap>> {
        if !AccessBoundary::is_enabled() {
            return self.credentials.headers(extensions).await;
        }

        let tag = extensions.get::<EntityTag>();
        let mut guard = self.cache.lock().await;

        let creds_resource = self
            .query_credentials(&guard.creds_tag, extensions.clone())
            .await?;
        let boundary_resource = self.query_boundary(&guard.boundary_tag);

        let new = match (tag, creds_resource, boundary_resource) {
            (Some(tag), CacheableResource::NotModified, CacheableResource::NotModified)
                if Some(tag) == guard.tag.as_ref() =>
            {
                return Ok(CacheableResource::NotModified);
            }
            (None | Some(_), CacheableResource::NotModified, CacheableResource::NotModified) => {
                return Ok(CacheableResource::New {
                    entity_tag: guard
                        .tag
                        .clone()
                        .expect("both credentials and access boundary returned NotModified, we should have a entity tag"),
                    data: guard.combine()?,
                });
            }
            (_, CacheableResource::New { entity_tag, data }, CacheableResource::NotModified) => {
                guard.update_credentials(entity_tag, data)?
            }
            (_, CacheableResource::NotModified, CacheableResource::New { entity_tag, data }) => {
                guard.update_boundary(entity_tag, data)?
            }
            (
                _,
                CacheableResource::New {
                    entity_tag: creds_tag,
                    data: creds_data,
                },
                CacheableResource::New {
                    entity_tag: boundary_tag,
                    data: boundary_data,
                },
            ) => guard.update_both(creds_tag, creds_data, boundary_tag, boundary_data)?,
        };

        Ok(new)
    }

    async fn universe_domain(&self) -> Option<String> {
        self.credentials.universe_domain().await
    }
}

impl<T> AccessTokenCredentialsProvider for CredentialsWithAccessBoundary<T>
where
    T: dynamic::AccessTokenCredentialsProvider + 'static,
{
    async fn access_token(&self) -> Result<AccessToken> {
        self.credentials.access_token().await
    }
}

// internal trait for testability and avoid dependency on reqwest
// which causes issues with tokio::time::advance and tokio::task::yield_now
#[async_trait::async_trait]
pub(crate) trait AccessBoundaryProvider: std::fmt::Debug + Send + Sync {
    async fn fetch_access_boundary(&self) -> Result<Option<String>>;
}

// default implementation that uses IAM Access Boundaries API
#[derive(Debug)]
struct IAMAccessBoundaryProvider<T>
where
    T: dynamic::AccessTokenCredentialsProvider + 'static,
{
    credentials: Arc<T>,
    url: Option<String>,
}

#[async_trait::async_trait]
impl<T> AccessBoundaryProvider for IAMAccessBoundaryProvider<T>
where
    T: dynamic::AccessTokenCredentialsProvider + 'static,
{
    async fn fetch_access_boundary(&self) -> Result<Option<String>> {
        let universe_domain = self.credentials.universe_domain().await;
        if !is_default_universe_domain(universe_domain.as_deref()) {
            return Ok(None);
        }
        match self.url.as_ref() {
            Some(url) => {
                let client = AccessBoundaryClient::new(self.credentials.clone(), url.clone());
                client.fetch().await
            }
            None => Ok(None), // No URL means no access boundary
        }
    }
}

// Extends default IAM implementation to use Metadata Service
#[derive(Debug)]
struct MDSAccessBoundaryProvider<T>
where
    T: dynamic::AccessTokenCredentialsProvider + 'static,
{
    credentials: Arc<T>,
    mds_client: MDSClient,
    iam_endpoint_override: Option<String>,
    url: OnceLock<String>,
}

#[async_trait::async_trait]
impl<T> AccessBoundaryProvider for MDSAccessBoundaryProvider<T>
where
    T: dynamic::AccessTokenCredentialsProvider + 'static,
{
    async fn fetch_access_boundary(&self) -> Result<Option<String>> {
        let universe_domain = self.credentials.universe_domain().await;
        if !is_default_universe_domain(universe_domain.as_deref()) {
            return Ok(None);
        }

        if self.url.get().is_none() {
            let email = self.mds_client.email().send().await?;

            // Ignore error if we can't set the client email.
            // Might be due to multiple tasks trying to set value
            let url = service_account_lookup_url(&email, self.iam_endpoint_override.as_deref());
            let _ = self.url.set(url);
        }

        let url = self.url.get().unwrap().to_string();
        let client = AccessBoundaryClient::new(self.credentials.clone(), url);
        client.fetch().await
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct AllowedLocationsResponse {
    #[allow(dead_code)]
    locations: Vec<String>,
    #[serde(rename = "encodedLocations")]
    encoded_locations: String,
}

/// Makes the `fetch()` function easier to test.
///
/// In the tests we need to override the retry policies, that is easier to do if the policies are
/// part of some struct.
#[derive(Debug)]
struct AccessBoundaryClient<T> {
    credentials: Arc<T>,
    url: String,
    retry_policy: Arc<dyn RetryPolicy>,
    backoff_policy: Arc<dyn BackoffPolicy>,
}

impl<T> AccessBoundaryClient<T> {
    fn new(credentials: Arc<T>, url: String) -> Self {
        let retry_policy = Aip194Strict.with_time_limit(Duration::from_secs(60));
        let backoff_policy = ExponentialBackoff::default();

        Self {
            credentials,
            url,
            retry_policy: Arc::new(retry_policy),
            backoff_policy: Arc::new(backoff_policy),
        }
    }
}

impl<T> AccessBoundaryClient<T>
where
    T: dynamic::AccessTokenCredentialsProvider + Send + Sync + 'static,
{
    async fn fetch(self) -> Result<Option<String>> {
        let resp = self
            .fetch_with_retry()
            .await
            .map_err(|e| crate::errors::from_gax_error(e, "failed to fetch access boundary"))?;

        if !resp.encoded_locations.is_empty() {
            return Ok(Some(resp.encoded_locations));
        }

        Ok(None)
    }

    async fn fetch_with_retry(self) -> GaxResult<AllowedLocationsResponse> {
        let client = Client::new();
        let sleep = async |d| tokio::time::sleep(d).await;

        let retry_throttler: RetryThrottlerArg = AdaptiveThrottler::default().into();
        let creds = self.credentials;
        let url = self.url;
        let inner = async move |d| {
            let headers = creds
                .headers(Extensions::new())
                .await
                .map_err(GaxError::authentication)?;

            let attempt = self::fetch_access_boundary_call(&client, &url, headers);
            match d {
                Some(timeout) => match tokio::time::timeout(timeout, attempt).await {
                    Ok(r) => r,
                    Err(e) => Err(GaxError::timeout(e)),
                },
                None => attempt.await,
            }
        };

        retry_loop(
            inner,
            sleep,
            true, // fetch access boundary is idempotent
            retry_throttler.into(),
            self.retry_policy.clone(),
            self.backoff_policy.clone(),
        )
        .await
    }
}

async fn fetch_access_boundary_call(
    client: &Client,
    url: &str,
    headers: CacheableResource<HeaderMap>,
) -> GaxResult<AllowedLocationsResponse> {
    let headers = match headers {
        CacheableResource::New { data, .. } => data,
        CacheableResource::NotModified => {
            unreachable!("requested access boundary without a caching etag")
        }
    };

    let resp = client
        .get(url)
        .headers(headers)
        .send()
        .await
        .map_err(GaxError::io)?;

    let status = resp.status();
    if !status.is_success() {
        let err_headers = resp.headers().clone();
        let err_payload = resp
            .bytes()
            .await
            .map_err(|e| GaxError::transport(err_headers.clone(), e))?;
        return Err(GaxError::http(status.as_u16(), err_headers, err_payload));
    }

    resp.json().await.map_err(GaxError::io)
}

async fn refresh_task<T>(provider: T, tx_header: watch::Sender<(Option<BoundaryValue>, EntityTag)>)
where
    T: AccessBoundaryProvider,
{
    loop {
        match provider.fetch_access_boundary().await {
            Ok(val) => {
                let _ = tx_header.send((Some(BoundaryValue::new(val)), EntityTag::new()));
                sleep(DEFAULT_TTL - REFRESH_SLACK).await
            }
            Err(_e) => {
                sleep(COOLDOWN_INTERVAL).await;
            }
        }
    }
}

pub(crate) fn service_account_lookup_url(
    email: &str,
    iam_endpoint_override: Option<&str>,
) -> String {
    let iam_endpoint = iam_endpoint_override.unwrap_or("https://iamcredentials.googleapis.com");
    format!("{iam_endpoint}/v1/projects/-/serviceAccounts/{email}/allowedLocations")
}

pub(crate) fn external_account_lookup_url(
    audience: &str,
    iam_endpoint_override: Option<&str>,
) -> Option<String> {
    let iam_endpoint = iam_endpoint_override.unwrap_or("https://iamcredentials.googleapis.com");

    // Strip common domain and scheme prefixes to normalize the relative path.
    let path = audience
        .strip_prefix("//iam.googleapis.com/")
        .or_else(|| audience.strip_prefix("https://iam.googleapis.com/"))
        .or_else(|| audience.strip_prefix('/'))
        .unwrap_or(audience);

    let parts: Vec<&str> = path.split('/').collect();

    match &parts[..] {
        // Workload Identity Pool
        [
            "projects",
            project,
            "locations",
            "global",
            "workloadIdentityPools",
            pool,
            "providers",
            provider,
        ] if !project.is_empty() && !pool.is_empty() && !provider.is_empty() => Some(format!(
            "{}/v1/projects/{}/locations/global/workloadIdentityPools/{}/allowedLocations",
            iam_endpoint, project, pool
        )),
        // Workforce Pool
        [
            "locations",
            "global",
            "workforcePools",
            pool,
            "providers",
            provider,
        ] if !pool.is_empty() && !provider.is_empty() => Some(format!(
            "{}/v1/locations/global/workforcePools/{}/allowedLocations",
            iam_endpoint, pool
        )),
        _ => None,
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::credentials::EntityTag;
    use crate::credentials::tests::{
        MockCredentials, get_access_boundary_from_headers, get_token_from_headers,
    };
    use crate::errors::CredentialsError;
    use google_cloud_gax::exponential_backoff::ExponentialBackoffBuilder;
    use http::header::{AUTHORIZATION, HeaderValue};
    use http::{Extensions, HeaderMap};
    use httptest::{Expectation, Server, cycle, matchers::*, responders::*};
    use serde_json::json;
    use serial_test::parallel;
    use test_case::test_case;

    type TestResult = anyhow::Result<()>;

    // Used by tests in other modules.
    mockall::mock! {
        #[derive(Debug)]
        pub AccessBoundaryProvider { }

        #[async_trait::async_trait]
        impl AccessBoundaryProvider for AccessBoundaryProvider {
            async fn fetch_access_boundary(&self) -> Result<Option<String>>;
        }
    }

    impl AccessBoundary {
        fn new_with_mock_provider<T>(provider: T) -> Self
        where
            T: AccessBoundaryProvider + 'static,
        {
            let (tx_header, rx_header) = watch::channel((None, EntityTag::new()));
            tokio::spawn(refresh_task(provider, tx_header));
            Self { rx_header }
        }

        fn header_value(&self) -> Option<String> {
            let (val, _) = self.latest_header_value_and_entity_tag();
            val
        }
    }

    #[test]
    #[parallel]
    fn test_service_account_url() {
        assert_eq!(
            service_account_lookup_url("sa@project.iam.gserviceaccount.com", None),
            "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/sa@project.iam.gserviceaccount.com/allowedLocations"
        );
    }

    #[test_case("//iam.googleapis.com/projects/p1/locations/global/workloadIdentityPools/pool1/providers/prov1", Some("https://iamcredentials.googleapis.com/v1/projects/p1/locations/global/workloadIdentityPools/pool1/allowedLocations"), None; "workload_full_prefix")]
    #[test_case("https://iam.googleapis.com/projects/p1/locations/global/workloadIdentityPools/pool1/providers/prov1", Some("https://iamcredentials.googleapis.com/v1/projects/p1/locations/global/workloadIdentityPools/pool1/allowedLocations"), None; "workload_https_prefix")]
    #[test_case("/projects/p1/locations/global/workloadIdentityPools/pool1/providers/prov1", Some("https://iamcredentials.googleapis.com/v1/projects/p1/locations/global/workloadIdentityPools/pool1/allowedLocations"), None; "workload_slash_prefix")]
    #[test_case("projects/p1/locations/global/workloadIdentityPools/pool1/providers/prov1", Some("https://iamcredentials.googleapis.com/v1/projects/p1/locations/global/workloadIdentityPools/pool1/allowedLocations"), None; "workload_no_prefix")]
    #[test_case("//iam.googleapis.com/locations/global/workforcePools/pool1/providers/prov1", Some("https://iamcredentials.googleapis.com/v1/locations/global/workforcePools/pool1/allowedLocations"), None; "workforce_full_prefix")]
    #[test_case("https://iam.googleapis.com/locations/global/workforcePools/pool1/providers/prov1", Some("https://iamcredentials.googleapis.com/v1/locations/global/workforcePools/pool1/allowedLocations"), None; "workforce_https_prefix")]
    #[test_case("/locations/global/workforcePools/pool1/providers/prov1", Some("https://iamcredentials.googleapis.com/v1/locations/global/workforcePools/pool1/allowedLocations"), None; "workforce_slash_prefix")]
    #[test_case("locations/global/workforcePools/pool1/providers/prov1", Some("https://iamcredentials.googleapis.com/v1/locations/global/workforcePools/pool1/allowedLocations"), None; "workforce_no_prefix")]
    #[test_case("projects/p1/locations/global/workloadIdentityPools/pool1/providers/prov1/", None, None; "trailing_slash_fails")]
    #[test_case("projects/p1/locations/global/workloadIdentityPools/pool1/providers/prov1/extra", None, None; "extra_parts_fails")]
    #[test_case("projects/p1/locations/global/workloadIdentityPools/pool1", None, None; "missing_parts_fails")]
    #[test_case("projects/p1/locations/global/workforcePools/pool1/providers/prov1", None, None; "workforce_in_workload_format_fails")]
    #[test_case("locations/global/workloadIdentityPools/pool1/providers/prov1", None, None; "workload_in_workforce_format_fails")]
    #[test_case("invalid", None, None; "invalid_string_fails")]
    #[test_case("//iam.googleapis.com/projects/p1/locations/global/workloadIdentityPools/pool1/providers/prov1", Some("http://localhost:8080/v1/projects/p1/locations/global/workloadIdentityPools/pool1/allowedLocations"), Some("http://localhost:8080"); "with_endpoint_override")]
    #[parallel]
    fn test_external_account_lookup_url(
        audience: &str,
        expected: Option<&str>,
        iam_endpoint_override: Option<&str>,
    ) {
        let actual = external_account_lookup_url(audience, iam_endpoint_override);
        assert_eq!(actual.as_deref(), expected);
    }

    #[tokio::test]
    #[parallel]
    #[cfg(google_cloud_unstable_trusted_boundaries)]
    async fn test_fetch_access_boundary_success() -> TestResult {
        let server = Server::run();
        server.expect(
            Expectation::matching(request::method_path("GET", "/allowedLocations")).respond_with(
                json_encoded(json!(
                    {
                        "encodedLocations": "0x123",
                        "locations": ["us-east1"]
                    }
                )),
            ),
        );

        let mut mock = MockCredentials::new();
        mock.expect_headers().returning(|_extensions| {
            let headers = HeaderMap::from_iter([(
                AUTHORIZATION,
                HeaderValue::from_static("Bearer test-token"),
            )]);
            Ok(CacheableResource::New {
                entity_tag: EntityTag::default(),
                data: headers,
            })
        });
        mock.expect_universe_domain().returning(|| None);

        let url = server.url("/allowedLocations").to_string();

        let creds = CredentialsWithAccessBoundary::new(mock, Some(url));

        // wait for the background task to fetch the access boundary.
        creds.wait_for_boundary().await;

        let cached_headers = creds.headers(Extensions::new()).await?;
        let token = get_token_from_headers(cached_headers.clone());
        assert!(token.is_some(), "{token:?}");
        let access_boundary = get_access_boundary_from_headers(cached_headers);
        assert_eq!(
            access_boundary.as_deref(),
            Some("0x123"),
            "{access_boundary:?}"
        );

        Ok(())
    }

    #[tokio::test]
    #[parallel]
    #[cfg(google_cloud_unstable_trusted_boundaries)]
    async fn test_fetch_access_boundary_mds_success() -> TestResult {
        use crate::mds::MDS_DEFAULT_URI;

        let server = Server::run();
        server.expect(
            Expectation::matching(request::method_path(
                "GET",
                "/v1/projects/-/serviceAccounts/some-client-email/allowedLocations",
            ))
            .respond_with(json_encoded(json!(
                {
                    "encodedLocations": "0x123",
                    "locations": ["us-east1"]
                }
            ))),
        );
        server.expect(
            Expectation::matching(all_of![request::path(format!("{MDS_DEFAULT_URI}/email")),])
                .respond_with(status_code(200).body("some-client-email")),
        );

        let mut mock = MockCredentials::new();
        mock.expect_headers().returning(|_extensions| {
            let headers = HeaderMap::from_iter([(
                AUTHORIZATION,
                HeaderValue::from_static("Bearer test-token"),
            )]);
            Ok(CacheableResource::New {
                entity_tag: EntityTag::default(),
                data: headers,
            })
        });
        mock.expect_universe_domain().returning(|| None);

        let endpoint = server.url("").to_string().trim_end_matches('/').to_string();
        let mds_client = MDSClient::new(Some(endpoint.clone()));

        let creds = CredentialsWithAccessBoundary::new_for_mds(mock, mds_client, Some(endpoint));

        // wait for the background task to fetch the access boundary.
        creds.wait_for_boundary().await;

        let cached_headers = creds.headers(Extensions::new()).await?;
        let token = get_token_from_headers(cached_headers.clone());
        assert!(token.is_some(), "{token:?}");
        let access_boundary = get_access_boundary_from_headers(cached_headers);
        assert_eq!(
            access_boundary.as_deref(),
            Some("0x123"),
            "{access_boundary:?}"
        );

        Ok(())
    }

    #[tokio::test]
    #[parallel]
    async fn test_fetch_access_boundary_empty() -> TestResult {
        let server = Server::run();
        server.expect(
            Expectation::matching(request::method_path("GET", "/allowedLocations")).respond_with(
                json_encoded(json!({
                    "encodedLocations": "",
                    "locations": []
                })),
            ),
        );

        let mut mock = MockCredentials::new();
        mock.expect_headers().return_once(|_extensions| {
            let headers = HeaderMap::from_iter([(
                AUTHORIZATION,
                HeaderValue::from_static("Bearer test-token"),
            )]);
            Ok(CacheableResource::New {
                entity_tag: EntityTag::default(),
                data: headers,
            })
        });
        let url = server.url("/allowedLocations").to_string();
        let client = AccessBoundaryClient::new(Arc::new(mock), url);
        let val = client.fetch().await?;
        assert!(val.is_none(), "{val:?}");

        Ok(())
    }

    #[tokio::test]
    #[parallel]
    async fn test_fetch_access_boundary_error() {
        let server = Server::run();
        server.expect(
            Expectation::matching(request::method_path("GET", "/allowedLocations"))
                .times(1..)
                .respond_with(status_code(503)),
        );

        let mut mock = MockCredentials::new();
        mock.expect_headers().returning(|_extensions| {
            let headers = HeaderMap::from_iter([(
                AUTHORIZATION,
                HeaderValue::from_static("Bearer test-token"),
            )]);
            Ok(CacheableResource::New {
                entity_tag: EntityTag::default(),
                data: headers,
            })
        });

        let url = server.url("/allowedLocations").to_string();
        let mut client = AccessBoundaryClient::new(Arc::new(mock), url);
        client.retry_policy = Arc::new(Aip194Strict.with_attempt_limit(3));
        client.backoff_policy = Arc::new(test_backoff_policy());

        let result = client.fetch().await;
        let err = result.unwrap_err();
        assert!(err.is_transient(), "{err:?}");
    }

    #[tokio::test]
    #[parallel]
    async fn test_fetch_access_boundary_token_error() {
        let mut mock = MockCredentials::new();
        mock.expect_headers().returning(|_extensions| {
            Err(CredentialsError::from_msg(
                false,
                "invalid creds".to_string(),
            ))
        });

        let client = AccessBoundaryClient::new(Arc::new(mock), "http://localhost".to_string());
        let err = client.fetch().await.unwrap_err();
        assert!(!err.is_transient(), "{err:?}");
    }

    #[tokio::test]
    #[parallel]
    async fn test_access_boundary_new_disabled() -> TestResult {
        let mut mock = MockCredentials::new();
        mock.expect_headers().return_once(|_extensions| {
            let headers = HeaderMap::from_iter([(
                AUTHORIZATION,
                HeaderValue::from_static("Bearer test-token"),
            )]);
            Ok(CacheableResource::New {
                entity_tag: EntityTag::default(),
                data: headers,
            })
        });
        let creds = CredentialsWithAccessBoundary::new(mock, None);

        let cached_headers = creds.headers(Extensions::new()).await?;
        let token = get_token_from_headers(cached_headers.clone());
        assert!(token.is_some(), "{token:?}");
        let access_boundary = get_access_boundary_from_headers(cached_headers);
        assert!(access_boundary.is_none(), "{access_boundary:?}");

        Ok(())
    }

    #[tokio::test]
    #[parallel]
    async fn test_access_boundary_header_value_no_op() {
        let (tx, rx_header) = watch::channel((None, EntityTag::new()));
        let access_boundary = AccessBoundary { rx_header };

        let _ = tx.send((
            Some(BoundaryValue::new(Some("0x123".to_string()))),
            EntityTag::new(),
        ));
        assert_eq!(access_boundary.header_value().as_deref(), Some("0x123"));

        let _ = tx.send((
            Some(BoundaryValue::new(Some(
                NO_OP_ENCODED_LOCATIONS.to_string(),
            ))),
            EntityTag::new(),
        ));
        let val = access_boundary.header_value();
        assert!(val.is_none(), "{val:?}");

        let _ = tx.send((None, EntityTag::new()));
        let val = access_boundary.header_value();
        assert!(val.is_none(), "{val:?}");
    }

    #[tokio::test(start_paused = true)]
    #[parallel]
    async fn test_refresh_task_backoff() {
        let mut mock_provider = MockAccessBoundaryProvider::new();
        mock_provider
            .expect_fetch_access_boundary()
            .times(2)
            .returning(|| Err(CredentialsError::from_msg(false, "test error")));

        mock_provider
            .expect_fetch_access_boundary()
            .times(1)
            .return_once(|| Ok(Some("0x123".to_string())));

        let (tx, rx) = watch::channel((None, EntityTag::new()));

        tokio::spawn(async move {
            refresh_task(mock_provider, tx).await;
        });

        // allow task to start and fail the first request
        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::task::yield_now().await;

        let (val, _) = rx.borrow().clone();
        assert!(val.is_none(), "should be None on startup/error: {val:?}");

        // advance 15 minutes, next call fails
        tokio::time::advance(COOLDOWN_INTERVAL).await;
        tokio::task::yield_now().await;

        let (val, _) = rx.borrow().clone();
        assert!(
            val.is_none(),
            "should still be None after second error: {val:?}"
        );

        // advance 15 minutes, third call succeeds
        tokio::time::advance(COOLDOWN_INTERVAL).await;
        tokio::task::yield_now().await;

        let (val, _) = rx.borrow().clone();
        let val = val.as_ref().and_then(|v| v.value.as_deref());
        assert_eq!(val, Some("0x123"), "{val:?}");
    }

    #[tokio::test(start_paused = true)]
    #[parallel]
    async fn expired_access_boundary_returns_none() {
        let (tx, rx_header) = watch::channel((None, EntityTag::new()));
        let access_boundary = AccessBoundary { rx_header };

        let ttl = Duration::from_secs(10);
        let expires_at = Instant::now() + ttl;
        let _ = tx.send((
            Some(BoundaryValue {
                value: Some("old-value".to_string()),
                expires_at,
            }),
            EntityTag::new(),
        ));

        // value is valid
        let val = access_boundary.header_value();
        assert_eq!(val.as_deref(), Some("old-value"), "{val:?}");

        // advance time plus some buffer to expire the value
        tokio::time::advance(ttl + Duration::from_secs(1)).await;

        // value should return None if expired (non-blocking)
        let val = access_boundary.header_value();
        assert!(val.is_none(), "{val:?}");

        // update with new value
        let _ = tx.send((
            Some(BoundaryValue::new(Some("new-value".to_string()))),
            EntityTag::new(),
        ));

        let val = access_boundary.header_value();
        assert_eq!(val.as_deref(), Some("new-value"), "{val:?}");
    }

    #[tokio::test(start_paused = true)]
    #[parallel]
    async fn access_boundary_provider_refreshes() {
        let mut mock_provider = MockAccessBoundaryProvider::new();

        mock_provider
            .expect_fetch_access_boundary()
            .times(1)
            .returning(|| Ok(Some("old-value".to_string())));

        mock_provider
            .expect_fetch_access_boundary()
            .times(1)
            .returning(|| Ok(Some("new-value".to_string())));

        let access_boundary = AccessBoundary::new_with_mock_provider(mock_provider);

        // allow task to start and fail the first request
        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::task::yield_now().await;

        // value is valid
        let val = access_boundary.header_value();
        assert_eq!(val.as_deref(), Some("old-value"), "{val:?}");

        // advance time beyond the time to refresh
        tokio::time::advance(DEFAULT_TTL).await;
        tokio::task::yield_now().await;

        let val = access_boundary.header_value();
        assert_eq!(val.as_deref(), Some("new-value"), "{val:?}");
    }

    #[tokio::test(start_paused = true)]
    #[parallel]
    #[cfg(google_cloud_unstable_trusted_boundaries)]
    async fn test_entity_tag_caching_behavior() -> TestResult {
        let mut mock_creds = MockCredentials::new();
        let latest_token_etag = Arc::new(std::sync::RwLock::new(EntityTag::new()));
        let closure_latest_token_etag = latest_token_etag.clone();
        mock_creds.expect_headers().returning(move |extensions| {
            let user_etag = extensions.get::<EntityTag>().cloned();
            let token_etag = closure_latest_token_etag.read().unwrap();
            match user_etag {
                Some(etag) if etag.eq(&*token_etag) => Ok(CacheableResource::NotModified),
                _ => Ok(CacheableResource::New {
                    entity_tag: token_etag.clone(),
                    data: HeaderMap::new(),
                }),
            }
        });

        let mut mock_boundary = MockAccessBoundaryProvider::new();
        mock_boundary
            .expect_fetch_access_boundary()
            .times(1)
            .returning(|| Ok(Some("0x123".to_string())));
        mock_boundary
            .expect_fetch_access_boundary()
            .times(1)
            .returning(|| Ok(Some("0x321".to_string())));

        let access_boundary = AccessBoundary::new_with_mock_provider(mock_boundary);
        let creds = CredentialsWithAccessBoundary {
            credentials: Arc::new(mock_creds),
            access_boundary: Arc::new(access_boundary),
            cache: Arc::new(tokio::sync::Mutex::new(EntityTagCache::new())),
        };

        // First call - no tag yet
        let cached_headers = creds.headers(Extensions::new()).await?;
        let tag1 = match cached_headers {
            CacheableResource::New { ref entity_tag, .. } => entity_tag.clone(),
            _ => panic!("expected New"),
        };
        let boundary = get_access_boundary_from_headers(cached_headers);
        assert!(boundary.is_none(), "{boundary:?}");

        // Second call with same tag - should be NotModified
        let mut ext = Extensions::new();
        ext.insert(tag1.clone());
        let cached_headers = creds.headers(ext).await?;
        assert!(
            matches!(cached_headers, CacheableResource::NotModified),
            "{cached_headers:?}"
        );

        tokio::time::advance(Duration::from_secs(2)).await; // allow boundary to fetch
        tokio::task::yield_now().await;
        creds.wait_for_boundary().await;

        // Using old tag - inner token didn't change but boundary DID.
        let mut ext = Extensions::new();
        ext.insert(tag1.clone());
        let cached_headers = creds.headers(ext).await?;
        let tag2 = match cached_headers {
            CacheableResource::New { ref entity_tag, .. } => entity_tag.clone(),
            _ => panic!("expected New with updated access boundary"),
        };
        let boundary = get_access_boundary_from_headers(cached_headers);
        assert_eq!(boundary.as_deref(), Some("0x123"), "{boundary:?}");
        assert_ne!(tag1, tag2, "New boundary should result in new ETags");

        // Passing the new tag should return NotModified again
        let mut ext = Extensions::new();
        ext.insert(tag2.clone());
        let cached_headers = creds.headers(ext).await?;
        assert!(
            matches!(cached_headers, CacheableResource::NotModified),
            "{cached_headers:?}"
        );

        // wait for boundary to refresh
        tokio::time::advance(DEFAULT_TTL).await;
        tokio::task::yield_now().await;
        creds.wait_for_boundary().await;

        // Using old tag - inner token didn't change but boundary DID.
        let mut ext = Extensions::new();
        ext.insert(tag2.clone());
        let cached_headers = creds.headers(ext).await?;
        let tag3 = match cached_headers {
            CacheableResource::New { ref entity_tag, .. } => entity_tag.clone(),
            _ => panic!("expected New with updated access boundary"),
        };
        let boundary = get_access_boundary_from_headers(cached_headers);
        assert_eq!(boundary.as_deref(), Some("0x321"), "{boundary:?}");
        assert_ne!(tag2, tag3, "New boundary should result in new ETags");

        // now update the token
        {
            let mut etag = latest_token_etag.write().unwrap();
            *etag = EntityTag::new();
        }

        // Using old tag - boundary didn't change but inner token DID.
        let mut ext = Extensions::new();
        ext.insert(tag3.clone());
        let cached_headers = creds.headers(ext).await?;
        let tag4 = match cached_headers {
            CacheableResource::New { ref entity_tag, .. } => entity_tag.clone(),
            _ => panic!("expected New with updated token"),
        };
        let boundary = get_access_boundary_from_headers(cached_headers);
        assert_eq!(boundary.as_deref(), Some("0x321"), "{boundary:?}");
        assert_ne!(tag3, tag4, "New token should result in new ETags");

        // Using random tag - should return token and boundary just fine.
        let mut ext = Extensions::new();
        ext.insert(EntityTag::new());
        let cached_headers = creds.headers(ext).await?;
        let tag5 = match cached_headers {
            CacheableResource::New { ref entity_tag, .. } => entity_tag.clone(),
            _ => panic!("expected New with updated token"),
        };
        let boundary = get_access_boundary_from_headers(cached_headers);
        assert_eq!(boundary.as_deref(), Some("0x321"), "{boundary:?}");
        assert_eq!(
            tag4, tag5,
            "Same token and boundary should result in same ETags"
        );

        Ok(())
    }

    #[tokio::test]
    #[parallel]
    async fn test_fetch_access_boundary_retry() -> TestResult {
        let server = Server::run();

        let invalid_res = http::Response::builder()
            .version(http::Version::HTTP_3) // unsupported version
            .status(204)
            .body(Vec::new())
            .unwrap();

        server.expect(
            Expectation::matching(request::method_path("GET", "/allowedLocations"))
                .times(3)
                .respond_with(cycle![
                    invalid_res, // forces i/o error
                    status_code(503).body("try-again"),
                    json_encoded(json!({
                        "encodedLocations": "0x123",
                        "locations": ["us-east1"]
                    }))
                ]),
        );

        let mut mock = MockCredentials::new();
        mock.expect_headers().returning(|_extensions| {
            let headers = HeaderMap::from_iter([(
                AUTHORIZATION,
                HeaderValue::from_static("Bearer test-token"),
            )]);
            Ok(CacheableResource::New {
                entity_tag: EntityTag::default(),
                data: headers,
            })
        });

        let url = server.url("/allowedLocations").to_string();
        let mut client = AccessBoundaryClient::new(Arc::new(mock), url);
        client.backoff_policy = Arc::new(test_backoff_policy());
        let val = client.fetch().await?;
        assert_eq!(val.as_deref(), Some("0x123"));

        Ok(())
    }

    #[tokio::test]
    #[parallel]
    #[cfg(google_cloud_unstable_trusted_boundaries)]
    async fn test_credentials_with_access_boundary_non_default_universe() -> TestResult {
        let mut mock = MockCredentials::new();
        mock.expect_headers().returning(|_extensions| {
            let headers = HeaderMap::from_iter([(
                AUTHORIZATION,
                HeaderValue::from_static("Bearer test-token"),
            )]);
            Ok(CacheableResource::New {
                entity_tag: EntityTag::default(),
                data: headers,
            })
        });
        mock.expect_universe_domain()
            .returning(|| Some("my-universe-domain.com".to_string()));

        let creds = CredentialsWithAccessBoundary::new(mock, Some("http://localhost".to_string()));

        let cached_headers = creds.headers(Extensions::new()).await?;
        let token = get_token_from_headers(cached_headers.clone());
        assert!(token.is_some(), "{token:?}");

        let access_boundary = get_access_boundary_from_headers(cached_headers);
        assert!(
            access_boundary.is_none(),
            "Expected no access boundary header for non-default universe: {access_boundary:?}"
        );

        Ok(())
    }

    #[tokio::test]
    #[parallel]
    #[cfg(google_cloud_unstable_trusted_boundaries)]
    async fn test_mds_provider_non_default_universe() -> TestResult {
        let mut mock = MockCredentials::new();
        mock.expect_universe_domain()
            .returning(|| Some("my-universe-domain.com".to_string()));

        let mds_client = MDSClient::new(None);

        let provider = MDSAccessBoundaryProvider {
            credentials: Arc::new(mock),
            mds_client,
            iam_endpoint_override: None,
            url: OnceLock::new(),
        };

        let val = provider.fetch_access_boundary().await?;
        assert!(
            val.is_none(),
            "Expected None for non-default universe domain: {val:?}"
        );

        Ok(())
    }

    /// Makes the tests go faster.
    fn test_backoff_policy() -> ExponentialBackoff {
        ExponentialBackoffBuilder::new()
            .with_initial_delay(Duration::from_millis(1))
            .with_maximum_delay(Duration::from_millis(1))
            .build()
            .expect("hard-coded policy succeeds")
    }
}
