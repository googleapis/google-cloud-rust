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
 
 use gax::retry_throttler::AdaptiveThrottler;
 use gax::Result;
 use gax::backoff_policy::BackoffPolicy;
 use gax::error::Error;
 use gax::error::HttpError;
 use gax::error::ServiceError;
 use gax::exponential_backoff::ExponentialBackoff;
 use gax::retry_policy::RetryPolicy;
 use gax::retry_throttler::SharedRetryThrottler;
 use std::sync::{Arc, Mutex};
 
 #[derive(Clone, Debug)]
 pub(crate) struct ReqwestClient {
     inner: reqwest::Client,
     endpoint: String,
     retry_policy: Option<Arc<dyn RetryPolicy>>,
     backoff_policy: Option<Arc<dyn BackoffPolicy>>,
     retry_throttler: SharedRetryThrottler,
 }

 pub(crate) struct RequestClientBuilder {
    inner: reqwest::Client,
    endpoint: String,
    retry_policy: Option<Arc<dyn RetryPolicy>>,
    backoff_policy: Option<Arc<dyn BackoffPolicy>>,
    retry_throttler: SharedRetryThrottler,
 }
 
impl RequestClientBuilder {
    fn new(endpoint: String) -> Self {
        let inner = reqwest::Client::new();
        Self {
            inner,
            endpoint,
            retry_policy: None,
            backoff_policy: None,
            retry_throttler: Arc::new(Mutex::new(AdaptiveThrottler::default())),

        }
    }

    fn with_retry_policy(mut self, retry_policy: Arc<dyn RetryPolicy>) -> Self {
        self.retry_policy = Some(retry_policy);
        self
    }

    fn with_backoff_policy(mut self, backoff_policy: Arc<dyn BackoffPolicy>) -> Self {
        self.backoff_policy = Some(backoff_policy);
        self
    }

    fn with_retry_throttler(mut self, retry_throttler: SharedRetryThrottler) -> Self {
        self.retry_throttler = retry_throttler;
        self
    }

    fn build(self) -> ReqwestClient {
        ReqwestClient {
            inner: self.inner,
            endpoint: self.endpoint,
            retry_policy: self.retry_policy,
            backoff_policy: self.backoff_policy,
            retry_throttler: self.retry_throttler,
        } 
    }
}

 impl ReqwestClient {

     pub fn builder(&self, method: reqwest::Method, path: String) -> reqwest::RequestBuilder {
         self.inner
             .request(method, format!("{}{path}", &self.endpoint))
     }
 
     pub async fn execute<I: serde::ser::Serialize, O: serde::de::DeserializeOwned>(
         &self,
         mut builder: reqwest::RequestBuilder,
         body: Option<I>,
     ) -> Result<O> {
         if let Some(body) = body {
             builder = builder.json(&body);
         }
         match self.retry_policy.clone() {
             None => self.request_attempt::<O>(builder, None).await,
             Some(policy) => self.retry_loop::<O>(builder, policy).await,
         }
     }
 
     async fn retry_loop<O: serde::de::DeserializeOwned>(
         &self,
         builder: reqwest::RequestBuilder,
         retry_policy: Arc<dyn RetryPolicy>,
     ) -> Result<O> {
         let throttler = self.retry_throttler.clone();
         let backoff = self.get_backoff_policy();
         let this = self.clone();
         let inner = async move |d| {
             let builder = builder
                 .try_clone()
                 .ok_or_else(|| Error::other("cannot clone builder in retry loop".to_string()))?;
             this.request_attempt(builder, d).await
         };
         let sleep = async |d| tokio::time::sleep(d).await;
         gax::retry_loop_internal::retry_loop(
             inner,
             sleep,
             true,
             throttler,
             retry_policy,
             backoff,
         )
         .await
     }
 
     async fn request_attempt<O: serde::de::DeserializeOwned>(
         &self,
         mut builder: reqwest::RequestBuilder,
         remaining_time: Option<std::time::Duration>,
     ) -> Result<O> {
         if let(Some(remaining_time)) = remaining_time {
             builder = builder.timeout(remaining_time);
         }
         
         let response = builder.send().await.map_err(Error::io)?;
         if !response.status().is_success() {
             return Self::to_http_error(response).await;
         }
         let response = response.json::<O>().await.map_err(Error::serde)?;
         Ok(response)
     }
 
     async fn to_http_error<O>(response: reqwest::Response) -> Result<O> {
         let status_code = response.status().as_u16();
         let headers = Self::convert_headers(response.headers());
         let body = response.bytes().await.map_err(Error::io)?;
         let error = if let Ok(status) = gax::error::rpc::Status::try_from(&body) {
             Error::rpc(
                 ServiceError::from(status)
                     .with_headers(headers)
                     .with_http_status_code(status_code),
             )
         } else {
             Error::rpc(HttpError::new(status_code, headers, Some(body)))
         };
         Err(error)
     }
 
     fn convert_headers(
         header_map: &reqwest::header::HeaderMap,
     ) -> std::collections::HashMap<String, String> {
         let mut headers = std::collections::HashMap::new();
         for (key, value) in header_map {
             if value.is_sensitive() {
                 headers.insert(key.to_string(), SENSITIVE_HEADER.to_string());
             } else if let Ok(value) = value.to_str() {
                 headers.insert(key.to_string(), value.to_string());
             }
         }
         headers
     }
 
     pub(crate) fn get_backoff_policy(
         &self,
     ) -> Arc<dyn BackoffPolicy> {
         self.backoff_policy.clone()
             .unwrap_or_else(|| Arc::new(ExponentialBackoff::default()))
     }
 }
 
 #[doc(hidden)]
 #[derive(serde::Serialize)]
 pub struct NoBody {}
 
 const SENSITIVE_HEADER: &str = "[sensitive]";