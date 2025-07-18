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

//! Defines some types and traits to simplify working with list RPCs.
//!
//! When listing large collections of items Google Cloud services typically
//! break the responses in "pages". Each one of these pages contains a limited
//! number of items, and a token to request the next page. The caller must
//! repeatedly call the list RPC (using the token from the previous page) to
//! obtain additional items.
//!
//! Sequencing these calls can be tedious. The Google Cloud client libraries
//! for Rust provide implementations of this trait to simplify the use of
//! these paginated APIs.
//!
//! For more information on the RPCs with paginated responses see [AIP-4233].
//!
//! # Example: stream each item for a list operation
//! ```no_run
//! # use google_cloud_gax::{paginator, Result, error::Error};
//! struct Page { items: Vec<String>, token: String }
//! # impl paginator::internal::PageableResponse for Page {
//! #     type PageItem = String;
//! #     fn items(self) -> Vec<String> { self.items }
//! #     fn next_page_token(&self) -> String { self.token.clone() }
//! # }
//! # async fn get_page(_: String) -> Result<Page> { panic!(); }
//! use paginator::{Paginator, ItemPaginator};
//! fn list() -> impl Paginator<Page, Error> {
//!     // ... details omitted ...
//!     # fn execute(_: String) -> Result<Page> { panic!(); }
//!     # paginator::internal::new_paginator(String::new(), get_page)
//! }
//! # tokio_test::block_on(async {
//! let mut items = list().items();
//! while let Some(item) = items.next().await {
//!     let item = item?;
//!     println!("  item = {item}");
//! }
//! # Result::<()>::Ok(()) });
//! ```
//!
//! # Example: manually iterate over all the pages for a list operation
//! ```no_run
//! # use google_cloud_gax::{paginator, Result, error::Error};
//! struct Page { items: Vec<String>, token: String }
//! # impl paginator::internal::PageableResponse for Page {
//! #     type PageItem = String;
//! #     fn items(self) -> Vec<String> { self.items }
//! #     fn next_page_token(&self) -> String { self.token.clone() }
//! # }
//! # async fn get_page(_: String) -> Result<Page> { panic!(); }
//! use paginator::Paginator;
//! fn list() -> impl Paginator<Page, Error> {
//!     // ... details omitted ...
//!     # fn execute(_: String) -> Result<Page> { panic!(); }
//!     # paginator::internal::new_paginator(String::new(), get_page)
//! }
//! # tokio_test::block_on(async {
//! let mut pages = list();
//! while let Some(page) = pages.next().await {
//!     let page = page?;
//!     println!("a page with {} items", page.items.len());
//!     page.items.into_iter().enumerate().for_each(|(n, i)| println!("  item[{n}]= {i}"));
//! }
//! # Result::<()>::Ok(()) });
//! ```
//!
//! [AIP-4233]: https://google.aip.dev/client-libraries/4233

use futures::stream::unfold;
use futures::{Stream, StreamExt};
use pin_project::pin_project;
use std::future::Future;
use std::pin::Pin;

#[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
pub mod internal {
    //! This module contains implementation details. It is not part of the
    //! public API. Types and functions in this module may be changed or removed
    //! without warnings. Applications should not use any types contained
    //! within.
    use super::*;

    /// Describes a type that can be iterated over asynchronously when used with
    /// [super::Paginator].
    pub trait PageableResponse {
        type PageItem: Send;

        // Consumes the [PageableResponse] and returns the items associated with the
        // current page.
        fn items(self) -> Vec<Self::PageItem>;

        /// Returns the next page token.
        fn next_page_token(&self) -> String;
    }

    /// Creates a new `impl Paginator<T, E>` given the initial page token and a function
    /// to fetch the next response.
    pub fn new_paginator<T, E, F>(
        seed_token: String,
        execute: impl Fn(String) -> F + Clone + Send + 'static,
    ) -> impl Paginator<T, E>
    where
        T: internal::PageableResponse,
        F: Future<Output = Result<T, E>> + Send + 'static,
    {
        PaginatorImpl::new(seed_token, execute)
    }
}

mod sealed {
    pub trait Paginator {}
}

/// Automatically streams pages in list RPCs.
///
/// When listing large collections of items Google Cloud services typically
/// break the responses in "pages", where each page contains a limited number
/// of items, and a token to request the next page. The caller must
/// repeatedly call the list RPC (using the token from the previous page) to
/// obtain additional items.
///
/// Sequencing these calls can be tedious. The Google Cloud client libraries
/// for Rust provide implementations of this trait to simplify the use of
/// these paginated APIs.
///
/// Enable the `unstable-stream` feature to convert this type to a [Stream].
///
/// For more information on the RPCs with paginated responses see [AIP-4233].
///
/// [AIP-4233]: https://google.aip.dev/client-libraries/4233
pub trait Paginator<PageType, Error>: Send + sealed::Paginator
where
    PageType: internal::PageableResponse,
{
    /// Creates a new [ItemPaginator] from an existing [Paginator].
    fn items(self) -> impl ItemPaginator<PageType, Error>;

    /// Returns the next mutation of the wrapped stream.
    fn next(&mut self) -> impl Future<Output = Option<Result<PageType, Error>>> + Send;

    #[cfg(feature = "unstable-stream")]
    #[cfg_attr(docsrs, doc(cfg(feature = "unstable-stream")))]
    /// Convert the paginator to a [Stream].
    fn into_stream(self) -> impl futures::Stream<Item = Result<PageType, Error>> + Unpin;
}

#[pin_project]
struct PaginatorImpl<T, E> {
    #[pin]
    stream: Pin<Box<dyn Stream<Item = Result<T, E>> + Send>>,
}

type ControlFlow = std::ops::ControlFlow<(), String>;

impl<T, E> PaginatorImpl<T, E>
where
    T: internal::PageableResponse,
{
    /// Creates a new [Paginator] given the initial page token and a function
    /// to fetch the next response.
    pub fn new<F>(
        seed_token: String,
        execute: impl Fn(String) -> F + Clone + Send + 'static,
    ) -> Self
    where
        F: Future<Output = Result<T, E>> + Send + 'static,
    {
        let stream = unfold(ControlFlow::Continue(seed_token), move |state| {
            let execute = execute.clone();
            async move {
                let token = match state {
                    ControlFlow::Continue(token) => token,
                    ControlFlow::Break(_) => return None,
                };
                match execute(token).await {
                    Ok(page_resp) => {
                        let tok = page_resp.next_page_token();
                        let next_state = if tok.is_empty() {
                            ControlFlow::Break(())
                        } else {
                            ControlFlow::Continue(tok)
                        };
                        Some((Ok(page_resp), next_state))
                    }
                    Err(e) => Some((Err(e), ControlFlow::Break(()))),
                }
            }
        });
        Self {
            stream: Box::pin(stream),
        }
    }
}

impl<T, E> Paginator<T, E> for PaginatorImpl<T, E>
where
    T: internal::PageableResponse,
{
    /// Creates a new [ItemPaginator] from an existing [Paginator].
    fn items(self) -> impl ItemPaginator<T, E> {
        ItemPaginatorImpl::new(self)
    }

    /// Returns the next mutation of the wrapped stream.
    async fn next(&mut self) -> Option<Result<T, E>> {
        self.stream.next().await
    }

    #[cfg(feature = "unstable-stream")]
    #[cfg_attr(docsrs, doc(cfg(feature = "unstable-stream")))]
    /// Convert the paginator to a [Stream][futures::stream::Stream].
    fn into_stream(self) -> impl futures::Stream<Item = Result<T, E>> + Unpin {
        Box::pin(unfold(Some(self), move |state| async move {
            if let Some(mut paginator) = state {
                if let Some(pr) = paginator.next().await {
                    return Some((pr, Some(paginator)));
                }
            };
            None
        }))
    }
}

impl<T, E> sealed::Paginator for PaginatorImpl<T, E> where T: internal::PageableResponse {}

impl<T, E> std::fmt::Debug for PaginatorImpl<T, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Paginator").finish()
    }
}

/// Automatically stream items in list RPCs.
///
/// When listing large collections of items Google Cloud services typically
/// break the responses in "pages", where each page contains a subset of
/// the items, and a token to request the next page. The caller must
/// repeatedly call the list RPC using the token from the previous page.
///
/// Sequencing these calls can be tedious. The Google Cloud client libraries
/// for rust provide implementations of this trait to simplify the use of
/// these paginated APIs.
///
/// Many applications want to operate on one item at a time. The Google Cloud
/// client libraries for Rust provide implementations of this trait that
/// automatically iterate over each item and then fetch the next page, stopping
/// when all the pages are processed.
///
/// Enable the `unstable-stream` feature to convert this type to a [Stream].
///
/// For more information on the RPCs with paginated responses see [AIP-4233].
///
/// [AIP-4233]: https://google.aip.dev/client-libraries/4233
pub trait ItemPaginator<PageType, Error>: Send + sealed::Paginator
where
    PageType: internal::PageableResponse,
{
    /// Returns the next item from the stream of (paginated) responses.
    fn next(&mut self) -> impl Future<Output = Option<Result<PageType::PageItem, Error>>> + Send;

    #[cfg(feature = "unstable-stream")]
    #[cfg_attr(docsrs, doc(cfg(feature = "unstable-stream")))]
    /// Convert the paginator to a [Stream].
    fn into_stream(self) -> impl futures::Stream<Item = Result<PageType::PageItem, Error>> + Unpin;
}

#[pin_project]
struct ItemPaginatorImpl<T, E>
where
    T: internal::PageableResponse,
{
    #[pin]
    stream: PaginatorImpl<T, E>,
    current_items: Option<std::vec::IntoIter<T::PageItem>>,
}

impl<T, E> ItemPaginatorImpl<T, E>
where
    T: internal::PageableResponse,
{
    /// Creates a new [ItemPaginator] from an existing [Paginator].
    fn new(paginator: PaginatorImpl<T, E>) -> Self {
        Self {
            stream: paginator,
            current_items: None,
        }
    }
}

impl<T, E> ItemPaginator<T, E> for ItemPaginatorImpl<T, E>
where
    T: internal::PageableResponse,
{
    /// Returns the next item in the stream of (paginated) responses.
    ///
    /// Enable the `unstable-stream` feature to convert this to a [Stream].
    async fn next(&mut self) -> Option<Result<T::PageItem, E>> {
        loop {
            if let Some(ref mut iter) = self.current_items {
                if let Some(item) = iter.next() {
                    return Some(Ok(item));
                }
            }

            let next_page = self.stream.next().await;
            match next_page {
                Some(Ok(page)) => {
                    self.current_items = Some(page.items().into_iter());
                }
                Some(Err(e)) => {
                    return Some(Err(e));
                }
                None => return None,
            }
        }
    }

    #[cfg(feature = "unstable-stream")]
    #[cfg_attr(docsrs, doc(cfg(feature = "unstable-stream")))]
    /// Convert the paginator to a [Stream].
    fn into_stream(self) -> impl Stream<Item = Result<T::PageItem, E>> + Unpin {
        Box::pin(unfold(Some(self), move |state| async move {
            if let Some(mut paginator) = state {
                if let Some(pr) = paginator.next().await {
                    return Some((pr, Some(paginator)));
                }
            };
            None
        }))
    }
}

impl<T, E> sealed::Paginator for ItemPaginatorImpl<T, E> where T: internal::PageableResponse {}

#[cfg(test)]
mod tests {
    use super::internal::*;
    use super::*;
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Default)]
    struct TestRequest {
        page_token: String,
    }

    struct TestResponse {
        items: Vec<PageItem>,
        next_page_token: String,
    }

    #[derive(Clone)]
    struct PageItem {
        name: String,
    }

    impl PageableResponse for TestResponse {
        type PageItem = PageItem;

        fn items(self) -> Vec<Self::PageItem> {
            self.items
        }

        fn next_page_token(&self) -> String {
            self.next_page_token.clone()
        }
    }

    #[derive(Clone)]
    struct Client {
        inner: Arc<InnerClient>,
    }

    struct InnerClient {
        data: Arc<Mutex<Vec<TestResponse>>>,
    }

    impl Client {
        async fn execute(
            data: Arc<Mutex<Vec<TestResponse>>>,
            _: TestRequest,
        ) -> Result<TestResponse, Box<dyn std::error::Error>> {
            // This is where we could run a request with a client
            let mut responses = data.lock().unwrap();
            let resp: TestResponse = responses.remove(0);
            Ok(resp)
        }

        async fn list_rpc(
            &self,
            req: TestRequest,
        ) -> Result<TestResponse, Box<dyn std::error::Error>> {
            let inner = self.inner.clone();
            Client::execute(inner.data.clone(), req).await
        }

        fn list_rpc_stream(
            &self,
            req: TestRequest,
        ) -> impl Paginator<TestResponse, Box<dyn std::error::Error>> {
            let client = self.clone();
            let tok = req.page_token.clone();
            let execute = move |token| {
                let mut req = req.clone();
                let client = client.clone();
                req.page_token = token;
                async move { client.list_rpc(req).await }
            };
            new_paginator(tok, execute)
        }
    }

    #[tokio::test]
    async fn test_pagination() {
        let seed = "token1".to_string();
        let mut responses = VecDeque::new();
        responses.push_back(TestResponse {
            items: vec![
                PageItem {
                    name: "item1".to_string(),
                },
                PageItem {
                    name: "item2".to_string(),
                },
            ],
            next_page_token: "token2".to_string(),
        });
        responses.push_back(TestResponse {
            items: vec![PageItem {
                name: "item3".to_string(),
            }],
            next_page_token: "".to_string(),
        });
        let mut expected_tokens = VecDeque::new();
        expected_tokens.push_back("token1".to_string());
        expected_tokens.push_back("token2".to_string());

        let state = Arc::new(Mutex::new(responses));
        let tokens = Arc::new(Mutex::new(expected_tokens));

        let execute = move |token: String| {
            let expected_token = tokens.clone().lock().unwrap().pop_front().unwrap();
            assert_eq!(token, expected_token);
            let resp: TestResponse = state.clone().lock().unwrap().pop_front().unwrap();
            async move { Ok::<_, Box<dyn std::error::Error>>(resp) }
        };

        let mut resps = vec![];
        let mut paginator = new_paginator(seed, execute);
        while let Some(resp) = paginator.next().await {
            if let Ok(resp) = resp {
                resps.push(resp)
            }
        }
        assert_eq!(resps.len(), 2);
        assert_eq!(resps[0].items[0].name, "item1");
        assert_eq!(resps[0].items[1].name, "item2");
    }

    #[tokio::test]
    async fn test_paginator_as_client() {
        let responses = vec![
            TestResponse {
                items: vec![
                    PageItem {
                        name: "item1".to_string(),
                    },
                    PageItem {
                        name: "item2".to_string(),
                    },
                ],
                next_page_token: "token1".to_string(),
            },
            TestResponse {
                items: vec![PageItem {
                    name: "item3".to_string(),
                }],
                next_page_token: "".to_string(),
            },
        ];

        let client = Client {
            inner: Arc::new(InnerClient {
                data: Arc::new(Mutex::new(responses)),
            }),
        };
        let mut resps = vec![];
        let mut paginator = client.list_rpc_stream(TestRequest::default());
        while let Some(resp) = paginator.next().await {
            if let Ok(resp) = resp {
                resps.push(resp)
            }
        }
        assert_eq!(resps.len(), 2);
        assert_eq!(resps[0].items[0].name, "item1");
        assert_eq!(resps[0].items[1].name, "item2");
        assert_eq!(resps[1].items[0].name, "item3");
    }

    #[tokio::test]
    async fn test_paginator_error() {
        let execute = |_| async { Err::<TestResponse, Box<dyn std::error::Error>>("err".into()) };

        let mut paginator = new_paginator(String::new(), execute);
        let mut count = 0;
        while let Some(resp) = paginator.next().await {
            match resp {
                Ok(_) => {
                    panic!("Should not succeed");
                }
                Err(e) => {
                    assert_eq!(e.to_string(), "err");
                    count += 1;
                }
            }
        }
        assert_eq!(count, 1);
    }

    #[cfg(feature = "unstable-stream")]
    #[tokio::test]
    async fn test_paginator_into_stream() {
        let responses = vec![
            TestResponse {
                items: vec![
                    PageItem {
                        name: "item1".to_string(),
                    },
                    PageItem {
                        name: "item2".to_string(),
                    },
                ],
                next_page_token: "token1".to_string(),
            },
            TestResponse {
                items: vec![PageItem {
                    name: "item3".to_string(),
                }],
                next_page_token: "".to_string(),
            },
        ];

        let client = Client {
            inner: Arc::new(InnerClient {
                data: Arc::new(Mutex::new(responses)),
            }),
        };
        let mut resps = vec![];
        let mut stream = client.list_rpc_stream(TestRequest::default()).into_stream();
        while let Some(resp) = stream.next().await {
            if let Ok(resp) = resp {
                resps.push(resp)
            }
        }
        assert_eq!(resps.len(), 2);
        assert_eq!(resps[0].items[0].name, "item1");
        assert_eq!(resps[0].items[1].name, "item2");
        assert_eq!(resps[1].items[0].name, "item3");
    }

    #[cfg(feature = "unstable-stream")]
    #[tokio::test]
    async fn test_item_paginator_into_stream() {
        let responses = vec![
            TestResponse {
                items: vec![
                    PageItem {
                        name: "item1".to_string(),
                    },
                    PageItem {
                        name: "item2".to_string(),
                    },
                ],
                next_page_token: "token1".to_string(),
            },
            TestResponse {
                items: vec![PageItem {
                    name: "item3".to_string(),
                }],
                next_page_token: "".to_string(),
            },
        ];

        let client = Client {
            inner: Arc::new(InnerClient {
                data: Arc::new(Mutex::new(responses)),
            }),
        };
        let mut items = vec![];
        let mut stream = client
            .list_rpc_stream(TestRequest::default())
            .items()
            .into_stream();
        while let Some(item) = stream.next().await {
            if let Ok(item) = item {
                items.push(item)
            }
        }
        assert_eq!(items.len(), 3);
        assert_eq!(items[0].name, "item1");
        assert_eq!(items[1].name, "item2");
        assert_eq!(items[2].name, "item3");
    }
}
