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

use futures::stream::unfold;
use futures::{Stream, StreamExt};
use pin_project::pin_project;
use std::future::Future;
use std::pin::Pin;

/// Describes a type that can be iterated over asyncly when used with [Paginator].
pub trait PageableResponse {
    fn next_page_token(&self) -> String;
}

/// An adapter that converts list RPCs as defined by [AIP-4233](https://google.aip.dev/client-libraries/4233)
/// into a [futures::Stream] that can be iterated over in an async fashion.
#[pin_project]
pub struct Paginator<T, E> {
    #[pin]
    stream: Pin<Box<dyn Stream<Item = Result<T, E>>>>,
}

impl<T, E> Paginator<T, E>
where
    T: PageableResponse,
{
    /// Creates a new [Paginator] given the initial page token and a function
    /// to fetch the next [PageableResponse].
    pub fn new<F>(seed_token: String, execute: impl Fn(String) -> F + Clone + 'static) -> Self
    where
        // TODO(codyoss): This, and other types, may also need send here in practice.
        F: Future<Output = Result<T, E>> + 'static,
    {
        let stream = unfold(IterState::Token(seed_token), move |state| {
            let execute = execute.clone();
            async move {
                let token = match state {
                    IterState::Token(token) => token,
                    IterState::End => return None,
                };
                let resp = match execute(token).await {
                    Ok(page_resp) => {
                        let tok = page_resp.next_page_token();
                        let next_state = if tok.is_empty() {
                            IterState::End
                        } else {
                            IterState::Token(tok)
                        };
                        Some((Ok(page_resp), next_state))
                    }
                    Err(e) => Some((Err(e), IterState::End)),
                };
                resp
            }
        });
        Self {
            stream: Box::pin(stream),
        }
    }

    /// Returns the next mutation of the wrapped stream.
    pub fn next(&mut self) -> futures::stream::Next<'_, Self> {
        StreamExt::next(self)
    }
}

enum IterState {
    Token(String),
    End,
}

impl<T, E> Stream for Paginator<T, E> {
    type Item = Result<T, E>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.project().stream.poll_next(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
        fn build_outgoing_list_request(&self) {
            // placeholder for building an http request
        }

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
            self.build_outgoing_list_request();
            let inner = self.inner.clone();
            Client::execute(inner.data.clone(), req).await
        }

        fn list_rpc_stream(
            &self,
            req: TestRequest,
        ) -> Paginator<TestResponse, Box<dyn std::error::Error>> {
            let client = self.clone();
            let tok = req.page_token.clone();
            let execute = move |token| {
                let mut req = req.clone();
                let client = client.clone();
                req.page_token = token;
                async move { client.list_rpc(req).await }
            };
            Paginator::new(tok, execute)
        }
    }

    #[tokio::test]
    async fn test_paginator() {
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
        let mut stream = client.list_rpc_stream(TestRequest::default());
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

    #[tokio::test]
    async fn test_paginator_error() {
        let execute = |_| async { Err::<TestResponse, Box<dyn std::error::Error>>("err".into()) };

        let mut paginator = Paginator::new(String::new(), execute);
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
}
