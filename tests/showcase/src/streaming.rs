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

use super::{Anonymous, NeverRetry, Result};
use google_cloud_gax::error::rpc::Code;
use google_cloud_gax::options::RequestOptionsBuilder as _;
use google_cloud_showcase_v1beta1::client::Echo;
use google_cloud_showcase_v1beta1::model::EchoRequest;
use google_cloud_wkt as wkt;

pub async fn run() -> Result<()> {
    let client = Echo::builder()
        .with_endpoint("http://localhost:7469")
        .with_credentials(Anonymous::new().build())
        .with_retry_policy(NeverRetry)
        .with_tracing()
        .build()
        .await?;

    chat_bidi_and_half_close(&client).await?;
    chat_send_before_recv(&client).await?;
    chat_zero_message_stream(&client).await?;
    chat_early_receiver_drop(&client).await?;
    chat_server_error(&client).await?;
    chat_options_and_headers(&client).await?;

    expand_happy_path(&client).await?;
    expand_empty_content(&client).await?;
    expand_early_receiver_drop(&client).await?;
    expand_stream_wait_time(&client).await?;
    expand_server_error(&client).await?;
    expand_options_and_headers(&client).await?;

    Ok(())
}

async fn chat_send_before_recv(client: &Echo) -> Result<()> {
    const TOTAL_MESSAGES: usize = 50;

    let (sender, mut resp_stream) = client.chat().build();

    for i in 0..TOTAL_MESSAGES {
        sender
            .send(EchoRequest::new().set_content(format!("burst-msg-{i}")))
            .await
            .expect("send should succeed even before receiver is polled");
    }
    drop(sender);

    let mut received = Vec::new();
    while let Some(res) = resp_stream.next().await {
        received.push(res?.content);
    }

    let expected: Vec<String> = (0..TOTAL_MESSAGES)
        .map(|i| format!("burst-msg-{i}"))
        .collect();
    assert_eq!(received, expected);
    Ok(())
}

async fn chat_bidi_and_half_close(client: &Echo) -> Result<()> {
    const TOTAL_MESSAGES: usize = 10;

    let (sender, mut resp_stream) = client.chat().build();

    let sender_handle = tokio::spawn(async move {
        for i in 0..TOTAL_MESSAGES {
            let req = EchoRequest::new().set_content(format!("concurrent-msg-{i}"));
            sender.send(req).await.expect("send should succeed");
            tokio::task::yield_now().await;
        }
        // Dropping sender triggers client half-close (EOF).
        drop(sender);
    });

    let mut received = Vec::new();
    while let Some(res) = resp_stream.next().await {
        received.push(res?.content);
    }

    sender_handle.await?;

    let expected: Vec<String> = (0..TOTAL_MESSAGES)
        .map(|i| format!("concurrent-msg-{i}"))
        .collect();
    assert_eq!(received, expected);

    // Further recv() calls on closed receiver must return None.
    assert!(resp_stream.next().await.is_none());
    assert!(resp_stream.next().await.is_none());

    Ok(())
}

async fn chat_server_error(client: &Echo) -> Result<()> {
    let (sender, mut resp_stream) = client.chat().build();

    // 1. First message should succeed.
    sender
        .send(EchoRequest::new().set_content("before-error"))
        .await?;
    let res = resp_stream.next().await.expect("expected response")?;
    assert_eq!(res.content, "before-error");

    // 2. Send request with injected error status.
    let error_request = EchoRequest::new().set_error(Box::new(
        google_cloud_rpc::model::Status::default()
            .set_code(Code::InvalidArgument as i32)
            .set_message("injected error for bidi test"),
    ));
    sender.send(error_request).await?;

    // 3. Server should return the error.
    let res = resp_stream
        .next()
        .await
        .expect("expected error item from receiver");
    let err = res.expect_err("response should be an error");
    let status = err
        .status()
        .expect("the error should include the service payload");
    assert_eq!(status.code, Code::InvalidArgument);
    assert_eq!(status.message.as_str(), "injected error for bidi test");

    // 4. Server stream should now be closed.
    assert!(resp_stream.next().await.is_none());

    // 5. Sending on stream after server termination should fail.
    let err = sender
        .send(EchoRequest::new().set_content("after-error"))
        .await
        .expect_err("sending on stream after server error termination should fail");
    assert!(matches!(
        err,
        google_cloud_gax::streaming::SendError::StreamClosed
    ));

    Ok(())
}

async fn chat_options_and_headers(client: &Echo) -> Result<()> {
    let header_name = http::header::HeaderName::from_static("x-custom-test-header");
    let header_value = http::header::HeaderValue::from_static("custom-header-value");
    let (sender, mut resp_stream) = client
        .chat()
        .with_request_stream_channel_capacity(8)
        .with_custom_header(header_name, header_value)
        .build();

    sender
        .send(EchoRequest::new().set_content("header-and-capacity-test"))
        .await?;
    let res = resp_stream.next().await.expect("expected response")?;
    assert_eq!(res.content, "header-and-capacity-test");
    drop(sender);
    assert!(resp_stream.next().await.is_none());

    Ok(())
}

async fn expand_happy_path(client: &Echo) -> Result<()> {
    let mut resp_stream = client
        .expand()
        .set_content("The quick brown fox jumps over the lazy dog")
        .send()
        .await?;

    let mut words = Vec::new();
    while let Some(res) = resp_stream.next().await {
        words.push(res?.content);
    }

    let expected = vec![
        "The", "quick", "brown", "fox", "jumps", "over", "the", "lazy", "dog",
    ];
    assert_eq!(words, expected);

    // Further recv() calls on closed receiver must return None.
    assert!(resp_stream.next().await.is_none());
    assert!(resp_stream.next().await.is_none());

    Ok(())
}

async fn expand_server_error(client: &Echo) -> Result<()> {
    let mut resp_stream = client
        .expand()
        .set_content("hello world")
        .set_error(
            google_cloud_rpc::model::Status::default()
                .set_code(Code::InvalidArgument as i32)
                .set_message("injected error for expand test"),
        )
        .send()
        .await?;

    // Words are streamed first.
    let first = resp_stream.next().await.expect("expected first word")?;
    assert_eq!(first.content, "hello");

    let second = resp_stream.next().await.expect("expected second word")?;
    assert_eq!(second.content, "world");

    // Server error is delivered after the words.
    let res = resp_stream
        .next()
        .await
        .expect("expected error item from receiver");
    let err = res.expect_err("response should be an error");
    let status = err
        .status()
        .expect("the error should include the service payload");
    assert_eq!(status.code, Code::InvalidArgument);
    assert_eq!(status.message.as_str(), "injected error for expand test");

    // Server stream should now be closed.
    assert!(resp_stream.next().await.is_none());

    Ok(())
}

async fn expand_options_and_headers(client: &Echo) -> Result<()> {
    let header_name = http::header::HeaderName::from_static("x-custom-test-header");
    let header_value = http::header::HeaderValue::from_static("custom-header-value");
    let mut resp_stream = client
        .expand()
        .with_custom_header(header_name, header_value)
        .set_content("header test")
        .send()
        .await?;

    let first = resp_stream.next().await.expect("expected first word")?;
    assert_eq!(first.content, "header");

    let second = resp_stream.next().await.expect("expected second word")?;
    assert_eq!(second.content, "test");

    assert!(resp_stream.next().await.is_none());

    Ok(())
}

async fn chat_zero_message_stream(client: &Echo) -> Result<()> {
    let (sender, mut resp_stream) = client.chat().build();
    drop(sender);

    assert!(resp_stream.next().await.is_none());

    Ok(())
}

async fn chat_early_receiver_drop(client: &Echo) -> Result<()> {
    let (sender, mut resp_stream) = client.chat().build();

    sender
        .send(EchoRequest::new().set_content("first-message"))
        .await?;
    let res = resp_stream.next().await.expect("expected response")?;
    assert_eq!(res.content, "first-message");

    drop(resp_stream);

    // Dropping the receiver closes the stream; subsequent sends may succeed into the
    // internal buffer or return an error if the background task has shut down.
    // Verify sending either succeeds or returns without panicking or deadlocking.
    let _ = sender
        .send(EchoRequest::new().set_content("second-message"))
        .await;

    Ok(())
}

async fn expand_empty_content(client: &Echo) -> Result<()> {
    let mut resp_stream = client.expand().set_content("").send().await?;

    assert!(resp_stream.next().await.is_none());

    Ok(())
}

async fn expand_early_receiver_drop(client: &Echo) -> Result<()> {
    let mut resp_stream = client
        .expand()
        .set_content("The quick brown fox jumps over the lazy dog")
        .send()
        .await?;

    let first = resp_stream.next().await.expect("expected first word")?;
    assert_eq!(first.content, "The");

    // Dropping receiver cancels the stream gracefully.
    drop(resp_stream);

    Ok(())
}

async fn expand_stream_wait_time(client: &Echo) -> Result<()> {
    let start = std::time::Instant::now();
    let mut resp_stream = client
        .expand()
        .set_content("one two three")
        .set_stream_wait_time(wkt::Duration::clamp(0, 50_000_000))
        .send()
        .await?;

    let mut words = Vec::new();
    while let Some(res) = resp_stream.next().await {
        words.push(res?.content);
    }

    assert_eq!(words, vec!["one", "two", "three"]);
    // 3 responses with 50ms pacing should take at least ~100ms
    assert!(start.elapsed() >= std::time::Duration::from_millis(100));

    Ok(())
}
