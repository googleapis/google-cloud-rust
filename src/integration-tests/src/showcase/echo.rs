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

use crate::Result;

const CONTENT: &str = r#"
Four score and seven years ago our fathers brought forth on this continent a new
nation, conceived in liberty, and dedicated to the proposition that all men are
created equal.

Now we are engaged in a great civil war, testing whether that nation, or any
nation so conceived and so dedicated, can long endure. We are met on a great
battlefield of that war. We have come to dedicate a portion of that field as a
final resting place for those who here gave their lives that that nation might
live. It is altogether fitting and proper that we should do this.

But in a larger sense we cannot dedicate, we cannot consecrate, we cannot hallow
this ground. The brave men, living and dead, who struggled here have consecrated
it, far above our poor power to add or detract. The world will little note, nor
long remember, what we say here, but it can never forget what they did here. It
is for us the living, rather, to be dedicated here to the unfinished work which
they who fought here have thus far so nobly advanced. It is rather for us to be
here dedicated to the great task remaining before us,that from these honored
dead we take increased devotion to that cause for which they gave the last full
measure of devotion, that we here highly resolve that these dead shall not have
died in vain, that this nation, under God, shall have a new birth of freedom,
and that government of the people, by the people, for the people, shall not
perish from the earth.
"#;

pub async fn run() -> Result<()> {
    let client = showcase::client::Echo::builder()
        .with_endpoint("http://localhost:7469")
        .with_credentials(auth::credentials::testing::test_credentials())
        .with_retry_policy(gax::retry_policy::NeverRetry)
        .with_tracing()
        .build()
        .await?;

    // TODO(#2202) - enums don't work: echo(&client).await?;
    echo_error_details(&client).await?;
    fail_echo_with_details(&client).await?;
    paged_expand(&client).await?;
    paged_expand_legacy(&client).await?;
    paged_expand_mapped(&client).await?;
    // Wait() tests timeouts, which we already have tests for.
    // Block() tests timeouts, which we already have tests for.
    request_id_unset(&client).await?;
    request_id_custom(&client).await?;

    Ok(())
}

async fn echo_error_details(client: &showcase::client::Echo) -> Result<()> {
    use gax::error::rpc::StatusDetails;

    const TEXT: &str = "the quick brown fox jumps over the lazy dog";
    let response = client
        .echo_error_details()
        .set_single_detail_text(TEXT)
        .set_multi_detail_text([TEXT, TEXT])
        .send()
        .await?;
    let any = response
        .single_detail
        .and_then(|f| f.error)
        .and_then(|f| f.details)
        .expect("has single_detail with error and any");
    if let StatusDetails::ErrorInfo(info) = StatusDetails::from(any) {
        assert_eq!(info.reason.as_str(), TEXT);
    }
    Ok(())
}

async fn fail_echo_with_details(client: &showcase::client::Echo) -> Result<()> {
    use gax::error::rpc::{Code, StatusDetails};
    const LINE: &str =
        "It matters not how strait the gate, How charged with punishments the scroll,";
    let result = client
        .fail_echo_with_details()
        .set_message(LINE)
        .send()
        .await;
    // This request should always fail and return an error with details.
    let err = result.unwrap_err();
    let status = err
        .status()
        .expect("the error should include the service payload");

    // Prints the detailed error, in debug format, formatted over multiple lines.
    println!("{err:#?}");

    // Manually access each error detail.
    assert_eq!(status.code, Code::Aborted);
    for detail in status.details.iter() {
        match detail {
            StatusDetails::Other(any) => {
                let poetry = any
                    .to_msg::<showcase::model::PoetryError>()
                    .expect("extraction should succeed");
                assert_eq!(poetry.poem, LINE);
            }
            StatusDetails::LocalizedMessage(m) => {
                assert_eq!(m.locale, "fr-CH");
                assert!(!m.message.is_empty(), "{m:?}");
            }
            StatusDetails::Help(h) => {
                let descriptions = h
                    .links
                    .iter()
                    .map(|l| l.description.as_str())
                    .collect::<Vec<_>>();
                assert_eq!(
                    descriptions,
                    vec![
                        "Help: first showcase help link",
                        "Help: second showcase help link"
                    ]
                );
            }
            _ => {}
        }
    }

    Ok(())
}

async fn paged_expand(client: &showcase::client::Echo) -> Result<()> {
    use gax::paginator::ItemPaginator;
    let mut words = Vec::new();
    let mut items = client.paged_expand().set_content(CONTENT).by_item();
    while let Some(w) = items.next().await {
        words.push(w?);
    }
    assert!(!words.is_empty(), "{words:?}");
    Ok(())
}

async fn paged_expand_legacy(client: &showcase::client::Echo) -> Result<()> {
    use gax::paginator::ItemPaginator;
    let mut words = Vec::new();
    let mut items = client.paged_expand_legacy().set_content(CONTENT).by_item();
    while let Some(w) = items.next().await {
        words.push(w?);
    }
    assert!(!words.is_empty(), "{words:?}");
    Ok(())
}

async fn paged_expand_mapped(client: &showcase::client::Echo) -> Result<()> {
    let response = client
        .paged_expand_legacy_mapped()
        .set_content(CONTENT)
        .send()
        .await?;
    assert!(!response.alphabetized.is_empty(), "{response:?}");
    Ok(())
}

// Verify that the library auto-populates request IDs
async fn request_id_unset(client: &showcase::client::Echo) -> Result<()> {
    let response = client.echo().set_content(CONTENT).send().await?;
    let uuid1 = uuid::Uuid::try_parse(&response.request_id)?;
    let uuid2 = uuid::Uuid::try_parse(&response.other_request_id)?;
    assert!(!uuid1.is_nil());
    assert!(!uuid2.is_nil());
    Ok(())
}

// Verify that the library preserves user-supplied request IDs
async fn request_id_custom(client: &showcase::client::Echo) -> Result<()> {
    let uuid1 = "92500ce6-fba2-4fc5-92ad-b7250282c2fc";
    let uuid2 = "7289ea3a-7f36-44e7-ac2f-3d906f199c3c";

    let response = client
        .echo()
        .set_content(CONTENT)
        .set_request_id(uuid1)
        .set_other_request_id(uuid2)
        .send()
        .await?;
    assert_eq!(uuid1, response.request_id);
    assert_eq!(uuid2, response.other_request_id);
    Ok(())
}
