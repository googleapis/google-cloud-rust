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

#![cfg(google_cloud_unstable_gapic_streaming)]

use super::{Anonymous, NeverRetry, Result};
use google_cloud_gax::options::RequestOptions;
use google_cloud_showcase_v1beta1::client::Messaging;
use google_cloud_showcase_v1beta1::model::connect_request::ConnectConfig;
use google_cloud_showcase_v1beta1::model::{Blurb, ConnectRequest, Room};

pub async fn run() -> Result<()> {
    let client = Messaging::builder()
        .with_endpoint("http://localhost:7469")
        .with_credentials(Anonymous::new().build())
        .with_retry_policy(NeverRetry)
        .build()
        .await?;

    connect(&client).await?;
    Ok(())
}

async fn connect(client: &Messaging) -> Result<()> {
    // 1. Create a room to get a valid parent resource
    let room = client
        .create_room()
        .set_room(Room::new().set_display_name("Bidi Test Room"))
        .send()
        .await?;

    // 2. Open bidi stream
    let (sender, mut receiver) = client.connect(RequestOptions::default()).await;

    // 3. Send initial ConnectConfig with valid parent room name
    let config_req = ConnectRequest::new().set_config(ConnectConfig::new().set_parent(&room.name));
    sender.send(config_req).await?;

    // 4. Send a Blurb message over the stream
    let blurb_req = ConnectRequest::new().set_blurb(
        Blurb::new()
            .set_user("users/123")
            .set_text("Hello from bidi streaming!"),
    );
    sender.send(blurb_req).await?;

    // 5. Receive response from stream
    if let Some(res) = receiver.recv().await {
        let response = res?;
        println!(
            "Successfully received bidi streaming response: {:?}",
            response
        );
    }

    Ok(())
}
