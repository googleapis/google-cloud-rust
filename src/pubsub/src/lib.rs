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

//! Google Cloud Client Libraries for Rust - Pub/Sub
//!
//! **WARNING:** this crate is under active development. We expect multiple
//! breaking changes in the upcoming releases. Testing is also incomplete, we do
//! **not** recommend that you use this crate in production. We welcome feedback
//! about the APIs, documentation, missing features, bugs, etc.
//!
//! This crate contains traits, types, and functions to interact with
//! [Pub/Sub]. Most applications will use the structs defined in the
//! [client] module.
//!
//! For administrative operations:
//! * [TopicAdmin][client::TopicAdmin]
//! * [SubscriptionAdmin][client::SubscriptionAdmin]
//! * [SchemaService][client::SchemaService]
//!
//! For publishing messages:
//! * [BasePublisher][client::BasePublisher] and [Publisher][client::Publisher]
//!
//! For receiving messages:
//! * [Subscriber][client::Subscriber]
//!
//! Receiving messages is not yet supported by this crate.
//!
//! **NOTE:** This crate used to contain a different implementation, with a
//! different surface. [@yoshidan](https://github.com/yoshidan) generously
//! donated the crate name to Google. Their crate continues to live as
//! [gcloud-pubsub].
//!
//! [pub/sub]: https://cloud.google.com/pubsub
//! [gcloud-pubsub]: https://crates.io/crates/gcloud-pubsub

#[allow(rustdoc::broken_intra_doc_links)]
pub(crate) mod generated;

pub(crate) mod publisher;
/// Types related to receiving messages with a [Subscriber][client::Subscriber]
/// client.
pub mod subscriber;

pub use gax::Result;
pub use gax::error::Error;

/// Request and client builders.
pub mod builder {
    /// Request and client builders for the [Publisher][crate::client::Publisher] client.
    pub mod publisher {
        // TODO(#3959) - remove internal types from the public API.
        #[doc(hidden)]
        pub use crate::generated::gapic_dataplane::builder::publisher::*;
        pub use crate::publisher::base_publisher::BasePublisherBuilder;
        pub use crate::publisher::publisher::PublisherPartialBuilder;
    }
    /// Request and client builders for the [SchemaService][crate::client::SchemaService] client.
    pub use crate::generated::gapic::builder::schema_service;
    /// Request and client builders for the [Subscriber][crate::client::Subscriber] client.
    pub mod subscriber {
        // TODO(#3959) - remove internal types from the public API.
        #[doc(hidden)]
        pub use crate::generated::gapic_dataplane::builder::subscriber::*;
        pub use crate::subscriber::builder::StreamingPull;
        pub use crate::subscriber::client_builder::ClientBuilder;
    }
    /// Request and client builders for the [SubscriptionAdmin][crate::client::SubscriptionAdmin] client.
    pub use crate::generated::gapic::builder::subscription_admin;
    /// Request and client builders for the [TopicAdmin][crate::client::TopicAdmin] client.
    pub use crate::generated::gapic::builder::topic_admin;
}

/// The messages and enums that are part of this client library.
pub mod model {
    pub use crate::generated::gapic::model::*;
    pub use crate::generated::gapic_dataplane::model::PubsubMessage;
    pub(crate) use crate::generated::gapic_dataplane::model::*;
}

/// Extends [model] with types that improve type safety and/or ergonomics.
pub mod model_ext {
    pub use crate::publisher::model_ext::*;
}

/// Clients to interact with Google Cloud Pub/Sub.
///
/// This module contains the primary entry points for the library, including
/// clients for publishing messages and managing topics and subscriptions.
///
/// # Example: Publishing Messages
///
/// ```
/// # async fn sample() -> anyhow::Result<()> {
/// use google_cloud_pubsub::client::BasePublisher;
/// use google_cloud_pubsub::model::PubsubMessage;
///
/// // Create a client for creating publishers.
/// let client = BasePublisher::builder().build().await?;
///
/// // Create a publisher that handles batching for a specific topic.
/// let publisher = client.publisher("projects/my-project/topics/my-topic").build();
///
/// // Publish several messages.
/// // The client will automatically batch them in the background.
/// let mut handles = Vec::new();
/// for i in 0..10 {
///     let msg = PubsubMessage::new().set_data(format!("message {}", i));
///     handles.push(publisher.publish(msg));
/// }
///
/// // The handles are futures that resolve to the server-assigned message IDs.
/// // You can await them to get the results. Messages will still be sent even
/// // if the handles are dropped.
/// for (i, handle) in handles.into_iter().enumerate() {
///     let message_id = handle.await?;
///     println!("Message {} sent with ID: {}", i, message_id);
/// }
/// # Ok(())
/// # }
/// ```
pub mod client {
    pub use crate::generated::gapic::client::*;
    pub use crate::publisher::base_publisher::BasePublisher;
    pub use crate::publisher::publisher::Publisher;
    pub use crate::subscriber::client::Subscriber;
}

pub mod error;

/// Traits to mock the clients in this library.
pub mod stub {
    pub use crate::generated::gapic::stub::*;
}

const DEFAULT_HOST: &str = "https://pubsub.googleapis.com";

mod info {
    const NAME: &str = env!("CARGO_PKG_NAME");
    const VERSION: &str = env!("CARGO_PKG_VERSION");
    lazy_static::lazy_static! {
        pub(crate) static ref X_GOOG_API_CLIENT_HEADER: String = {
            let ac = gaxi::api_header::XGoogApiClient{
                name:          NAME,
                version:       VERSION,
                library_type:  gaxi::api_header::GAPIC,
            };
            ac.grpc_header_value()
        };
    }
}

#[allow(dead_code)]
pub(crate) mod google {
    pub mod pubsub {
        #[allow(clippy::enum_variant_names)]
        pub mod v1 {
            include!("generated/protos/pubsub/google.pubsub.v1.rs");
            include!("generated/convert/pubsub/convert.rs");
        }
    }
}
