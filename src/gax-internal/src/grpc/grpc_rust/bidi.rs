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

pub use super::receive::ReceiveTask;
use bytes::{Buf, Bytes};
use grpc::core::SendMessage;

/// A [`SendMessage`] adapter that encodes Prost protobuf messages for `grpc-rust`.
pub struct GrpcRustSend<T>(pub T);

impl<T> SendMessage for GrpcRustSend<T>
where
    T: prost::Message,
{
    fn encode(&self) -> std::result::Result<Box<dyn Buf + Send + Sync>, String> {
        Ok(Box::new(Bytes::from(self.0.encode_to_vec())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use prost::Message;

    #[derive(Clone, PartialEq, prost::Message)]
    struct TestMessage {
        #[prost(string, tag = "1")]
        value: String,
    }

    #[test]
    fn prost_message_adapter_codecs() -> anyhow::Result<()> {
        // Arrange
        let want = TestMessage {
            value: "hello".to_string(),
        };

        // Act
        let mut encoded = GrpcRustSend(want.clone())
            .encode()
            .map_err(anyhow::Error::msg)?;
        let got = TestMessage::decode(encoded.copy_to_bytes(encoded.remaining()))?;

        // Assert
        assert_eq!(got, want);
        Ok(())
    }
}
