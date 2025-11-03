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

/// Options for configurating publisher batching behavior.
///
/// To turn off batching, set the value of message_count_threshold to 1.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct BatchingOptions {
    pub message_count_threshold: u32,
    pub(crate) byte_threshold: u32,
    pub(crate) delay_threshold: std::time::Duration,
}

impl BatchingOptions {
    /// Create a new instance.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the [BatchingOptions][Self::message_count_threshold] field.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_pubsub::options::publisher::BatchingOptions;
    /// let options = BatchingOptions::new().set_message_count_threshold(100_u32);
    /// ```
    pub fn set_message_count_threshold<V: Into<u32>>(mut self, v: V) -> Self {
        self.message_count_threshold = v.into();
        self
    }

    /// Set the [BatchingOptions][Self::byte_threshold] field.
    ///
    /// # Example
    /// ```ignore
    /// # use google_cloud_pubsub::options::publisher::BatchingOptions;
    /// let options = BatchingOptions::new().set_byte_threshold(1000_u32);
    /// ```
    // TODO(#3686): support byte thresholds.
    #[allow(dead_code)]
    pub(crate) fn set_byte_threshold<V: Into<u32>>(mut self, v: V) -> Self {
        self.byte_threshold = v.into();
        self
    }

    /// Set the [BatchingOptions][Self::delay_threshold] field.
    ///
    /// # Example
    ///
    /// ```ignore
    /// # use google_cloud_pubsub::options::publisher::BatchingOptions;
    /// let options = BatchingOptions::new().set_delay_threshold(std::time::Duration::from_millis(10));
    /// ```
    // TODO(#3687): support delay thresholds.
    #[allow(dead_code)]
    pub(crate) fn set_delay_threshold<V: Into<std::time::Duration>>(mut self, v: V) -> Self {
        self.delay_threshold = v.into();
        self
    }
}

impl std::default::Default for BatchingOptions {
    fn default() -> Self {
        Self {
            message_count_threshold: 100_u32,
            byte_threshold: 1000_u32,
            delay_threshold: std::time::Duration::from_millis(10),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::BatchingOptions;

    #[tokio::test]
    async fn batching_options() -> anyhow::Result<()> {
        let options = BatchingOptions::new()
            .set_byte_threshold(1_234_u32)
            .set_message_count_threshold(123_u32)
            .set_delay_threshold(std::time::Duration::from_millis(12));
        assert_eq!(options.byte_threshold, 1_234_u32);
        assert_eq!(options.message_count_threshold, 123_u32);
        assert_eq!(
            options.delay_threshold,
            std::time::Duration::from_millis(12)
        );
        Ok(())
    }
}
