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

use std::time::Duration;

pub(crate) const MAX_DELAY: Duration = Duration::from_secs(60 * 60 * 24); // 1 day
// These limits come from https://cloud.google.com/pubsub/docs/batch-messaging#quotas_and_limits_on_batch_messaging.
// Client libraries are expected to enforce these limits on batch siziing.
pub(crate) const MAX_MESSAGES: u32 = 1000;
pub(crate) const MAX_BYTES: u32 = 1e7 as u32; // 10MB

#[allow(dead_code)]
pub(crate) const DEFAULT_HEDGING_DELAY: std::time::Duration = std::time::Duration::from_secs(1);
pub(crate) const MIN_HEDGING_DELAY: Duration = Duration::from_millis(100);
pub(crate) const MAX_HEDGING_DELAY: Duration = Duration::from_secs(10);
#[allow(dead_code)]
pub(crate) const DEFAULT_HEDGING_MAX_TOKENS: u32 = 50_u32;
pub(crate) const MIN_HEDGING_MAX_TOKENS: u32 = 1;
pub(crate) const MAX_HEDGING_MAX_TOKENS: u32 = 250;
pub(crate) const DEFAULT_HEDGING_REFILL_RATIO: f32 = 0.1_f32;
pub(crate) const MIN_HEDGING_REFILL_RATIO: f32 = 0.001;
pub(crate) const MAX_HEDGING_REFILL_RATIO: f32 = 0.2;
