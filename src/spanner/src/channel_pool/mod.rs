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

//! Dynamic Channel Pooling for Spanner.
//!
//! Provides capacity management, load-balanced channel selection (Power of Two Least Busy),
//! health-aware error penalization, caller-owned transaction affinity pinning, and background
//! scaling and priming for gRPC channels.

// TODO(dynamic-channel-pooling): Remove allow(dead_code, unused_imports) once integrated into Spanner client.
#![allow(dead_code)]
#![allow(unused_imports)]

pub(crate) mod affinity;
pub(crate) mod config;
pub(crate) mod entry;
pub(crate) mod pool;
pub(crate) mod scaler;

pub(crate) use affinity::TransactionAffinity;
pub(crate) use config::{
    ChannelPoolConfig, ChannelSelectionStrategy, DynamicChannelPoolConfig, StaticChannelPoolConfig,
};
pub(crate) use entry::{ActiveRpcGuard, ChannelEntry, ChannelLease, RwTransactionAffinityGuard};
pub(crate) use pool::ChannelPool;
