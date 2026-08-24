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

//! Configuration options for Spanner channel pools.

use google_cloud_gax::error::Error as GaxError;
use std::time::Duration;

/// Maximum supported channels per Spanner client pool.
pub(crate) const MAX_SUPPORTED_CHANNELS: usize = 256;

/// Strategy used to select channels from the active pool.
// TODO: Make public when dynamic channel pooling feature is ready for release.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum ChannelSelectionStrategy {
    /// Power of Two Least Busy (samples 2 candidates, picks lower effective load, breaks ties with warmer channel).
    #[default]
    PowerOfTwoLeastBusy,
}

/// Configuration for the Spanner client channel pool.
// TODO: Make public when dynamic channel pooling feature is ready for release.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ChannelPoolConfig {
    /// Fixed-size static channel pool (default: 4 channels).
    Static(StaticChannelPoolConfig),
    /// Dynamic load-based channel pool.
    Dynamic(DynamicChannelPoolConfig),
}

impl Default for ChannelPoolConfig {
    fn default() -> Self {
        Self::Static(StaticChannelPoolConfig::default())
    }
}

impl ChannelPoolConfig {
    /// Validates the pool configuration.
    pub(crate) fn validate(&self) -> Result<(), GaxError> {
        match self {
            Self::Static(config) => config.validate(),
            Self::Dynamic(config) => config.validate(),
        }
    }

    /// Returns a reference to the `DynamicChannelPoolConfig` if dynamic.
    pub(crate) fn dynamic_config(&self) -> Option<&DynamicChannelPoolConfig> {
        match self {
            Self::Dynamic(config) => Some(config),
            Self::Static(_) => None,
        }
    }

    /// Returns a reference to the `StaticChannelPoolConfig` if static.
    pub(crate) fn static_config(&self) -> Option<&StaticChannelPoolConfig> {
        match self {
            Self::Static(config) => Some(config),
            Self::Dynamic(_) => None,
        }
    }
}

/// Configuration for a static (fixed-size) channel pool.
// TODO: Make public when dynamic channel pooling feature is ready for release.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StaticChannelPoolConfig {
    /// Number of channels in the static pool (default: 4).
    pub(crate) num_channels: usize,
}

impl Default for StaticChannelPoolConfig {
    fn default() -> Self {
        Self { num_channels: 4 }
    }
}

impl StaticChannelPoolConfig {
    /// Validates the static pool configuration.
    pub(crate) fn validate(&self) -> Result<(), GaxError> {
        if self.num_channels == 0 {
            return Err(GaxError::binding("num_channels must be at least 1"));
        }
        if self.num_channels > MAX_SUPPORTED_CHANNELS {
            return Err(GaxError::binding(format!(
                "num_channels cannot exceed maximum supported limit of {MAX_SUPPORTED_CHANNELS}"
            )));
        }
        Ok(())
    }
}

impl From<StaticChannelPoolConfig> for ChannelPoolConfig {
    fn from(config: StaticChannelPoolConfig) -> Self {
        Self::Static(config)
    }
}

/// Configuration for a dynamically scaling channel pool.
// TODO: Make public when dynamic channel pooling feature is ready for release.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct DynamicChannelPoolConfig {
    /// Number of channels created eagerly at startup (default: 4).
    pub(crate) initial_channels: usize,
    /// Minimum number of channels retained during scale-down (default: 4).
    pub(crate) min_channels: usize,
    /// Maximum number of channels allowed during scale-up (default: 10, configurable up to 256).
    pub(crate) max_channels: usize,
    /// Low-load threshold (per channel) triggering scale-down evaluation (default: 15.0).
    pub(crate) min_rpc_per_channel: f64,
    /// High-load threshold (per channel) triggering scale-up (default: 25.0).
    pub(crate) max_rpc_per_channel: f64,
    /// Synthetic picker load added per qualifying error (default: 5).
    pub(crate) error_penalty_step: u32,
    /// Sliding window duration for active error penalties (default: 5 seconds).
    pub(crate) error_penalty_duration: Duration,
    /// Maximum penalty load that can accumulate on a single channel (default: 25).
    pub(crate) error_penalty_max: u32,
    /// Interval between periodic scale-down evaluations (default: 3 minutes).
    pub(crate) scale_down_check_interval: Duration,
    /// Cooldown period between consecutive scale-up bursts (default: 10 seconds).
    pub(crate) scale_up_cooldown: Duration,
    /// Number of consecutive low-load checks required before scale-down (default: 3).
    pub(crate) consecutive_low_load_checks: usize,
    /// Maximum percentage of current pool size added per scale-up event (default: 30%, min 2).
    pub(crate) max_scale_up_percent: u32,
    /// Maximum number of channels marked draining per scale-down cycle (default: 2).
    pub(crate) max_remove_channels: usize,
    /// Idle grace duration a draining channel is kept alive after load drops to 0 (default: 1 minute).
    pub(crate) drain_idle_grace: Duration,
    /// Timeout for executing SELECT 1 priming on a new scaled-up channel (default: 10 seconds).
    pub(crate) prime_timeout: Duration,
    /// Maximum retry attempts for SELECT 1 priming (default: 3).
    pub(crate) prime_max_attempts: usize,
    /// Channel selection strategy (default: PowerOfTwoLeastBusy).
    pub(crate) selection_strategy: ChannelSelectionStrategy,
}

impl Default for DynamicChannelPoolConfig {
    fn default() -> Self {
        Self {
            initial_channels: 4,
            min_channels: 4,
            max_channels: 10,
            min_rpc_per_channel: 15.0,
            max_rpc_per_channel: 25.0,
            error_penalty_step: 5,
            error_penalty_duration: Duration::from_secs(5),
            error_penalty_max: 25,
            scale_down_check_interval: Duration::from_secs(180),
            scale_up_cooldown: Duration::from_secs(10),
            consecutive_low_load_checks: 3,
            max_scale_up_percent: 30,
            max_remove_channels: 2,
            drain_idle_grace: Duration::from_secs(60),
            prime_timeout: Duration::from_secs(10),
            prime_max_attempts: 3,
            selection_strategy: ChannelSelectionStrategy::PowerOfTwoLeastBusy,
        }
    }
}

impl DynamicChannelPoolConfig {
    /// Validates dynamic channel pool configuration boundaries and invariant relationships.
    pub(crate) fn validate(&self) -> Result<(), GaxError> {
        if self.min_channels == 0 {
            return Err(GaxError::binding("min_channels must be at least 1"));
        }
        if self.max_channels < self.min_channels {
            return Err(GaxError::binding(
                "max_channels must be greater than or equal to min_channels",
            ));
        }
        if self.max_channels > MAX_SUPPORTED_CHANNELS {
            return Err(GaxError::binding(format!(
                "max_channels cannot exceed maximum supported limit of {MAX_SUPPORTED_CHANNELS}"
            )));
        }
        if self.initial_channels < self.min_channels || self.initial_channels > self.max_channels {
            return Err(GaxError::binding(
                "initial_channels must be between min_channels and max_channels",
            ));
        }
        if self.min_rpc_per_channel.is_nan() || self.min_rpc_per_channel <= 0.0 {
            return Err(GaxError::binding(
                "min_rpc_per_channel must be greater than 0.0",
            ));
        }
        if self.max_rpc_per_channel.is_nan() || self.max_rpc_per_channel <= self.min_rpc_per_channel
        {
            return Err(GaxError::binding(
                "max_rpc_per_channel must be strictly greater than min_rpc_per_channel",
            ));
        }
        if self.max_scale_up_percent == 0 || self.max_scale_up_percent > 100 {
            return Err(GaxError::binding(
                "max_scale_up_percent must be between 1 and 100",
            ));
        }
        if self.consecutive_low_load_checks == 0 {
            return Err(GaxError::binding(
                "consecutive_low_load_checks must be at least 1",
            ));
        }
        if self.max_remove_channels == 0 {
            return Err(GaxError::binding("max_remove_channels must be at least 1"));
        }
        if self.scale_down_check_interval.is_zero() {
            return Err(GaxError::binding(
                "scale_down_check_interval must be greater than zero",
            ));
        }
        if self.prime_timeout.is_zero() {
            return Err(GaxError::binding("prime_timeout must be greater than zero"));
        }
        if self.prime_max_attempts == 0 {
            return Err(GaxError::binding("prime_max_attempts must be at least 1"));
        }
        Ok(())
    }

    /// Computes the midpoint target RPC capacity per channel (e.g. (15 + 25) / 2 = 20).
    pub(crate) fn target_rpc_per_channel(&self) -> u32 {
        let midpoint = ((self.min_rpc_per_channel + self.max_rpc_per_channel) / 2.0).floor() as u32;
        midpoint.max(1)
    }

    /// Computes the desired channel count to service the given aggregate RPC load.
    pub(crate) fn desired_channel_count(&self, total_load: u32) -> usize {
        let target_rpc = self.target_rpc_per_channel() as f64;
        ((total_load as f64) / target_rpc).ceil() as usize
    }
}

impl From<DynamicChannelPoolConfig> for ChannelPoolConfig {
    fn from(config: DynamicChannelPoolConfig) -> Self {
        Self::Dynamic(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fmt::Debug;

    #[test]
    fn traits() {
        static_assertions::assert_impl_all!(
            ChannelPoolConfig: Clone,
            Debug,
            PartialEq,
            Send,
            Sync
        );
        static_assertions::assert_impl_all!(
            StaticChannelPoolConfig: Clone,
            Debug,
            PartialEq,
            Eq,
            Send,
            Sync
        );
        static_assertions::assert_impl_all!(
            DynamicChannelPoolConfig: Clone,
            Debug,
            PartialEq,
            Send,
            Sync
        );
        static_assertions::assert_impl_all!(
            ChannelSelectionStrategy: Clone,
            Copy,
            Debug,
            PartialEq,
            Eq,
            Send,
            Sync
        );
    }

    #[test]
    fn config_defaults_and_precedence() {
        let static_config = StaticChannelPoolConfig::default();
        assert_eq!(
            static_config.num_channels, 4,
            "StaticChannelPoolConfig default num_channels must be 4"
        );
        assert!(
            static_config.validate().is_ok(),
            "Default StaticChannelPoolConfig must pass validation"
        );

        let dynamic_config = DynamicChannelPoolConfig::default();
        assert_eq!(
            dynamic_config.initial_channels, 4,
            "DynamicChannelPoolConfig default initial_channels must be 4"
        );
        assert_eq!(
            dynamic_config.min_channels, 4,
            "DynamicChannelPoolConfig default min_channels must be 4"
        );
        assert_eq!(
            dynamic_config.max_channels, 10,
            "DynamicChannelPoolConfig default max_channels must be 10"
        );
        assert_eq!(
            dynamic_config.min_rpc_per_channel, 15.0,
            "DynamicChannelPoolConfig default min_rpc_per_channel must be 15.0"
        );
        assert_eq!(
            dynamic_config.max_rpc_per_channel, 25.0,
            "DynamicChannelPoolConfig default max_rpc_per_channel must be 25.0"
        );
        assert_eq!(
            dynamic_config.error_penalty_step, 5,
            "DynamicChannelPoolConfig default error_penalty_step must be 5"
        );
        assert_eq!(
            dynamic_config.error_penalty_duration,
            Duration::from_secs(5),
            "DynamicChannelPoolConfig default error_penalty_duration must be 5s"
        );
        assert_eq!(
            dynamic_config.error_penalty_max, 25,
            "DynamicChannelPoolConfig default error_penalty_max must be 25"
        );
        assert_eq!(
            dynamic_config.consecutive_low_load_checks, 3,
            "DynamicChannelPoolConfig default consecutive_low_load_checks must be 3"
        );
        assert_eq!(
            dynamic_config.max_remove_channels, 2,
            "DynamicChannelPoolConfig default max_remove_channels must be 2"
        );
        assert!(
            dynamic_config.validate().is_ok(),
            "Default DynamicChannelPoolConfig must pass validation"
        );

        let default_pool_config = ChannelPoolConfig::default();
        assert_eq!(
            default_pool_config,
            ChannelPoolConfig::Static(StaticChannelPoolConfig::default()),
            "Default ChannelPoolConfig must be Static"
        );

        let from_static: ChannelPoolConfig = static_config.into();
        assert_eq!(
            from_static,
            ChannelPoolConfig::Static(StaticChannelPoolConfig::default()),
            "Conversion from StaticChannelPoolConfig must produce Static variant"
        );

        let from_dynamic: ChannelPoolConfig = dynamic_config.into();
        assert_eq!(
            from_dynamic,
            ChannelPoolConfig::Dynamic(DynamicChannelPoolConfig::default()),
            "Conversion from DynamicChannelPoolConfig must produce Dynamic variant"
        );
    }

    #[test]
    fn config_validation_errors() {
        let invalid_static = StaticChannelPoolConfig { num_channels: 0 };
        assert!(
            invalid_static.validate().is_err(),
            "StaticChannelPoolConfig with 0 channels must fail validation"
        );

        let invalid_max = DynamicChannelPoolConfig {
            min_channels: 10,
            max_channels: 5,
            ..Default::default()
        };
        assert!(
            invalid_max.validate().is_err(),
            "DynamicChannelPoolConfig with max_channels < min_channels must fail validation"
        );

        let invalid_initial = DynamicChannelPoolConfig {
            initial_channels: 2,
            min_channels: 4,
            max_channels: 10,
            ..Default::default()
        };
        assert!(
            invalid_initial.validate().is_err(),
            "DynamicChannelPoolConfig with initial_channels < min_channels must fail validation"
        );

        let invalid_rpc = DynamicChannelPoolConfig {
            min_rpc_per_channel: 25.0,
            max_rpc_per_channel: 20.0,
            ..Default::default()
        };
        assert!(
            invalid_rpc.validate().is_err(),
            "DynamicChannelPoolConfig with max_rpc <= min_rpc must fail validation"
        );

        let nan_min_rpc = DynamicChannelPoolConfig {
            min_rpc_per_channel: f64::NAN,
            ..Default::default()
        };
        assert!(
            nan_min_rpc.validate().is_err(),
            "DynamicChannelPoolConfig with NaN min_rpc must fail validation"
        );

        let nan_max_rpc = DynamicChannelPoolConfig {
            max_rpc_per_channel: f64::NAN,
            ..Default::default()
        };
        assert!(
            nan_max_rpc.validate().is_err(),
            "DynamicChannelPoolConfig with NaN max_rpc must fail validation"
        );

        let invalid_interval = DynamicChannelPoolConfig {
            scale_down_check_interval: Duration::ZERO,
            ..Default::default()
        };
        assert!(
            invalid_interval.validate().is_err(),
            "DynamicChannelPoolConfig with zero scale_down_check_interval must fail validation"
        );

        let invalid_timeout = DynamicChannelPoolConfig {
            prime_timeout: Duration::ZERO,
            ..Default::default()
        };
        assert!(
            invalid_timeout.validate().is_err(),
            "DynamicChannelPoolConfig with zero prime_timeout must fail validation"
        );

        let invalid_percent = DynamicChannelPoolConfig {
            max_scale_up_percent: 150,
            ..Default::default()
        };
        assert!(
            invalid_percent.validate().is_err(),
            "DynamicChannelPoolConfig with max_scale_up_percent > 100 must fail validation"
        );
    }
}
