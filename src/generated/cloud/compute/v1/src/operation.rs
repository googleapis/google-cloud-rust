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

#![cfg(any(
    feature = "accelerator-types",
    feature = "addresses",
    feature = "advice",
    feature = "autoscalers",
    feature = "backend-buckets",
    feature = "backend-services",
    feature = "cross-site-networks",
    feature = "disk-types",
    feature = "disks",
    feature = "external-vpn-gateways",
    feature = "firewall-policies",
    feature = "firewalls",
    feature = "forwarding-rules",
    feature = "future-reservations",
    feature = "global-addresses",
    feature = "global-forwarding-rules",
    feature = "global-network-endpoint-groups",
    feature = "global-operations",
    feature = "global-organization-operations",
    feature = "global-public-delegated-prefixes",
    feature = "global-vm-extension-policies",
    feature = "health-checks",
    feature = "hosts",
    feature = "http-health-checks",
    feature = "https-health-checks",
    feature = "image-family-views",
    feature = "images",
    feature = "instance-group-manager-resize-requests",
    feature = "instance-group-managers",
    feature = "instance-groups",
    feature = "instance-settings",
    feature = "instance-templates",
    feature = "instances",
    feature = "instant-snapshot-groups",
    feature = "instant-snapshots",
    feature = "interconnect-attachment-groups",
    feature = "interconnect-attachments",
    feature = "interconnect-groups",
    feature = "interconnect-locations",
    feature = "interconnect-remote-locations",
    feature = "interconnects",
    feature = "license-codes",
    feature = "licenses",
    feature = "machine-images",
    feature = "machine-types",
    feature = "network-attachments",
    feature = "network-edge-security-services",
    feature = "network-endpoint-groups",
    feature = "network-firewall-policies",
    feature = "network-profiles",
    feature = "networks",
    feature = "node-groups",
    feature = "node-templates",
    feature = "node-types",
    feature = "organization-security-policies",
    feature = "packet-mirrorings",
    feature = "preview-features",
    feature = "projects",
    feature = "public-advertised-prefixes",
    feature = "public-delegated-prefixes",
    feature = "region-autoscalers",
    feature = "region-backend-buckets",
    feature = "region-backend-services",
    feature = "region-commitments",
    feature = "region-composite-health-checks",
    feature = "region-disk-types",
    feature = "region-disks",
    feature = "region-health-aggregation-policies",
    feature = "region-health-check-services",
    feature = "region-health-checks",
    feature = "region-health-sources",
    feature = "region-instance-group-manager-resize-requests",
    feature = "region-instance-group-managers",
    feature = "region-instance-groups",
    feature = "region-instance-templates",
    feature = "region-instances",
    feature = "region-instant-snapshot-groups",
    feature = "region-instant-snapshots",
    feature = "region-network-endpoint-groups",
    feature = "region-network-firewall-policies",
    feature = "region-notification-endpoints",
    feature = "region-operations",
    feature = "region-security-policies",
    feature = "region-snapshot-settings",
    feature = "region-snapshots",
    feature = "region-ssl-certificates",
    feature = "region-ssl-policies",
    feature = "region-target-http-proxies",
    feature = "region-target-https-proxies",
    feature = "region-target-tcp-proxies",
    feature = "region-url-maps",
    feature = "region-zones",
    feature = "regions",
    feature = "reliability-risks",
    feature = "reservation-blocks",
    feature = "reservation-slots",
    feature = "reservation-sub-blocks",
    feature = "reservations",
    feature = "resource-policies",
    feature = "rollout-plans",
    feature = "rollouts",
    feature = "routers",
    feature = "routes",
    feature = "security-policies",
    feature = "service-attachments",
    feature = "snapshot-settings",
    feature = "snapshots",
    feature = "ssl-certificates",
    feature = "ssl-policies",
    feature = "storage-pool-types",
    feature = "storage-pools",
    feature = "subnetworks",
    feature = "target-grpc-proxies",
    feature = "target-http-proxies",
    feature = "target-https-proxies",
    feature = "target-instances",
    feature = "target-pools",
    feature = "target-ssl-proxies",
    feature = "target-tcp-proxies",
    feature = "target-vpn-gateways",
    feature = "url-maps",
    feature = "vpn-gateways",
    feature = "vpn-tunnels",
    feature = "wire-groups",
    feature = "zone-operations",
    feature = "zone-vm-extension-policies",
    feature = "zones",
))]

use crate::model::Operation;
use google_cloud_gax::error::rpc::{Code, Status};

impl google_cloud_lro::internal::DiscoveryOperation for Operation {
    fn name(&self) -> Option<&String> {
        self.name.as_ref()
    }
    fn done(&self) -> bool {
        self.status == Some(crate::model::operation::Status::Done)
    }
    fn error(&self) -> Option<Status> {
        if self.error.is_none()
            && self.http_error_status_code.is_none()
            && self.http_error_message.is_none()
        {
            return None;
        }

        let mut status = Status::default();

        let http_status = self.http_error_status_code.unwrap_or(200);
        let mut code = match http_status {
            200 => Code::Ok,
            400 => Code::InvalidArgument,
            401 => Code::Unauthenticated,
            403 => Code::PermissionDenied,
            404 => Code::NotFound,
            409 => Code::AlreadyExists,
            429 => Code::ResourceExhausted,
            499 => Code::Cancelled,
            500 => Code::Internal,
            501 => Code::Unimplemented,
            503 => Code::Unavailable,
            504 => Code::DeadlineExceeded,
            _ => Code::Unknown,
        };

        let mut message = self.http_error_message.clone().unwrap_or_default();

        if let Some(first_err) = self.error.as_ref().and_then(|err| err.errors.first()) {
            if code == Code::Ok {
                code = Code::Unknown;
            }
            if let Some(err_code) = &first_err.code {
                code = match err_code.as_str() {
                    "QUOTA_EXCEEDED" => Code::ResourceExhausted,
                    other => Code::try_from(other).unwrap_or(code),
                };
            }
            if let Some(err_msg) = &first_err.message {
                message = err_msg.clone();
            }
        }

        if code == Code::Ok && http_status == 200 {
            return None;
        }

        status = status.set_code(code).set_message(message);
        Some(status)
    }
}
