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

/// Test that we can encode and decode common [Any] related types, like [Status]
/// and [ErrorInfo].
library;

import 'dart:convert';

import 'package:google_cloud_protobuf/protobuf.dart';
import 'package:google_cloud_rpc/rpc.dart';
import 'package:test/test.dart';

void main() {
  test('Status.details helpers', () {
    final status = Status.fromJson(jsonDecode(sampleStatus));
    expect(status.code, 429);
    expect(status.message, isNotEmpty);
    expect(status.details, isNotEmpty);

    final details = status.detailsAsMessages;
    expect(details, isNotEmpty);
    expect(details[0], isA<ErrorInfo>());
    expect(details[1], isA<LocalizedMessage>());
    expect(details[2], isA<Help>());

    expect(status.errorInfo, isNotNull);
    expect(status.errorInfo!.reason, 'RESOURCE_AVAILABILITY');
    expect(status.errorInfo!.metadata, isNotEmpty);
  });
}

const sampleStatus = r'''{
  "code": 429,
  "message": "The zone 'us-east1-a' does not have enough resources available to fulfill the request. Try a different zone, or try again later.",
  "details": [
    {
      "@type": "type.googleapis.com/google.rpc.ErrorInfo",
      "reason": "RESOURCE_AVAILABILITY",
      "domain": "compute.googleapis.com",
      "metadata": {
        "zone": "us-east1-a",
        "vmType": "e2-medium",
        "attachment": "local-ssd=3,nvidia-t4=2",
        "zonesWithCapacity": "us-central1-f,us-central1-c"
      }
    },
    {
      "@type": "type.googleapis.com/google.rpc.LocalizedMessage",
      "locale": "en-US",
      "message": "An <e2-medium> VM instance with <local-ssd=3,nvidia-t4=2> is currently unavailable in the <us-east1-a> zone. Consider trying your request in the <us-central1-f,us-central1-c> zone(s), which currently has/have capacity to accommodate your request. Alternatively, you can try your request again with a different VM hardware configuration or at a later time. For more information, see the troubleshooting documentation."
    },
    {
      "@type": "type.googleapis.com/google.rpc.Help",
      "links": [
        {
          "description": "Additional information on this error",
          "url": "https://cloud.google.com/compute/docs/resource-error"
        }
      ]
    }
  ]
}
''';
