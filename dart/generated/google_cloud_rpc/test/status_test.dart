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

import 'package:google_cloud_protobuf/protobuf.dart';
import 'package:google_cloud_rpc/rpc.dart';
import 'package:test/test.dart';

void main() {
  final sampleStatus = Status(
    code: 429,
    message: "Zone 'us-east1-a': not enough resources",
    details: [
      Any.from(
        ErrorInfo(
          reason: 'RESOURCE_AVAILABILITY',
          domain: 'compute.googleapis.com',
        ),
      ),
      Any.from(LocalizedMessage(locale: 'en-US', message: 'Lorem ipsum.')),
    ],
  );

  test('detailsAsMessages', () {
    expect(sampleStatus.detailsAsMessages, isNotEmpty);
    expect(sampleStatus.detailsAsMessages, contains(isA<ErrorInfo>()));
    expect(sampleStatus.detailsAsMessages, contains(isA<LocalizedMessage>()));
  });

  test('errorInfo', () {
    expect(sampleStatus.errorInfo, isNotNull);
    expect(sampleStatus.errorInfo!.reason, 'RESOURCE_AVAILABILITY');
  });

  test('localizedMessage', () {
    expect(sampleStatus.localizedMessage, isNotNull);
    expect(sampleStatus.localizedMessage!.locale, 'en-US');
    expect(sampleStatus.localizedMessage!.message, isNotEmpty);
  });
}
