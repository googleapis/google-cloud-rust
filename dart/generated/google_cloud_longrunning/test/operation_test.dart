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

import 'package:google_cloud_longrunning/longrunning.dart';
import 'package:google_cloud_protobuf/protobuf.dart';
import 'package:google_cloud_rpc/rpc.dart';
import 'package:test/test.dart';

void main() {
  test('responseAsMessage', () {
    final result = LocalizedMessage(locale: 'en-US', message: 'Lorem ipsum.');
    final operation = Operation<LocalizedMessage, Any>(
      name: 'temp-name',
      done: true,
      response: Any.from(result),
      operationHelper: OperationHelper(LocalizedMessage.fromJson),
    );

    expect(operation.response, isNotNull);

    final actual = operation.responseAsMessage!;
    expect(actual, isA<LocalizedMessage>());
    expect(actual.locale, 'en-US');
    expect(actual.message, 'Lorem ipsum.');
  });

  test('metadataAsMessage', () {
    final metadata = Status(code: 200, message: 'OK', details: []);
    final operation = Operation<LocalizedMessage, Status>(
      name: 'temp-name',
      done: false,
      metadata: Any.from(metadata),
      operationHelper:
          OperationHelper(LocalizedMessage.fromJson, Status.fromJson),
    );

    expect(operation.metadata, isNotNull);

    final actual = operation.metadataAsMessage!;
    expect(actual, isA<Status>());
    expect(actual.code, 200);
    expect(actual.message, 'OK');
  });
}
