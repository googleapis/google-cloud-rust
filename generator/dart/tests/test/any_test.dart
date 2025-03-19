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
  test('encode into ErrorInfo', () {
    final data = '''{
  "@type": "type.googleapis.com/google.rpc.ErrorInfo",
  "reason": "CREDENTIALS_MISSING",
  "domain": "googleapis.com",
  "metadata": {
    "method": "google.cloud.functions.v2.FunctionService.ListFunctions",
    "service": "cloudfunctions.googleapis.com"
  }
}''';
    final json = jsonDecode(data);
    final any = Any.fromJson(json);

    expect(any.typeName, 'google.rpc.ErrorInfo');
    expect(any.isType(ErrorInfo.fullyQualifiedName), true);

    final errorInfo = any.unpackFrom(ErrorInfo.fromJson);
    expect(errorInfo, isNotNull);
    expect(errorInfo.reason, 'CREDENTIALS_MISSING');
    expect(errorInfo.domain, 'googleapis.com');

    final metadata = errorInfo.metadata!.entries
        .map((entry) => '${entry.key}:${entry.value}')
        .join('\n');
    expect(metadata, '''
method:google.cloud.functions.v2.FunctionService.ListFunctions
service:cloudfunctions.googleapis.com''');
  });

  test('decode from Status', () {
    final status = Status(
      code: 404,
      message: 'not found',
      details: [
        Any.from(
            ErrorInfo(reason: 'CREDENTIALS_MISSING', domain: 'googleapis.com')),
      ],
    );

    // Write the status to json, read it back, and validate the info.
    final status2 = Status.fromJson(jsonDecode(jsonEncode(status.toJson())));
    expect(status2.code, 404);
    expect(status2.message, 'not found');
    expect(status2.details, hasLength(1));

    final any = status2.details![0];
    expect(any.isType(ErrorInfo.fullyQualifiedName), true);
    final actual = any.unpackFrom(ErrorInfo.fromJson);
    expect(actual.reason, 'CREDENTIALS_MISSING');
    expect(actual.domain, 'googleapis.com');
  });
}
