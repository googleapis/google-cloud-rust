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

import 'dart:convert';

import 'package:google_cloud_protobuf/protobuf.dart';
import 'package:test/test.dart';

void main() {
  test('pack into and from Duration', () {
    final duration = Duration(seconds: 10, nanos: 100);

    final any = Any(json: {});
    any.packInto(duration);
    expect(any.typeName, 'google.protobuf.Duration');

    final actual = any.unpackFrom(Duration.fromJson);

    expect(actual.seconds, 10);
    expect(actual.nanos, 100);
  });

  test('json round-tripping', () {
    final duration = Duration(seconds: 10, nanos: 100);

    final any = Any()..packInto(duration);

    final anyCopy = Any.fromJson(any.toJson());
    final actual = anyCopy.unpackFrom(Duration.fromJson);

    expect(actual.seconds, 10);
    expect(actual.nanos, 100);
  });

  test('pack into and from FieldMask', () {
    final fieldMask = FieldMask(paths: ['foo', 'bar']);

    final any = Any()..packInto(fieldMask);
    expect(any.typeName, 'google.protobuf.FieldMask');

    final actual = any.unpackFrom(FieldMask.fromJson);

    expect(actual.paths, isNotEmpty);
    expect(actual.paths!.join('|'), 'foo|bar');
  });

  test('from JSON', () {
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
  });

  test('bad type prefix', () {
    final data = '''{
  "@type": "www.cheese.com/google.rpc.ErrorInfo",
  "foo": "bar"
}''';
    final json = jsonDecode(data);
    final any = Any.fromJson(json);

    expect(any.typeName, equals('www.cheese.com/google.rpc.ErrorInfo'));
  });
}
