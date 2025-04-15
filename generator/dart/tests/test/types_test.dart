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

/// Round trip some types from google.type as integration tests.
library;

import 'dart:convert';

import 'package:google_cloud_protobuf/protobuf.dart';
import 'package:google_cloud_type/type.dart';
import 'package:test/test.dart';

void main() {
  test('Color', () {
    var expected =
        Color(red: 0, green: 1, blue: 2, alpha: FloatValue(value: 0.5));
    var actual = Color.fromJson(encodeDecode(expected.toJson()));
    expect(actual.red, expected.red);
    expect(actual.green, expected.green);
    expect(actual.blue, expected.blue);
    expect(actual.alpha!.value, expected.alpha!.value);
  });
}

dynamic encodeDecode(Object json) => jsonDecode(jsonEncode(json));
