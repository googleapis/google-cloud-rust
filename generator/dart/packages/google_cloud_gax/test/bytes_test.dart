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

import 'dart:typed_data';

import 'package:google_cloud_gax/src/json_helpers.dart';
import 'package:test/test.dart';

void main() {
  test('encode empty', () {
    final bytes = Uint8List.fromList([]);
    expect(encodeBytes(bytes), '');
  });

  test('encode simple', () {
    final bytes = Uint8List.fromList([1, 2, 3]);
    expect(encodeBytes(bytes), 'AQID');
  });

  test('decode empty', () {
    final bytes = decodeBytes('AQID')!;
    final actual = bytes.map((item) => '$item').join(',');
    expect(actual, '1,2,3');
  });

  test('decode simple', () {
    final bytes = decodeBytes('bG9yZW0gaXBzdW0=')!;
    final actual = bytes.map((item) => '$item').join(',');
    // "lorem ipsum"
    expect(actual, '108,111,114,101,109,32,105,112,115,117,109');
  });

  test('decode simple', () {
    final bytes = decodeBytes('YWJjMTIzIT8kKiYoKSctPUB+')!;
    final actual = bytes.map((item) => '$item').join(',');
    expect(actual, '97,98,99,49,50,51,33,63,36,42,38,40,41,39,45,61,64,126');
  });
}
