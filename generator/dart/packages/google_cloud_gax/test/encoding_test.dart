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

import 'package:google_cloud_gax/src/encoding.dart';
import 'package:google_cloud_rpc/rpc.dart';
import 'package:test/test.dart';

void main() {
  test('int64', () {
    expect(encodeInt64(decodeInt64('1')), '1');
    expect(encodeInt64(decodeInt64(1)), '1');
  });

  test('double', () {
    expect(decodeDouble(1), 1);
    expect(decodeDouble(1.1), 1.1);
    expect(decodeDouble(encodeDouble(1)), 1);
    expect(decodeDouble(encodeDouble(1.1)), 1.1);
  });

  test('double NaN', () {
    expect(decodeDouble('NaN'), isNaN);
    expect(decodeDouble('Infinity'), double.infinity);
    expect(decodeDouble('-Infinity'), double.negativeInfinity);

    // don't allow arbitrary strings for doubles
    expect(() => decodeDouble('1.0'), throwsFormatException);

    expect(encodeDouble(double.nan), 'NaN');
    expect(encodeDouble(double.infinity), 'Infinity');
    expect(encodeDouble(double.negativeInfinity), '-Infinity');
  });

  test('enum', () {
    final actual = decodeEnum(Code('NOT_FOUND').toJson(), Code.fromJson);
    expect(actual!.value, 'NOT_FOUND');
  });

  test('message', () {
    final actual = decode(
        Status(code: 200, message: 'OK').toJson() as Map<String, Object?>,
        Status.fromJson);
    expect(actual!.code, 200);
    expect(actual.message, 'OK');
  });

  test('list of enums', () {
    expect(
      decodeListEnum(encodeList([Code.notFound]), Code.fromJson),
      [Code.notFound],
    );
  });

  test('list of bytes', () {
    final actual = decodeListBytes(encodeListBytes([
      Uint8List.fromList([1]),
      Uint8List.fromList([1, 2]),
      Uint8List.fromList([1, 2, 3]),
    ]));

    expect(actual, hasLength(3));

    expect(stringify(actual![0]), '1');
    expect(stringify(actual[1]), '1,2');
    expect(stringify(actual[2]), '1,2,3');
  });

  test('list of messages', () {
    final actual =
        decodeListMessage(encodeList([Status(code: 200)]), Status.fromJson);
    expect(actual![0], isA<Status>());
    expect(actual[0].code, 200);
  });

  test('map of enums', () {
    final actual = decodeMapEnum(
      encodeMap({
        'one': Code.aborted,
        'two': Code.alreadyExists,
        'three': Code.notFound,
      }),
      Code.fromJson,
    );
    expect(actual, isA<Map>());
    expect(actual!['one'], Code.aborted);
  });

  test('map of bytes', () {
    final actual = decodeMapBytes(
      encodeMapBytes({
        1: Uint8List.fromList([1, 2]),
        2: Uint8List.fromList([1, 2, 3, 4]),
      }),
    );
    expect(actual, isA<Map>());
    expect(stringify(actual![1]!), '1,2');
  });

  test('map of messages', () {
    final actual = decodeMapMessage(
      encodeMap({
        'one': Status(code: 200),
        'two': Status(code: 301),
      }),
      Status.fromJson,
    );
    expect(actual, isA<Map>());
    expect(actual!['one']!.code, 200);
  });

  group('bytes', () {
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
  });
}

String stringify(Uint8List list) => list.map((i) => '$i').join(',');
