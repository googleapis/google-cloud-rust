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
  test('Struct', () {
    const data = '''{
  "indexes_entries_scanned": "1000",
  "documents_scanned": "20",
  "billing_details" : {
    "documents_billable": "20",
    "index_entries_billable": "1000",
    "min_query_cost": "0"
  }
}
''';

    final struct = Struct.fromJson(jsonDecode(data));
    expect(struct.fields, hasLength(3));
    expect(struct.fields!['billing_details'], isA<Value>());
    final billingDetails = struct.fields!['billing_details']!;
    expect(billingDetails.structValue, isNotNull);

    expect(struct.toJson(), isA<Map>());
  });

  test('ListValue', () {
    const data = '''[
  {"query_scope": "Collection", "properties": "(foo ASC, __name__ ASC)"},
  {"query_scope": "Collection", "properties": "(bar ASC, __name__ ASC)"}
]
''';

    final list = ListValue.fromJson(jsonDecode(data));
    expect(list.values, hasLength(2));
    expect(list.values![0].structValue, isNotNull);
    expect(list.values![1].structValue, isNotNull);
    final childStruct = list.values![0].structValue!;
    expect(childStruct.fields, hasLength(2));

    expect(list.toJson(), isA<List>());
  });

  test('NullValue', () {
    expect(Value.fromJson(null).nullValue, isNotNull);
    expect(Value.fromJson('foo').nullValue, isNull);

    expect(Value(nullValue: NullValue.nullValue).toJson(), isNull);
  });

  group('Value', () {
    test('numberValue', () {
      expect(Value.fromJson(3).numberValue, 3);
      expect(Value.fromJson(3.14).numberValue, 3.14);

      expect(Value(numberValue: 3).toJson(), 3);
      expect(Value(numberValue: 3.14).toJson(), 3.14);
    });

    test('stringValue', () {
      expect(Value.fromJson('foo bar').stringValue, 'foo bar');
      expect(Value(stringValue: 'foo bar').toJson(), 'foo bar');
    });

    test('boolValue', () {
      expect(Value.fromJson(true).boolValue, true);
      expect(Value(boolValue: true).toJson(), true);
    });

    test('structValue', () {
      const data = '{"foo": "one", "bar": 3.14, "baz": null}';

      final struct = Value.fromJson(jsonDecode(data)).structValue;
      expect(struct, isNotNull);
      expect(struct!.fields, hasLength(3));

      final actual = struct.toJson();
      expect(actual, isA<Map>());
      expect(actual, hasLength(3));
      expect((actual as Map)['foo'], 'one');
      expect(actual['bar'], 3.14);
      expect(actual['baz'], isNull);
    });

    test('listValue', () {
      const data = '["foo", 3, false, true, 3.14, null]';

      final list = Value.fromJson(jsonDecode(data)).listValue;
      expect(list, isNotNull);
      expect(list!.values, hasLength(6));

      final actual = list.toJson();
      expect(actual, isA<List>());
      expect(actual, hasLength(6));
      expect((actual as List).join(','), 'foo,3.0,false,true,3.14,null');
    });
  });
}
