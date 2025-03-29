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

import 'package:google_cloud_rpc/rpc.dart';
import 'package:test/test.dart';

void main() {
  group("test 'bytes' encoding", () {
    test('rpc.HttpResponse.body', () {
      final data = '''{
  "status": 200,
  "reason": "OK",
  "headers": [],
  "body": "$base64EncodedText"
}''';
      final json = jsonDecode(data);
      final response = HttpResponse.fromJson(json);

      expect(response.status, 200);
      expect(response.reason, 'OK');
      expect(response.headers, isEmpty);
      expect(response.body!, isNotEmpty);

      final body = response.body!;
      final bodyText = utf8.decode(body);
      expect(bodyText, expectedText);

      // Re-encode HttpResponse and validate.
      final jsonEncoder = JsonEncoder.withIndent('  ');

      final encodedActual = jsonEncoder.convert(response.toJson());
      final encodedExpected = jsonEncoder.convert(json);
      expect(encodedActual, encodedExpected);
    });
  });
}

const String expectedText = '''
Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor
incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis
nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.''';

const String base64EncodedText =
    'TG9yZW0gaXBzdW0gZG9sb3Igc2l0IGFtZXQsIGNvbnNlY3RldHVyIGFkaXBpc2NpbmcgZWxpdC'
    'wgc2VkIGRvIGVpdXNtb2QgdGVtcG9yCmluY2lkaWR1bnQgdXQgbGFib3JlIGV0IGRvbG9yZSBt'
    'YWduYSBhbGlxdWEuIFV0IGVuaW0gYWQgbWluaW0gdmVuaWFtLCBxdWlzCm5vc3RydWQgZXhlcm'
    'NpdGF0aW9uIHVsbGFtY28gbGFib3JpcyBuaXNpIHV0IGFsaXF1aXAgZXggZWEgY29tbW9kbyBj'
    'b25zZXF1YXQu';
