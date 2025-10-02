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

import 'package:google_cloud_gax/gax.dart';
import 'package:google_cloud_rpc/rpc.dart';
import 'package:http/http.dart';
import 'package:http/testing.dart';
import 'package:test/test.dart';

class TestMessage extends JsonEncodable {
  final String message;
  TestMessage(this.message);
  @override
  Object? toJson() {
    return message;
  }
}

final sampleUrl = Uri.https('example.org', '/path');

void main() {
  group('non-streaming', () {
    group('requests without body', () {
      late Request actualRequest;
      final service = ServiceClient(
        client: MockClient((request) async {
          actualRequest = request;
          return Response('', 200);
        }),
      );

      for (final (method, fn) in [
        ('DELETE', service.delete),
        ('GET', service.get),
        ('PATCH', service.patch),
        ('POST', service.post),
        ('PUT', service.put),
      ]) {
        test(method, () async {
          await fn(sampleUrl);

          expect(actualRequest.method, method);
          expect(actualRequest.url, sampleUrl);
          expect(actualRequest.headers, {'x-goog-api-client': anything});
          expect(actualRequest.body, isEmpty);
        });
      }
    });

    group('requests with body', () {
      late Request actualRequest;
      final service = ServiceClient(
        client: MockClient((request) async {
          actualRequest = request;
          return Response('', 200);
        }),
      );

      for (final (method, fn) in [
        ('PATCH', service.patch),
        ('POST', service.post),
        ('PUT', service.put),
      ]) {
        test(method, () async {
          await fn(sampleUrl, body: TestMessage('<test payload>'));

          expect(actualRequest.method, method);
          expect(actualRequest.url, sampleUrl);
          expect(actualRequest.headers, {
            'content-type': 'application/json',
            'x-goog-api-client': anything,
          });
          expect(actualRequest.body, '"<test payload>"');
        });
      }
    });

    test('500 response, no status, no response body', () async {
      final service = ServiceClient(
        client: MockClient((request) async {
          return Response('', 500);
        }),
      );

      await expectLater(
        () => service.post(sampleUrl),
        throwsA(isA<ClientException>()),
      );
    });

    test('400 response, status body', () async {
      final status = Status(code: 1, message: "failure", details: []);
      final statusJson = jsonEncode(status.toJson());
      final service = ServiceClient(
        client: MockClient((request) async {
          return Response('{"error":$statusJson}', 400);
        }),
      );

      await expectLater(
        () => service.post(sampleUrl),
        throwsA(
          isA<Status>()
              .having((e) => e.code, 'code', 1)
              .having((e) => e.message, 'message', 'failure')
              .having((e) => e.details, 'details', []),
        ),
      );
    });

    test('200 response, json response', () async {
      final service = ServiceClient(
        client: MockClient((request) async {
          return Response('{"fruit":"apple"}', 200);
        }),
      );

      expect(await service.post(sampleUrl), {"fruit": "apple"});
    });
  });

  group('streaming', () {
    group('requests without body', () {
      late Request actualRequest;
      final service = ServiceClient(
        client: MockClient((request) async {
          actualRequest = request;
          return Response('', 200);
        }),
      );

      for (final (method, fn) in [
        ('DELETE', service.deleteStreaming),
        ('GET', service.getStreaming),
        ('PATCH', service.patchStreaming),
        ('POST', service.postStreaming),
        ('PUT', service.putStreaming),
      ]) {
        test(method, () async {
          await fn(Uri.parse('http://example.com/')).drain();

          expect(actualRequest.method, method);
          expect(actualRequest.url, Uri.parse('http://example.com/?alt=sse'));
          expect(actualRequest.headers, {'x-goog-api-client': anything});
          expect(actualRequest.body, isEmpty);
        });
      }
    });

    group('requests with body', () {
      late Request actualRequest;
      final service = ServiceClient(
        client: MockClient((request) async {
          actualRequest = request;
          return Response('', 200);
        }),
      );

      for (final (method, fn) in [
        ('PATCH', service.patchStreaming),
        ('POST', service.postStreaming),
        ('PUT', service.putStreaming),
      ]) {
        test(method, () async {
          await fn(
            Uri.parse('http://example.com/'),
            body: TestMessage('<test payload>'),
          ).drain();

          expect(actualRequest.method, method);
          expect(actualRequest.url, Uri.parse('http://example.com/?alt=sse'));
          expect(actualRequest.headers, {
            'content-type': 'application/json',
            'x-goog-api-client': anything,
          });
          expect(actualRequest.body, '"<test payload>"');
        });
      }
    });

    test('500 response, no status, no response body', () async {
      final service = ServiceClient(
        client: MockClient((request) async {
          return Response('', 500);
        }),
      );

      expect(
        service.postStreaming(sampleUrl),
        emitsError(isA<ClientException>()),
      );
    });

    test('400 response, status body', () async {
      final status = Status(code: 1, message: "failure", details: []);
      final statusJson = jsonEncode(status.toJson());
      final service = ServiceClient(
        client: MockClient((request) async {
          return Response('{"error":$statusJson}', 400);
        }),
      );

      expect(
        service.postStreaming(sampleUrl),
        emitsInOrder([
          emitsError(
            isA<Status>()
                .having((e) => e.code, 'code', 1)
                .having((e) => e.message, 'message', 'failure')
                .having((e) => e.details, 'details', []),
          ),
          emitsDone,
        ]),
      );
    });

    test('200 response, empty response', () async {
      final service = ServiceClient(
        client: MockClient((request) async {
          return Response('', 200);
        }),
      );

      expect(service.postStreaming(sampleUrl), emitsDone);
    });

    test('200 response, single data response', () async {
      final service = ServiceClient(
        client: MockClient((request) async {
          return Response('data: {"fruit":"apple"}', 200);
        }),
      );

      expect(
        service.postStreaming(sampleUrl),
        emitsInOrder([
          {'fruit': 'apple'},
          emitsDone,
        ]),
      );
    });

    test('200 response, two data response', () async {
      final service = ServiceClient(
        client: MockClient((request) async {
          return Response(
            'data: {"fruit":"apple"}\ndata: {"fruit":"banana"}',
            200,
          );
        }),
      );

      expect(
        service.postStreaming(sampleUrl),
        emitsInOrder([
          {'fruit': 'apple'},
          {'fruit': 'banana'},
          emitsDone,
        ]),
      );
    });

    test('200 response, non-data lines response', () async {
      final service = ServiceClient(
        client: MockClient((request) async {
          return Response(
            'data: {"fruit":"apple"}\nevent: ?\n\n\ndata: {"fruit":"banana"}',
            200,
          );
        }),
      );

      expect(
        service.postStreaming(sampleUrl),
        emitsInOrder([
          {'fruit': 'apple'},
          {'fruit': 'banana'},
          emitsDone,
        ]),
      );
    });
  });
}
