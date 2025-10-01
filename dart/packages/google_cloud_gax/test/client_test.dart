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

final sampleUrl = Uri.https('example.org', '/path');
final samplePayload = Status(code: 200, message: 'OK');

void main() {
  Client? httpClient;
  Request? request;

  setUp(() {
    httpClient = MockClient((Request r) {
      request = r;
      return Future.value(Response('', 200));
    });
  });

  test('get', () async {
    final service = ServiceClient(client: httpClient!);

    await service.get(sampleUrl);

    expect(request!.method, 'GET');
    expect(request!.headers.keys, contains('x-goog-api-client'));
  });

  test('post', () async {
    final service = ServiceClient(client: httpClient!);

    await service.post(sampleUrl, body: samplePayload);

    expect(request!.method, 'POST');
    expect(request!.headers.keys, contains('x-goog-api-client'));
  });

  group('postStreaming', () {
    test('validate request', () async {
      late Request actualRequest;
      final service = ServiceClient(
        client: MockClient((request) async {
          actualRequest = request;
          return Response('', 200);
        }),
      );

      await service.postStreaming(sampleUrl, body: samplePayload).drain();

      expect(actualRequest.method, 'POST');
      expect(actualRequest.body, jsonEncode(samplePayload.toJson()));
      expect(actualRequest.headers.keys, contains('x-goog-api-client'));
      expect(
        actualRequest.headers,
        containsPair('content-type', 'application/json'),
      );
      expect(actualRequest.url.queryParameters['alt'], 'sse');
    });

    test('500 response, no status, no response body', () async {
      final service = ServiceClient(
        client: MockClient((request) async {
          return Response('', 500);
        }),
      );

      await expectLater(
        () => service.post(sampleUrl, body: samplePayload),
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
        () => service.post(sampleUrl, body: samplePayload),
        throwsA(
          isA<Status>()
              .having((e) => e.code, 'code', 1)
              .having((e) => e.message, 'message', 'failure')
              .having((e) => e.details, 'details', []),
        ),
      );
    });

    test('200 response, empty response', () async {
      final service = ServiceClient(
        client: MockClient((request) async {
          return Response('', 200);
        }),
      );

      expect(service.postStreaming(sampleUrl, body: samplePayload), emitsDone);
    });

    test('200 response, single data response', () async {
      final service = ServiceClient(
        client: MockClient((request) async {
          return Response('data: {"fruit":"apple"}', 200);
        }),
      );

      expect(
        service.postStreaming(sampleUrl, body: samplePayload),
        emitsInOrder([
          {'fruit': 'apple'},
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
        service.postStreaming(sampleUrl, body: samplePayload),
        emitsInOrder([
          {'fruit': 'apple'},
          {'fruit': 'banana'},
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
        service.postStreaming(sampleUrl, body: samplePayload),
        emitsInOrder([
          {'fruit': 'apple'},
          {'fruit': 'banana'},
        ]),
      );
    });
  });

  test('put', () async {
    final service = ServiceClient(client: httpClient!);

    await service.put(sampleUrl, body: samplePayload);

    expect(request!.method, 'PUT');
    expect(request!.headers.keys, contains('x-goog-api-client'));
  });

  test('patch', () async {
    final service = ServiceClient(client: httpClient!);

    await service.patch(sampleUrl, body: samplePayload);

    expect(request!.method, 'PATCH');
    expect(request!.headers.keys, contains('x-goog-api-client'));
  });

  test('delete', () async {
    final service = ServiceClient(client: httpClient!);

    await service.delete(sampleUrl);

    expect(request!.method, 'DELETE');
    expect(request!.headers.keys, contains('x-goog-api-client'));
  });
}
