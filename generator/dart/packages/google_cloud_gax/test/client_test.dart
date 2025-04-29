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
