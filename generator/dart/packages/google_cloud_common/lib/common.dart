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

import 'package:http/http.dart';

const _clientName = 'dart-test-client';

abstract class Jsonable {
  Object toJson();
}

abstract class CloudMessage {}

abstract class CloudEnum {
  final String value;

  const CloudEnum(this.value);

  @override
  int get hashCode => value.hashCode;

  Object toJson() => value;
}

abstract class CloudService {
  final Client client;

  CloudService({required this.client});

  Future<Map<String, dynamic>> $get(Uri url) async {
    final response = await client.get(
      url,
      headers: {
        'x-goog-api-client': _clientName,
      },
    );
    return _processResponse(response);
  }

  Future<Map<String, dynamic>> $post(Uri url, {Jsonable? body}) async {
    final response = await client.post(
      url,
      body: body == null ? null : jsonEncode(body.toJson()),
      headers: {
        'x-goog-api-client': _clientName,
        if (body != null) 'content-type': 'application/json',
      },
    );
    return _processResponse(response);
  }

  Future<Map<String, dynamic>> $patch(Uri url, {Jsonable? body}) async {
    final response = await client.patch(
      url,
      body: body == null ? null : jsonEncode(body.toJson()),
      headers: {
        'x-goog-api-client': _clientName,
        if (body != null) 'content-type': 'application/json',
      },
    );
    return _processResponse(response);
  }

  Future<Map<String, dynamic>> $delete(Uri url) async {
    final response = await client.delete(
      url,
      headers: {
        'x-goog-api-client': _clientName,
      },
    );
    return _processResponse(response);
  }

  Map<String, dynamic> _processResponse(Response response) {
    final statusOK = response.statusCode >= 200 && response.statusCode < 300;
    if (statusOK) {
      final body = response.body;
      return body.isEmpty ? {} : jsonDecode(body);
    }

    // TODO(#1454): This is a placeholder until we're able to parse 'Status'.
    throw ClientException('${response.statusCode}: ${response.reasonPhrase}');
  }
}
