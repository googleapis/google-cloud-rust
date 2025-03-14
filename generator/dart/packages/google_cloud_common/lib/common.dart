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
import 'package:http/http.dart';

const String _clientName = 'dart-test-client';

/// An abstract class that can return a JSON encodable representation of itself.
///
/// Classes that implement [JsonEncodable] will often have a `fromJson()`
/// constructor.
abstract class JsonEncodable {
  Object toJson();
}

abstract class Message implements JsonEncodable {}

abstract class Enum implements JsonEncodable {
  final String value;

  const Enum(this.value);

  @override
  int get hashCode => value.hashCode;

  @override
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

  Future<Map<String, dynamic>> $post(Uri url, {JsonEncodable? body}) async {
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

  Future<Map<String, dynamic>> $patch(Uri url, {JsonEncodable? body}) async {
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

    Status status;

    try {
      final json = jsonDecode(response.body);
      status = Status.fromJson(json['error']);
    } catch (_) {
      // If we're not able to parse the Status error, return a general HTTP
      // exception.
      throw ClientException('${response.statusCode}: ${response.reasonPhrase}');
    }

    throw status;
  }
}

T? $decode<T, S>(dynamic json, T Function(S) decoder) {
  return json == null ? null : decoder(json);
}

List<T>? $decodeList<T, S>(dynamic json, T Function(S) decoder) {
  return (json as List?)?.map((item) => decoder(item)).toList().cast();
}

Map<String, T>? $decodeMap<T>(
    dynamic json, T Function(Map<String, dynamic>) decoder) {
  return (json as Map?)
      ?.map((key, value) => MapEntry(key, decoder(value)))
      .cast();
}

List? $encodeList(List<JsonEncodable>? items) {
  return items?.map((item) => item.toJson()).toList();
}

Map? $encodeMap(Map<String, JsonEncodable>? items) {
  return items?.map((key, value) => MapEntry(key, value.toJson()));
}
