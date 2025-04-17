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
import 'package:http/http.dart' as http;

const String _clientName = 'dart-test-client';

/// An abstract class that can return a JSON encodable representation of itself.
///
/// Classes that implement [JsonEncodable] will often have a `fromJson()`
/// constructor.
abstract class JsonEncodable {
  Object? toJson();
}

/// The abstract common superclass of all messages.
abstract class Message implements JsonEncodable {
  /// The fully qualified name of this message, i.e., `google.protobuf.Duration`
  /// or `google.rpc.ErrorInfo`
  final String qualifiedName;

  Message(this.qualifiedName);
}

/// The abstract common superclass of all enum values.
abstract class Enum implements JsonEncodable {
  final String value;

  const Enum(this.value);

  @override
  String toJson() => value;

  @override
  bool operator ==(Object other) {
    return other.runtimeType == runtimeType && value == (other as Enum).value;
  }

  @override
  int get hashCode => value.hashCode;
}

class ServiceClient {
  final http.Client client;

  ServiceClient({required this.client});

  Future<Map<String, dynamic>> get(Uri url) async {
    final response = await client.get(
      url,
      headers: {
        'x-goog-api-client': _clientName,
      },
    );
    return _processResponse(response);
  }

  Future<Map<String, dynamic>> post(Uri url, {JsonEncodable? body}) async {
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

  Future<Map<String, dynamic>> patch(Uri url, {JsonEncodable? body}) async {
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

  Future<Map<String, dynamic>> delete(Uri url) async {
    final response = await client.delete(
      url,
      headers: {
        'x-goog-api-client': _clientName,
      },
    );
    return _processResponse(response);
  }

  /// Closes the client and cleans up any resources associated with it.
  ///
  /// Once [close] is called, no other methods should be called.
  void close() => client.close();

  Map<String, dynamic> _processResponse(http.Response response) {
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
      throw http.ClientException(
          '${response.statusCode}: ${response.reasonPhrase}');
    }

    throw status;
  }
}
