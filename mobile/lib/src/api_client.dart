import 'dart:async';
import 'dart:convert';
import 'package:http/http.dart' as http;
import '../config/api_config.dart';

class ApiException implements Exception {
  final String message;
  final int? statusCode;
  final String? code;
  final List<Map<String, dynamic>> suggestions;
  ApiException({
    required this.message,
    this.statusCode,
    this.code,
    this.suggestions = const [],
  });
  @override
  String toString() => message;
}

/// Shared deadlines and safe errors; never display upstream response bodies.
class ApiClient {
  final http.Client _client;
  final String baseUrl;
  final Duration timeout;
  ApiClient({
    http.Client? client,
    String? baseUrl,
    this.timeout = const Duration(seconds: 45),
  }) : _client = client ?? http.Client(),
       baseUrl = baseUrl ?? ApiConfig.baseUrl;

  Future<Map<String, dynamic>> get(
    String path,
    Map<String, String> params,
  ) async {
    try {
      final uri = Uri.parse(
        '${baseUrl.replaceFirst(RegExp(r"/$"), "")}$path',
      ).replace(queryParameters: params);
      final response = await _client
          .get(uri, headers: const {'Accept': 'application/json'})
          .timeout(timeout);
      if (response.statusCode < 200 || response.statusCode >= 300) {
        if (response.statusCode == 404) {
          List<Map<String, dynamic>> suggestions = const [];
          try {
            final data = jsonDecode(response.body);
            if (data is Map && data['suggestions'] is List) {
              suggestions = (data['suggestions'] as List)
                  .whereType<Map>()
                  .map((m) => m.map((k, v) => MapEntry(k.toString(), v)))
                  .toList();
            }
          } on FormatException {
            /* The status still identifies not found. */
          }
          throw ApiException(
            message:
                'Address not found. Check the address or choose a suggestion.',
            statusCode: 404,
            suggestions: suggestions,
          );
        }
        throw ApiException(
          message: 'Property data is unavailable. Please try again.',
          statusCode: response.statusCode,
        );
      }
      final decoded = jsonDecode(response.body);
      if (decoded is! Map<String, dynamic>) throw const FormatException();
      return decoded;
    } on TimeoutException {
      throw ApiException(
        message: 'The request took too long. Please try again.',
        code: 'timeout',
      );
    } on http.ClientException {
      throw ApiException(
        message: 'Cannot connect. Check your connection and try again.',
        code: 'connection',
      );
    } on FormatException {
      throw ApiException(
        message: 'Property data could not be read. Please try again.',
        code: 'invalid_response',
      );
    }
  }

  void close() => _client.close();
}
