import 'api_client.dart';
import 'models/property_result.dart';
export 'api_client.dart' show ApiException;

class ApiService {
  final ApiClient _client;
  ApiService({ApiClient? client}) : _client = client ?? ApiClient();

  Future<List<Map<String, dynamic>>> suggest(
    String query, {
    int limit = 10,
  }) async {
    final q = query.trim();
    if (q.isEmpty) return [];
    final data = await _client.get('/api/opa/suggest', {
      'query': q,
      'limit': '$limit',
    });
    final raw = data['suggestions'];
    if (raw is! List)
      throw ApiException(
        message: 'Suggestions unavailable. You can still scan an address.',
      );
    return raw
        .whereType<Map>()
        .map((m) => m.map((k, v) => MapEntry(k.toString(), v)))
        .toList();
  }

  Future<PropertyResult> scan(String input, {String? opaOverride}) async {
    final q = input.trim();
    if (q.isEmpty)
      throw ApiException(message: 'Enter an address or OPA number.');
    final opa = opaOverride?.trim();
    final params =
        (opa != null && opa.isNotEmpty) || RegExp(r'^\d{6,12}$').hasMatch(q)
        ? {'opa': opa != null && opa.isNotEmpty ? opa : q}
        : {'address': q, 'limit': '1'};
    final data = await _client.get('/api/opa/search', params);
    final result = data['result'];
    if (result is! Map<String, dynamic> ||
        (result['opa'] ?? '').toString().trim().isEmpty ||
        (result['address'] ?? '').toString().trim().isEmpty) {
      throw ApiException(
        message: 'The property could not be identified. Please try again.',
      );
    }
    return PropertyResult.fromJson(result);
  }

  void close() => _client.close();
}
