import 'dart:async';
import 'dart:convert';
import 'package:flutter_test/flutter_test.dart';
import 'package:http/http.dart' as http;
import 'package:http/testing.dart';
import 'package:philly_scan_app/config/api_config.dart';
import 'package:philly_scan_app/src/api_client.dart';
import 'package:philly_scan_app/src/api_service.dart';
import 'package:philly_scan_app/src/violations_service.dart';

void main() {
  test('test builds use the existing repair staging origin', () {
    expect(ApiConfig.baseUrl, ApiConfig.testBaseUrl);
  });
  test(
    'lookup and enforcement use the same configured host and preserve OPA zeros',
    () async {
      final requests = <Uri>[];
      final transport = ApiClient(
        baseUrl: 'https://example.test/',
        client: MockClient((r) async {
          requests.add(r.url);
          return http.Response(
            jsonEncode(
              r.url.path.endsWith('search')
                  ? {
                      'result': {'opa': '001234567', 'address': '1 MAIN ST'},
                    }
                  : {
                      'ok': true,
                      'opa': '001234567',
                      'active_count': 1,
                      'historical_count': 2,
                      'flags': {},
                    },
            ),
            200,
          );
        }),
      );
      await ApiService(
        client: transport,
      ).scan('1 MAIN ST', opaOverride: '001234567');
      await ViolationsService(client: transport).fetchSummary(opa: '001234567');
      expect(requests.map((r) => r.host), everyElement('example.test'));
      expect(
        requests.map((r) => r.queryParameters['opa']),
        everyElement('001234567'),
      );
      transport.close();
    },
  );
  test(
    '404 alternatives are returned as choices, never silently scanned',
    () async {
      var count = 0;
      final api = ApiService(
        client: ApiClient(
          client: MockClient((_) async {
            count++;
            return http.Response(
              jsonEncode({
                'suggestions': [
                  {'address': 'NEIGHBOR', 'opa': '123456789'},
                ],
              }),
              404,
            );
          }),
        ),
      );
      await expectLater(
        api.scan('missing'),
        throwsA(
          isA<ApiException>()
              .having((e) => e.statusCode, 'status', 404)
              .having((e) => e.suggestions.length, 'choices', 1),
        ),
      );
      expect(count, 1);
      api.close();
    },
  );
  test('deadline produces a recoverable error', () async {
    final api = ApiClient(
      timeout: const Duration(milliseconds: 1),
      client: MockClient((_) => Completer<http.Response>().future),
    );
    await expectLater(
      api.get('/test', {}),
      throwsA(isA<ApiException>().having((e) => e.code, 'code', 'timeout')),
    );
    api.close();
  });
  test('connection failures are friendly', () async {
    final api = ApiClient(
      client: MockClient(
        (_) async => throw http.ClientException('private diagnostic'),
      ),
    );
    await expectLater(
      api.get('/test', {}),
      throwsA(
        isA<ApiException>()
            .having((e) => e.code, 'code', 'connection')
            .having(
              (e) => e.message,
              'safe message',
              isNot(contains('private')),
            ),
      ),
    );
    api.close();
  });
  test('HTML and malformed success responses do not leak into UI', () async {
    for (final body in ['<html>private error</html>', '[]']) {
      final api = ApiClient(
        client: MockClient((_) async => http.Response(body, 200)),
      );
      await expectLater(
        api.get('/test', {}),
        throwsA(
          isA<ApiException>().having((e) => e.code, 'code', 'invalid_response'),
        ),
      );
      api.close();
    }
  });
  test('upstream errors do not expose raw response body', () async {
    final api = ApiClient(
      client: MockClient(
        (_) async => http.Response('private upstream detail', 503),
      ),
    );
    await expectLater(
      api.get('/test', {}),
      throwsA(
        isA<ApiException>()
            .having((e) => e.statusCode, 'status', 503)
            .having((e) => e.message, 'message', isNot(contains('private'))),
      ),
    );
    api.close();
  });
  test(
    'incomplete enforcement data is unavailable rather than zero violations',
    () async {
      final service = ViolationsService(
        client: ApiClient(
          client: MockClient((_) async => http.Response('{"ok":true}', 200)),
        ),
      );
      await expectLater(
        service.fetchSummary(opa: '123456789'),
        throwsA(isA<ApiException>()),
      );
      service.close();
    },
  );
  test('missing property identity cannot produce a result card', () async {
    final api = ApiService(
      client: ApiClient(
        client: MockClient((_) async => http.Response('{"result":{}}', 200)),
      ),
    );
    await expectLater(api.scan('1 MAIN ST'), throwsA(isA<ApiException>()));
    api.close();
  });
}
