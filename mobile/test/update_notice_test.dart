import 'dart:convert';
import 'package:flutter_test/flutter_test.dart';
import 'package:http/http.dart' as http;
import 'package:http/testing.dart';
import 'package:philly_scan_app/widgets/update_notice.dart';

void main() {
  test(
    'numeric versions handle multi-digit, equal, older and malformed values',
    () {
      expect(isNewerVersion('1.10.0', '1.9.0'), isTrue);
      expect(isNewerVersion('1.1', '1.1.0'), isFalse);
      expect(isNewerVersion('1.0.3', '1.1.0'), isFalse);
      expect(isNewerVersion('garbage', '1.1.0'), isFalse);
      expect(isNewerVersion('2.0.0', '1.1.0'), isTrue);
    },
  );
  test('only the correct App Store app can trigger an update', () async {
    var bundle = 'wrong.app';
    final client = MockClient(
      (_) async => http.Response(
        jsonEncode({
          'results': [
            {'bundleId': bundle, 'trackId': 6758272125, 'version': '1.2.0'},
          ],
        }),
        200,
      ),
    );
    final service = AppUpdateService(client);
    await expectLater(service.newerVersion(), throwsFormatException);
    bundle = 'com.dorian.phillyscan';
    expect(await service.newerVersion(), '1.2.0');
    client.close();
  });
  test('outage is not treated as up to date', () async {
    final client = MockClient((_) async => http.Response('offline', 503));
    await expectLater(
      AppUpdateService(client).newerVersion(),
      throwsFormatException,
    );
    client.close();
  });
}
