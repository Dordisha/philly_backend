import 'dart:async';
import 'dart:convert';
import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:http/http.dart' as http;
import 'package:http/testing.dart';
import 'package:philly_scan_app/config/pro_preview_config.dart';
import 'package:philly_scan_app/screens/owner_search_page.dart';
import 'package:philly_scan_app/services/pro_subscription.dart';
import 'package:philly_scan_app/src/api_client.dart';

void main() {
  TestWidgetsFlutterBinding.ensureInitialized();
  final messenger = TestDefaultBinaryMessengerBinding.instance.defaultBinaryMessenger;
  final store = ProSubscription.instance;
  var active = false;
  final requests = <http.Request>[];
  late Future<http.Response> Function(http.Request) reply;
  late ApiClient client;

  setUp(() {
    active = false;
    requests.clear();
    messenger.setMockMethodCallHandler(ProSubscription.channel, (_) async => {
      'active': active,
      'expiresAt': DateTime.now().add(const Duration(hours: 1)).millisecondsSinceEpoch,
    });
    reply = (_) async => http.Response('{}', 403);
    client = ApiClient(baseUrl: 'https://protected.example.test', client: MockClient((r) {
      requests.add(r);
      return reply(r);
    }));
  });
  tearDown(() async {
    active = false;
    await store.refresh();
    client.close();
    messenger.setMockMethodCallHandler(ProSubscription.channel, null);
  });
  Future<void> open(WidgetTester tester, {bool paid = true}) async {
    active = paid;
    await store.refresh();
    await tester.pumpWidget(MaterialApp(home: OwnerSearchPage(
      owner: 'DISHANI DORIAN', opa: '363258000', client: client,
    )));
    await tester.pumpAndSettle();
  }
  http.Response page({String? cursor}) => http.Response(jsonEncode({
    'ok': true,
    'rows': [{
      'parcel_number': '100000001', 'location': 'FIXTURE ADDRESS',
      'owner_1': 'DISHANI DORIAN', 'owner_2': null,
    }],
    'next_cursor': cursor,
    'note': 'Fixture response, not live records.',
  }), 200);

  // Explicitly skipped in the preview run; the preview-disabled CI run executes
  // every case. No return-early test is counted as testing the protected path.
  group('protected owner search', () {
    testWidgets('Free is locked before any data request', (tester) async {
      await open(tester, paid: false);
      expect(find.text('Pro subscription required.'), findsOneWidget);
      expect(find.text('Upgrade to Pro'), findsOneWidget);
      expect(requests, isEmpty);
    });
    testWidgets('local paid state cannot bypass server denial', (tester) async {
      await open(tester);
      expect(requests, hasLength(1));
      expect(requests.single.url.host, 'protected.example.test');
      expect(requests.single.url.path, '/api/owners');
      expect(requests.single.url.queryParameters, {'name': 'DISHANI DORIAN'});
      expect(requests.single.headers['Authorization'], isNull);
      expect(find.text('Pro subscription required.'), findsOneWidget);
      expect(find.text('No matching properties found.'), findsNothing);
    });
    testWidgets('outages and malformed success never become empty or fallback', (tester) async {
      for (final response in [http.Response('{}', 404), http.Response('{}', 503), http.Response('{"ok":false,"rows":[]}', 200)]) {
        reply = (_) async => response;
        await tester.pumpWidget(const SizedBox());
        await open(tester);
        expect(find.text('Owner records unavailable. Please retry.'), findsOneWidget);
        expect(find.text('No matching properties found.'), findsNothing);
      }
      expect(requests, hasLength(3));
      expect(requests.map((r) => r.url.path), everyElement('/api/owners'));
    });
    testWidgets('pagination is server controlled and subsequent denial clears rows', (tester) async {
      reply = (_) async => requests.length == 1 ? page(cursor: '100000001') : http.Response('{}', 403);
      await open(tester);
      expect(find.text('FIXTURE ADDRESS'), findsOneWidget);
      await tester.ensureVisible(find.text('Load more'));
      await tester.tap(find.text('Load more'));
      await tester.pumpAndSettle();
      expect(requests, hasLength(2));
      expect(requests.last.url.queryParameters['after'], '100000001');
      expect(find.text('FIXTURE ADDRESS'), findsNothing);
      expect(find.text('Fixture response, not live records.'), findsNothing);
      expect(find.text('Pro subscription required.'), findsOneWidget);
    });
    testWidgets('revocation clears records and discards a pending page', (tester) async {
      final pending = Completer<http.Response>();
      reply = (_) async => requests.length == 1 ? page(cursor: '100000001') : pending.future;
      await open(tester);
      await tester.ensureVisible(find.text('Load more'));
      await tester.tap(find.text('Load more'));
      await tester.pump();
      expect(requests, hasLength(2));
      active = false;
      await store.refresh();
      await tester.pump();
      expect(find.text('FIXTURE ADDRESS'), findsNothing);
      pending.complete(page());
      await tester.pumpAndSettle();
      expect(find.text('FIXTURE ADDRESS'), findsNothing);
      expect(find.text('Pro subscription required.'), findsOneWidget);
    });
  }, skip: ProPreviewConfig.enabled ? 'Protected path executes with PRO_PREVIEW=false.' : false);
}
