import 'dart:convert';
import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:http/http.dart' as http;
import 'package:http/testing.dart';
import 'package:philly_scan_app/src/api_client.dart';
import 'package:philly_scan_app/screens/owner_search_page.dart';
import 'package:philly_scan_app/widgets/access_information.dart';

void main() {
  testWidgets('owner records load without StoreKit or a subscription', (
    tester,
  ) async {
    var calls = 0;
    final client = ApiClient(
      client: MockClient((request) async {
        calls++;
        expect(request.url.path, '/api/v2/sql');
        expect(request.headers['Authorization'], isNull);
        return http.Response(
          jsonEncode({
            'rows': [
              {
                'parcel_number': '123456789',
                'location': '123 TEST ST',
                'owner_1': 'TEST OWNER',
                'owner_2': null,
              },
            ],
          }),
          200,
        );
      }),
    );
    await tester.pumpWidget(
      MaterialApp(
        home: OwnerSearchPage(owner: 'TEST OWNER', client: client),
      ),
    );
    await tester.pumpAndSettle();
    expect(calls, 1);
    expect(find.text('123 TEST ST'), findsOneWidget);
    expect(find.text('Upgrade to Pro'), findsNothing);
    expect(tester.takeException(), isNull);
    await tester.pumpWidget(const SizedBox());
    client.close();
  });

  testWidgets(
    'access information explains free sources and offers no purchase',
    (tester) async {
      await tester.pumpWidget(const MaterialApp(home: AccessInformationPage()));
      expect(
        find.textContaining('No subscription is required'),
        findsOneWidget,
      );
      expect(
        find.textContaining('Not available to purchase yet'),
        findsOneWidget,
      );
      expect(
        find.textContaining('not affiliated with or endorsed'),
        findsOneWidget,
      );
      expect(find.textContaining(r'$2.99'), findsNothing);
      expect(find.text('Subscribe'), findsNothing);
    },
  );
}
