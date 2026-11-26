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
  for (final invalid in [
    null,
    {'parcel_number': '123456789', 'location': '', 'owner_1': 'TEST OWNER'},
    {'parcel_number': '123456789', 'location': 123, 'owner_1': 'TEST OWNER'},
    {'parcel_number': '123456789', 'location': 'WRONG PROPERTY', 'owner_1': 'TEST OWNER', 'owner_2': 123},
    {'parcel_number': 'bad', 'location': 'WRONG PROPERTY', 'owner_1': 'TEST OWNER'},
    {'parcel_number': '123456789', 'location': 'WRONG PROPERTY', 'owner_1': 'OTHER OWNER'},
  ]) {
    testWidgets('free owner search rejects mismatched identity $invalid', (tester) async {
      final client = ApiClient(client: MockClient((_) async =>
        http.Response(jsonEncode({'rows': [invalid]}), 200)));
      await tester.pumpWidget(MaterialApp(home: OwnerSearchPage(owner: 'TEST OWNER', client: client)));
      await tester.pumpAndSettle();
      expect(find.text('WRONG PROPERTY'), findsNothing);
      expect(find.text('Owner records unavailable. Please retry.'), findsOneWidget);
      await tester.pumpWidget(const SizedBox());
      client.close();
    });
  }

  testWidgets('free pagination retries the same cursor and opens the exact OPA', (tester) async {
    Map<String, dynamic> record(int index) => {
      'parcel_number': (100000000 + index).toString(),
      'location': '$index TEST ST',
      'owner_1': 'OTHER PERSON',
      'owner_2': 'TEST OWNER',
    };
    var pageTwoCalls = 0;
    final client = ApiClient(client: MockClient((request) async {
      expect(request.headers['Authorization'], isNull);
      final sql = request.url.queryParameters['q']!;
      if (!sql.contains('parcel_number >')) {
        return http.Response(jsonEncode({'rows': List.generate(26, (i) => record(i + 1))}), 200);
      }
      expect(sql, contains("parcel_number > '100000025'"));
      pageTwoCalls++;
      if (pageTwoCalls == 1) return http.Response('unavailable', 503);
      return http.Response(jsonEncode({'rows': [record(26)]}), 200);
    }));
    String? opened;
    await tester.pumpWidget(MaterialApp(home: Builder(builder: (context) =>
      TextButton(onPressed: () async {
        opened = await Navigator.push<String>(context, MaterialPageRoute(builder: (_) =>
          OwnerSearchPage(owner: 'TEST OWNER', client: client)));
      }, child: const Text('Open owners')))));
    await tester.tap(find.text('Open owners'));
    await tester.pumpAndSettle();
    expect(find.text('26 TEST ST'), findsNothing);
    await tester.scrollUntilVisible(find.text('Load more'), 400, maxScrolls: 30);
    await tester.tap(find.text('Load more'));
    await tester.pumpAndSettle();
    await tester.scrollUntilVisible(find.text('Retry'), -400, maxScrolls: 30);
    expect(find.text('Owner records unavailable. Please retry.'), findsOneWidget);
    expect(find.text('No matching properties found.'), findsNothing);
    await tester.tap(find.text('Retry'));
    await tester.pumpAndSettle();
    expect(pageTwoCalls, 2);
    expect(find.text('Retry'), findsNothing);
    expect(find.text('Load more'), findsNothing);
    await tester.scrollUntilVisible(find.text('26 TEST ST'), 400, maxScrolls: 30);
    await tester.tap(find.text('26 TEST ST'));
    await tester.pumpAndSettle();
    expect(opened, '100000026');
    expect(tester.takeException(), isNull);
    await tester.pumpWidget(const SizedBox());
    client.close();
  });

  testWidgets('oversized and invalid lookahead pages show unavailable without partial results', (tester) async {
    final valid = {'parcel_number': '123456789', 'location': 'VALID TEST ST', 'owner_1': 'TEST OWNER'};
    for (final rows in [
      List.generate(27, (i) => {...valid, 'parcel_number': (100000000 + i).toString()}),
      [...List.generate(25, (i) => {...valid, 'parcel_number': (100000000 + i).toString()}),
        {'parcel_number': '100000025', 'location': ' ', 'owner_1': 'TEST OWNER'}],
    ]) {
      final client = ApiClient(client: MockClient((_) async => http.Response(jsonEncode({'rows': rows}), 200)));
      await tester.pumpWidget(MaterialApp(home: OwnerSearchPage(owner: 'TEST OWNER', client: client)));
      await tester.pumpAndSettle();
      expect(find.text('VALID TEST ST'), findsNothing);
      expect(find.text('Owner records unavailable. Please retry.'), findsOneWidget);
      expect(find.text('No matching properties found.'), findsNothing);
      await tester.pumpWidget(const SizedBox());
      client.close();
    }
  });

}
