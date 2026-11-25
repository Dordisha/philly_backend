import 'dart:convert';
import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:http/http.dart' as http;
import 'package:http/testing.dart';
import 'package:philly_scan_app/src/api_client.dart';
import 'package:philly_scan_app/screens/crime_trends_page.dart';

void main() {
  testWidgets('temporary crime failure retries once without showing zero',
      (tester) async {
    var calls = 0;
    final client = ApiClient(client: MockClient((request) async {
      calls++;
      if (calls == 1) return http.Response('temporarily unavailable', 503);
      return http.Response(
          jsonEncode({
            'ok': true,
            'opa': '363258000',
            'radius_m': 402,
            'current_count': 130,
            'previous_count': 141,
            'change_percent': -7.8,
            'stale': false,
            'categories': [],
            'months': [],
            'current_period': {'from': '2026-08-26', 'through': '2026-11-23'},
            'previous_period': {'from': '2026-05-28', 'through': '2026-08-25'},
          }),
          200);
    }));
    await tester.pumpWidget(MaterialApp(
        home: CrimeTrendsPage(
      opa: '363258000',
      address: '1539 S LAMBERT ST',
      client: client,
    )));
    await tester.pumpAndSettle();
    expect(calls, 2);
    expect(find.text('130'), findsOneWidget);
    expect(find.text('141'), findsOneWidget);
    expect(find.text('Crime records unavailable. No counts are shown.'),
        findsNothing);
    await tester.pumpWidget(const SizedBox());
    client.close();
  });
  testWidgets('crime permanent errors and unrelated results never become zero',
      (tester) async {
    for (final status in [404, 503, 200]) {
      var calls = 0;
      final client = ApiClient(client: MockClient((_) async {
        calls++;
        return http.Response(
            status == 200
                ? jsonEncode({
                    'ok': true,
                    'opa': '999999999',
                    'radius_m': 402,
                    'current_count': 0,
                    'previous_count': 0,
                  })
                : 'unavailable',
            status);
      }));
      await tester.pumpWidget(MaterialApp(
          home: CrimeTrendsPage(
        opa: '363258000',
        address: '1539 S LAMBERT ST',
        client: client,
      )));
      await tester.pumpAndSettle();
      expect(calls, status == 503 ? 2 : 1);
      expect(find.text('Crime records unavailable. No counts are shown.'),
          findsOneWidget);
      expect(find.text('Not calculated'), findsNothing);
      await tester.pumpWidget(const SizedBox());
      client.close();
    }
  });
  testWidgets(
    'crime uses selected OPA and radius, preserves zero baseline and retries errors',
    (tester) async {
      final requests = <Uri>[];
      bool fail = false;
      final client = ApiClient(
        client: MockClient((r) async {
          requests.add(r.url);
          return http.Response(
            fail
                ? 'unavailable'
                : jsonEncode({
                    'ok': true,
                    'opa': '782273400',
                    'radius_m': int.parse(r.url.queryParameters['radius_m']!),
                    'current_count': 0,
                    'previous_count': 0,
                    'change_percent': null,
                    'stale': false,
                    'current_period': {
                      'from': '2026-08-04',
                      'through': '2026-11-01',
                    },
                    'previous_period': {
                      'from': '2026-05-06',
                      'through': '2026-08-03',
                    },
                    'categories': [],
                    'months': [],
                    'latest_source_date': '2026-11-01',
                    'retrieved_at': '2026-11-02',
                    'note': 'Approximate locations',
                  }),
            fail ? 503 : 200,
          );
        }),
      );
      await tester.pumpWidget(
        MaterialApp(
          home: CrimeTrendsPage(
            opa: '782273400',
            address: 'City Hall',
            client: client,
          ),
        ),
      );
      await tester.pumpAndSettle();
      expect(requests.last.queryParameters, {
        'opa': '782273400',
        'radius_m': '402',
      });
      expect(find.text('Not calculated'), findsOneWidget);
      fail = true;
      await tester.tap(find.text('½ mile'));
      await tester.pumpAndSettle();
      expect(requests.last.queryParameters['radius_m'], '805');
      expect(
        find.text('Crime records unavailable. No counts are shown.'),
        findsOneWidget,
      );
      expect(find.text('Not calculated'), findsNothing);
      fail = false;
      await tester.tap(find.text('Retry'));
      await tester.pumpAndSettle();
      expect(find.text('Not calculated'), findsOneWidget);
      client.close();
    },
  );
}
