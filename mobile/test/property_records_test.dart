import 'dart:convert';
import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:http/http.dart' as http;
import 'package:http/testing.dart';
import 'package:philly_scan_app/src/api_client.dart';
import 'package:philly_scan_app/screens/property_records_page.dart';

void main() {
  test('sale benchmark uses actual prices/areas, excludes nominal/missing and needs 3 sales', () {
    final rows = [
      {'sale_price': 100000, 'total_livable_area': 1000},
      {'sale_price': 200000, 'total_livable_area': 1000},
      {'sale_price': 300000, 'total_livable_area': 1000},
      {'sale_price': 1, 'total_livable_area': 1000},
      {'sale_price': 100000, 'total_livable_area': null},
    ];
    expect(saleBenchmark(rows, 1500), {'low': 225000.0, 'mid': 300000.0, 'high': 375000.0, 'count': 3.0});
    expect(saleBenchmark(rows.take(2).toList(), 1500), isNull);
    expect(saleBenchmark(rows, null), isNull);
  });
  testWidgets('records use selected parcel, show raw status, paginate and switch independently', (tester) async {
    final requests = <Uri>[];
    final client = ApiClient(client: MockClient((r) async {
      requests.add(r.url);
      return http.Response(jsonEncode({
        'ok': true, 'opa': '363258000', 'retrieved_at': '2026-11-01',
        'source': {'label': 'City records'}, 'note': 'Published source records',
        'has_more': true,
        'rows': [{'service_name': 'Maintenance Complaint', 'service_request_id': 123,
          'status': 'Closed'}],
      }), 200);
    }));
    await tester.pumpWidget(MaterialApp(home: PropertyRecordsPage(
      opa: '363258000', address: 'TEST PROPERTY', client: client,
    )));
    await tester.pumpAndSettle();
    expect(find.text('Maintenance Complaint'), findsOneWidget);
    expect(find.text('Status: Closed'), findsOneWidget);
    await tester.scrollUntilVisible(find.text('Next'), 200);
    await tester.tap(find.text('Next'));
    await tester.pumpAndSettle();
    expect(requests.last.queryParameters['offset'], '25');
    await tester.tap(find.text('L&I records'));
    await tester.pumpAndSettle();
    expect(requests.last.path, '/api/records/violations');
    expect(requests.last.queryParameters['offset'], '0');
    expect(requests.every((r) => r.queryParameters['opa'] == '363258000'), isTrue);
    client.close();
  });
  testWidgets('record source errors show retry, never successful empty records', (tester) async {
    var fail = true;
    final client = ApiClient(client: MockClient((_) async => fail
      ? http.Response('unavailable', 503)
      : http.Response(jsonEncode({'ok':true,'opa':'363258000','rows':[], 'note':'Exact address match'}), 200)));
    await tester.pumpWidget(MaterialApp(home: PropertyRecordsPage(
      opa: '363258000', address: 'TEST PROPERTY', client: client,
    )));
    await tester.pumpAndSettle();
    expect(find.text('Retry records'), findsOneWidget);
    expect(find.textContaining('No published records'), findsNothing);
    fail = false;
    await tester.tap(find.text('Retry records'));
    await tester.pumpAndSettle();
    expect(find.text('No published records matched these filters.'), findsOneWidget);
    client.close();
  });
}
