import 'dart:convert';
import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:http/http.dart' as http;
import 'package:http/testing.dart';
import 'package:philly_scan_app/src/api_client.dart';
import 'package:philly_scan_app/src/violations_service.dart';
import 'package:philly_scan_app/widgets/violation_summary_card.dart';

Map<String, dynamic> summary() => {
  'ok': true, 'summary_version': 2, 'opa': '363258000',
  'status_mapping': 'unverified', 'total_count': 2,
  'last_activity': null,
  'status_counts': [{'casestatus': 'CLOSED', 'violationstatus': 'OPEN', 'count': 2}],
};
ViolationsService serviceFor(Map<String, dynamic> body) => ViolationsService(
  client: ApiClient(client: MockClient((_) async => http.Response(jsonEncode(body), 200))),
);

void main() {
  test('summary rejects legacy, malformed and wrong-property responses', () async {
    final invalid = <Map<String, dynamic>>[
      {'ok': true, 'active_count': 0, 'historical_count': 0, 'flags': {}},
      {...summary(), 'opa': '999999999'},
      {...summary(), 'total_count': 0},
      {...summary(), 'total_count': -1},
      {...summary(), 'total_count': 2.5},
      {...summary(), 'total_count': '2'},
      {...summary(), 'status_counts': null},
      {...summary(), 'status_counts': [null]},
      {...summary(), 'status_counts': [{'casestatus': false, 'violationstatus': 'OPEN', 'count': 2}]},
      {...summary(), 'status_counts': [{'casestatus': null, 'violationstatus': null, 'count': -2}]},
      {...summary(), 'total_count': 4, 'status_counts': [...summary()['status_counts'] as List, ...summary()['status_counts'] as List]},
      {...summary(), 'last_activity': 'invalid'},
      {...summary(), 'total_count': 0, 'status_counts': [], 'last_activity': '2026-01-01'},
    ];
    for (final body in invalid) {
      final service = serviceFor(body);
      await expectLater(service.fetchSummary(opa: '363258000'), throwsA(isA<ApiException>()));
      service.close();
    }
  });
  testWidgets('raw conflicting case and violation statuses do not imply active or safe', (tester) async {
    final service = serviceFor(summary());
    await tester.pumpWidget(MaterialApp(home: Scaffold(body: ViolationSummaryCard(opa: '363258000', service: service))));
    await tester.pumpAndSettle();
    expect(find.text('Records: 2'), findsOneWidget);
    expect(find.text('Case: CLOSED · Violation: OPEN · Records: 2'), findsOneWidget);
    expect(find.textContaining('Active:'), findsNothing);
    expect(find.textContaining('History:'), findsNothing);
    expect(find.textContaining('Low'), findsNothing);
    expect(find.textContaining('No flags'), findsNothing);
    expect(find.textContaining('risk are not inferred'), findsOneWidget);
    await tester.pumpWidget(const SizedBox());
    service.close();
  });
  testWidgets('valid zero, unknown status and unavailable remain distinct', (tester) async {
    final bodies = [
      {...summary(), 'total_count': 0, 'status_counts': []},
      {...summary(), 'status_counts': [{'casestatus': null, 'violationstatus': null, 'count': 2}]},
      {'ok': true},
    ];
    for (var i = 0; i < bodies.length; i++) {
      final service = serviceFor(bodies[i]);
      await tester.pumpWidget(MaterialApp(home: Scaffold(body: ViolationSummaryCard(opa: '363258000', service: service))));
      await tester.pumpAndSettle();
      if (i == 0) {
        expect(find.text('Records: 0'), findsOneWidget);
        expect(find.textContaining('This is not a safety assessment'), findsOneWidget);
      } else if (i == 1) {
        expect(find.text('Case: Not reported · Violation: Not reported · Records: 2'), findsOneWidget);
      } else {
        expect(find.textContaining('Enforcement records unavailable'), findsOneWidget);
        expect(find.text('Records: 0'), findsNothing);
      }
      await tester.pumpWidget(const SizedBox());
      service.close();
    }
  });
}
