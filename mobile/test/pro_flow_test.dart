import 'dart:convert';
import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:http/http.dart' as http;
import 'package:http/testing.dart';
import 'package:philly_scan_app/config/pro_preview_config.dart';
import 'package:philly_scan_app/src/api_client.dart';
import 'package:philly_scan_app/src/api_service.dart';
import 'package:philly_scan_app/src/models/property_result.dart';
import 'package:philly_scan_app/src/main.dart';
import 'package:philly_scan_app/screens/pro_overview_page.dart';
import 'search_reliability_test.dart' show ControlledViolations;

class ImmediateProperty extends ApiService {
  int scans = 0;
  @override
  Future<PropertyResult> scan(String input, {String? opaOverride}) async {
    scans++;
    return PropertyResult(opa: '363258000', address: '1539 S LAMBERT ST');
  }
}

void main() {
  testWidgets(
    'Home Pro scans once and opens all four live-data features without another Scan tap',
    (tester) async {
      if (!ProPreviewConfig.enabled) return;
      final api = ImmediateProperty();
      final violations = ControlledViolations()..fail = false;
      final seen = <String>[];
      final client = ApiClient(
        client: MockClient((r) async {
          seen.add(r.url.path);
          final section = r.url.path.split('/').last;
          return http.Response(
            jsonEncode({
              'ok': true,
              'opa': '363258000',
              'subject_area': 1500,
              'source': {'label': 'City records'},
              'rows': section == '311'
                  ? [
                      {
                        'service_request_id': 15012021,
                        'service_name': 'Maintenance Complaint',
                        'status': 'Closed',
                      },
                    ]
                  : section == 'violations'
                  ? [
                      {
                        'violationnumber': '4170025',
                        'violationcodetitle': 'CLIP VIOLATION NOTICE',
                        'casestatus': 'CLOSED',
                      },
                    ]
                  : section == 'sales'
                  ? [
                      {
                        'location': 'SALE A',
                        'sale_price': 100000,
                        'total_livable_area': 1000,
                      },
                      {
                        'location': 'SALE B',
                        'sale_price': 200000,
                        'total_livable_area': 1000,
                      },
                      {
                        'location': 'SALE C',
                        'sale_price': 300000,
                        'total_livable_area': 1000,
                      },
                    ]
                  : [
                      {'parcel_number': '363258000'},
                    ],
              'signals': {
                'label': 'Weak evidence only',
                'note': 'Observed property context',
                'factors': [],
              },
            }),
            200,
          );
        }),
      );
      await tester.pumpWidget(
        MaterialApp(
          home: HomeScreen(
            apiService: api,
            violationsService: violations,
            recordsClient: client,
          ),
        ),
      );
      await tester.tap(find.text('Open Pro • Live data'));
      await tester.pumpAndSettle();
      expect(api.scans, 1);
      expect(find.byType(ProOverviewPage), findsOneWidget);
      expect(seen, isEmpty); // Fetch a section only when opened.
      for (final label in ['311', 'L&I', 'Resale', 'Signals']) {
        final scroll = find
            .descendant(
              of: find.byType(ListView),
              matching: find.byType(Scrollable),
            )
            .first;
        await tester.scrollUntilVisible(
          find.text(label),
          200,
          scrollable: scroll,
        );
        await tester.tap(find.text(label));
        await tester.pumpAndSettle();
        expect(find.text('Property records'), findsOneWidget);
        if (label == '311')
          expect(find.text('Maintenance Complaint'), findsOneWidget);
        if (label == 'L&I')
          expect(find.text('CLIP VIOLATION NOTICE'), findsOneWidget);
        if (label == 'Resale')
          expect(find.text(r'$225,000 – $375,000'), findsOneWidget);
        if (label == 'Signals')
          expect(
            find.text('Property signals: Weak evidence only'),
            findsOneWidget,
          );
        await tester.pageBack();
        await tester.pumpAndSettle();
      }
      expect(seen.toSet(), {
        '/api/records/311',
        '/api/records/violations',
        '/api/records/sales',
        '/api/records/facts',
      });
      await tester.pumpWidget(const SizedBox());
      client.close();
      api.close();
      violations.close();
    },
  );
  testWidgets('schools stay hidden and AI questions disclose missing data', (
    tester,
  ) async {
    if (!ProPreviewConfig.enabled) return;
    await tester.pumpWidget(
      const MaterialApp(
        home: ProOverviewPage(opa: '363258000', address: '1539 S LAMBERT ST'),
      ),
    );
    await tester.pumpAndSettle();
    await tester.scrollUntilVisible(
      find.text('Ask AI'),
      200,
      scrollable: find
          .descendant(
            of: find.byType(ListView),
            matching: find.byType(Scrollable),
          )
          .first,
    );
    expect(find.text('Schools'), findsNothing);
    // The lazy list can build a tile before its tap target enters the viewport.
    await tester.ensureVisible(find.text('Ask AI'));
    await tester.pumpAndSettle();
    expect(find.text('Ask AI').hitTestable(), findsOneWidget);
    await tester.tap(find.text('Ask AI').hitTestable());
    await tester.pumpAndSettle();
    expect(find.text('AI answers are not available yet.'), findsOneWidget);
    await tester.tap(find.text('What should I investigate first?'));
    await tester.pumpAndSettle();
    expect(find.text('Your question'), findsOneWidget);
    expect(tester.takeException(), isNull);
  });
}
