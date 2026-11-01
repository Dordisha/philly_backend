import 'dart:async';
import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:philly_scan_app/src/main.dart';
import 'package:philly_scan_app/src/api_service.dart';
import 'package:philly_scan_app/src/models/property_result.dart';
import 'package:philly_scan_app/src/violations_service.dart';
import 'package:philly_scan_app/widgets/violation_summary_card.dart';

class ControlledApi extends ApiService {
  final suggestions = <String, Completer<List<Map<String, dynamic>>>>{};
  final scans = <Completer<PropertyResult>>[];
  final opaChoices = <String?>[];
  @override
  Future<List<Map<String, dynamic>>> suggest(String query, {int limit = 10}) =>
      (suggestions[query] = Completer<List<Map<String, dynamic>>>()).future;
  @override
  Future<PropertyResult> scan(String input, {String? opaOverride}) {
    opaChoices.add(opaOverride);
    final pending = Completer<PropertyResult>();
    scans.add(pending);
    return pending.future;
  }
}

class ControlledViolations extends ViolationsService {
  final requested = <String>[];
  bool fail = true;
  @override
  Future<ViolationSummary> fetchSummary({required String opa}) async {
    requested.add(opa);
    if (fail) throw ApiException(message: 'Unavailable');
    return ViolationSummary(
      opa: opa,
      activeCount: 1,
      historicalCount: 2,
      lastActivity: null,
      riskLevel: 'Low',
      hasCourt: false,
      hasStopWork: false,
      hasUnsafeStructure: false,
      hasHazardous: false,
    );
  }
}

Future<void> home(WidgetTester tester, ControlledApi api) async {
  await tester.pumpWidget(MaterialApp(home: HomeScreen(apiService: api)));
  await tester.tap(find.byType(TextField));
  await tester.pump();
}

void main() {
  testWidgets('an older autocomplete response cannot replace a newer query', (
    tester,
  ) async {
    final api = ControlledApi();
    await home(tester, api);
    await tester.enterText(find.byType(TextField), 'old');
    await tester.pump(const Duration(milliseconds: 260));
    await tester.enterText(find.byType(TextField), 'new');
    // Old reply arrives before the new debounce fires.
    api.suggestions['old']!.complete([
      {'address': 'OLD RESULT'},
    ]);
    await tester.pump();
    expect(find.text('OLD RESULT'), findsNothing);
    await tester.pump(const Duration(milliseconds: 260));
    api.suggestions['new']!.complete([
      {'address': 'NEW RESULT'},
    ]);
    await tester.pump();
    expect(find.text('NEW RESULT'), findsOneWidget);
    await tester.pumpWidget(const SizedBox());
    api.close();
  });
  testWidgets(
    '404 shows choices and only scans a choice after an explicit tap',
    (tester) async {
      final api = ControlledApi();
      await home(tester, api);
      await tester.testTextInput.receiveAction(TextInputAction.done);
      await tester.pump();
      api.scans.single.completeError(
        ApiException(
          message: 'Address not found',
          statusCode: 404,
          suggestions: [
            {'address': 'NEIGHBOR', 'opa': '001234567'},
          ],
        ),
      );
      await tester.pump();
      expect(api.scans.length, 1);
      expect(find.text('Address not found'), findsOneWidget);
      await tester.ensureVisible(find.text('NEIGHBOR'));
      await tester.tap(find.text('NEIGHBOR'));
      await tester.pump();
      expect(api.scans.length, 2);
      expect(api.opaChoices.last, '001234567');
      await tester.pumpWidget(const SizedBox());
      api.scans.last.complete(
        PropertyResult(opa: '001234567', address: 'NEIGHBOR'),
      );
      await tester.pump();
      api.close();
    },
  );
  testWidgets('editing during a scan discards the previous result', (
    tester,
  ) async {
    final api = ControlledApi();
    await home(tester, api);
    await tester.testTextInput.receiveAction(TextInputAction.done);
    await tester.pump();
    await tester.enterText(find.byType(TextField), 'changed');
    api.scans.single.complete(
      PropertyResult(opa: '123456789', address: 'STALE PROPERTY'),
    );
    await tester.pump();
    expect(find.text('STALE PROPERTY'), findsNothing);
    expect(find.byType(CircularProgressIndicator), findsNothing);
    await tester.pumpWidget(const SizedBox());
    api.close();
  });
  testWidgets(
    'enforcement failure offers retry and reloads when the OPA changes',
    (tester) async {
      final service = ControlledViolations();
      Future<void> card(String opa) => tester.pumpWidget(
        MaterialApp(
          home: Scaffold(
            body: ViolationSummaryCard(opa: opa, service: service),
          ),
        ),
      );
      await card('111111111');
      await tester.pump();
      expect(find.textContaining('does not mean'), findsOneWidget);
      service.fail = false;
      await tester.tap(find.text('Retry enforcement'));
      await tester.pumpAndSettle();
      expect(find.text('Active: 1'), findsOneWidget);
      await card('222222222');
      await tester.pumpAndSettle();
      expect(service.requested, ['111111111', '111111111', '222222222']);
      service.close();
    },
  );
}
