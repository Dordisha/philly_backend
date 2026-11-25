import 'dart:convert';
import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:integration_test/integration_test.dart';
import 'package:philly_scan_app/config/api_config.dart';
import 'package:philly_scan_app/screens/property_records_page.dart';
import 'package:philly_scan_app/src/api_client.dart';
import 'package:philly_scan_app/src/api_service.dart';
import 'package:philly_scan_app/src/main.dart';
import 'package:philly_scan_app/src/violations_service.dart';
import 'package:philly_scan_app/widgets/premium_ui.dart';
import 'package:philly_scan_app/widgets/violation_summary_card.dart';

// Real transport only: records the responses consumed by the production widgets.
class ObservedClient extends ApiClient {
  final responses = <String, Map<String, dynamic>>{};
  @override
  Future<Map<String, dynamic>> get(String path, Map<String, String> params,
      {String? accessToken}) async {
    final data = await super.get(path, params, accessToken: accessToken);
    responses[path] = data;
    return data;
  }
}

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();

  testWidgets(
      'Lambert records and all crime radii use live matched staging without summary',
      (tester) async {
    expect(ApiConfig.baseUrl, ApiConfig.testBaseUrl);
    final client = ObservedClient();
    final api = ApiService(client: client);
    final violations = ViolationsService(client: client);
    final root = await client.get('/_root', {});
    expect(root['commit'],
        const String.fromEnvironment('EXPECTED_BACKEND_COMMIT'));

    await tester.pumpWidget(MaterialApp(
        theme: premiumTheme(),
        home: HomeScreen(
          apiService: api,
          violationsService: violations,
          recordsClient: client,
        )));
    await tester.enterText(find.byType(TextField), '1539 S LAMBERT ST');
    await tester.testTextInput.receiveAction(TextInputAction.done);
    await tester.pumpAndSettle(const Duration(milliseconds: 100),
        EnginePhase.sendSemanticsUpdate, const Duration(seconds: 120));

    expect(client.responses['/api/opa/search']?['result']?['opa'], '363258000');
    expect(client.responses.containsKey('/api/violations/summary'), isFalse);
    expect(find.byType(ViolationSummaryCard), findsNothing);
    expect(find.textContaining('We couldn’t load City violation records'),
        findsNothing);
    expect(find.textContaining('Low risk'), findsNothing);

    for (final section in [('311', '311'), ('L&I', 'violations')]) {
      await tester.ensureVisible(find.text(section.$1));
      await tester.tap(find.text(section.$1));
      await tester.pumpAndSettle(const Duration(milliseconds: 100),
          EnginePhase.sendSemanticsUpdate, const Duration(seconds: 120));
      expect(find.byType(PropertyRecordsPage), findsOneWidget);
      final data = client.responses['/api/records/${section.$2}']!;
      expect(data['ok'], true);
      expect(data['opa'], '363258000');
      expect(data['rows'], isNotEmpty);
      await tester.pageBack();
      await tester.pumpAndSettle();
    }

    await tester.ensureVisible(find.text('Crime trends'));
    await tester.tap(find.text('Crime trends'));
    await tester.pumpAndSettle(const Duration(milliseconds: 100),
        EnginePhase.sendSemanticsUpdate, const Duration(seconds: 120));
    for (final radius in [(402, '¼ mile'), (805, '½ mile'), (1609, '1 mile')]) {
      if (radius.$1 != 402) {
        await tester.tap(find.text(radius.$2));
        await tester.pumpAndSettle(const Duration(milliseconds: 100),
            EnginePhase.sendSemanticsUpdate, const Duration(seconds: 120));
      }
      final data = client.responses['/api/crime']!;
      expect(data['ok'], true);
      expect(data['opa'], '363258000');
      expect(data['radius_m'], radius.$1);
      expect(find.text('1539 S LAMBERT ST'), findsOneWidget);
      expect(find.text('${data['current_count']}'), findsWidgets);
      expect(find.text('${data['previous_count']}'), findsWidgets);
      expect(find.text('Crime records unavailable. No counts are shown.'),
          findsNothing);
      // Keep exact observations in CI without recording personal owner fields.
      debugPrint(jsonEncode({
        'radius_m': radius.$1,
        'current_count': data['current_count'],
        'previous_count': data['previous_count'],
        'source_date': data['latest_source_date']
      }));
    }
    debugPrint(
        'LIVE_RECORDS_VERIFIED backend=${root['commit']}');
    await tester.pumpWidget(const SizedBox());
    client.close();
  }, timeout: const Timeout(Duration(minutes: 8)));
}
