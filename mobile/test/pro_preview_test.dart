import 'package:philly_scan_app/services/pro_subscription.dart';
import 'package:philly_scan_app/screens/pro_overview_page.dart';
import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:philly_scan_app/config/pro_preview_config.dart';
import 'package:philly_scan_app/widgets/pro_coming_soon.dart';

void main() {
  TestWidgetsFlutterBinding.ensureInitialized();
  final messenger = TestDefaultBinaryMessengerBinding.instance.defaultBinaryMessenger;
  setUp(() {
    messenger.setMockMethodCallHandler(ProSubscription.channel, (call) async {
      if (call.method == 'product') return {'price': r'$2.99'};
      return {'active': false};
    });
  });
  tearDown(() {
    messenger.setMockMethodCallHandler(ProSubscription.channel, null);
  });
  testWidgets('preview requires a scanned property; normal build stays gated', (
    tester,
  ) async {
    await tester.pumpWidget(
      MaterialApp(
        home: Builder(
          builder: (context) => Scaffold(
            body: TextButton(
              onPressed: () => showProComingSoonDialog(context),
              child: const Text('Upgrade'),
            ),
          ),
        ),
      ),
    );
    await tester.tap(find.text('Upgrade'));
    await tester.pumpAndSettle();
    if (!ProPreviewConfig.enabled)
      expect(find.text('Philly Scan Pro'), findsOneWidget);
    if (ProPreviewConfig.enabled)
      expect(find.textContaining('Scan a property first'), findsOneWidget);
    expect(find.byType(ProOverviewPage), findsNothing);
    expect(find.textContaining('SAMPLE DATA'), findsNothing);
  });
}
