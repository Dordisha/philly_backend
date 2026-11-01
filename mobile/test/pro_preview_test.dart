import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:philly_scan_app/config/pro_preview_config.dart';
import 'package:philly_scan_app/widgets/pro_coming_soon.dart';

void main() {
  testWidgets('Upgrade stays locked unless debug preview is opted in', (
    tester,
  ) async {
    await tester.pumpWidget(MaterialApp(
      home: Builder(builder: (context) => Scaffold(
        body: TextButton(
          onPressed: () => showProComingSoonDialog(context),
          child: const Text('Upgrade'),
        ),
      )),
    ));
    await tester.tap(find.text('Upgrade'));
    await tester.pumpAndSettle();
    if (!ProPreviewConfig.enabled) {
      expect(find.text('Coming Soon – Pro Feature'), findsOneWidget);
      expect(find.text('Pro preview'), findsNothing);
      return;
    }
    expect(find.text('Pro preview'), findsOneWidget);
    expect(find.textContaining('SAMPLE DATA'), findsOneWidget);
    for (final title in [
      '311 complaint details',
      'Open violation details',
      'Renovation resale analysis',
    ]) {
      await tester.scrollUntilVisible(find.text(title), 200);
      await tester.tap(find.text(title));
      await tester.pumpAndSettle();
      expect(tester.takeException(), isNull);
    }
    await tester.scrollUntilVisible(find.textContaining('Sample value scenarios'), 200);
    expect(find.textContaining('Sample value scenarios'), findsOneWidget);
    await tester.pageBack();
    await tester.pumpAndSettle();
    expect(find.text('Upgrade'), findsOneWidget);
  });
}
