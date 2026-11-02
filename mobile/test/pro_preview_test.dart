import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:philly_scan_app/config/pro_preview_config.dart';
import 'package:philly_scan_app/widgets/pro_coming_soon.dart';

void main() {
  testWidgets('preview requires a scanned property; normal build stays gated', (tester) async {
    await tester.pumpWidget(MaterialApp(home: Builder(builder: (context) => Scaffold(
      body: TextButton(onPressed: () => showProComingSoonDialog(context), child: const Text('Upgrade')),
    ))));
    await tester.tap(find.text('Upgrade'));
    await tester.pumpAndSettle();
    expect(find.text(ProPreviewConfig.enabled ? 'Property records' : 'Coming Soon – Pro Feature'), findsOneWidget);
    if (ProPreviewConfig.enabled) expect(find.textContaining('Scan a property first'), findsOneWidget);
    expect(find.textContaining('SAMPLE DATA'), findsNothing);
  });
}
