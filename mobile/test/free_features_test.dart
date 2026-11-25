import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:philly_scan_app/src/main.dart';
import 'package:philly_scan_app/src/api_service.dart';
import 'package:philly_scan_app/src/models/property_result.dart';
import 'package:philly_scan_app/config/pro_preview_config.dart';
import 'package:philly_scan_app/widgets/premium_ui.dart';
import 'package:philly_scan_app/widgets/violation_summary_card.dart';

class FreeProperty extends ApiService {
  @override
  Future<PropertyResult> scan(String input, {String? opaOverride}) async =>
      PropertyResult(
        opa: '123456789',
        address: '123 TEST ST',
        owner: 'TEST OWNER',
        zoning: 'CMX5',
      );
}

void main() {
  testWidgets(
    'summary is absent while property facts and free actions remain usable',
    (tester) async {
      final api = FreeProperty();
      String? copied;
      tester.binding.defaultBinaryMessenger.setMockMethodCallHandler(
        SystemChannels.platform,
        (call) async {
          if (call.method == 'Clipboard.setData')
            copied = call.arguments['text'];
          return null;
        },
      );
      await tester.pumpWidget(
        MaterialApp(
          theme: premiumTheme(),
          home: HomeScreen(apiService: api),
        ),
      );
      await tester.ensureVisible(find.text('Scan'));
      await tester.tap(find.text('Scan'));
      await tester.pumpAndSettle();
      expect(find.byType(ViolationSummaryCard), findsNothing);
      expect(find.text('Recorded L&I Violations'), findsNothing);
      expect(find.text('Retry enforcement'), findsNothing);
      expect(find.textContaining('Active:'), findsNothing);
      expect(find.textContaining('History:'), findsNothing);
      expect(find.textContaining('Center City Core'), findsOneWidget);
      for (final label in [
        '311',
        'L&I',
        'Zillow',
        'Crime trends',
        'Search by owner'
      ]) {
        expect(find.text(label), findsOneWidget);
      }
      for (final pair in [
        ('Copy Address', '123 TEST ST'),
        ('Copy Owner', 'TEST OWNER'),
        ('Copy OPA', '123456789'),
      ]) {
        await tester.ensureVisible(find.text(pair.$1));
        await tester.tap(find.text(pair.$1));
        await tester.pumpAndSettle();
        expect(copied, pair.$2);
      }
      expect(find.text('Your Voice Matters'), findsNothing);
      if (!ProPreviewConfig.enabled) {
        expect(find.text('Ask AI'), findsNothing);
        expect(find.text('Subscribe'), findsNothing);
        await tester.ensureVisible(find.text('See what’s planned').last);
        await tester.tap(find.text('See what’s planned').last);
        await tester.pumpAndSettle();
        expect(find.text('What’s planned'), findsOneWidget);
        expect(find.textContaining(r'$12.99/month'), findsOneWidget);
        expect(find.textContaining('No payment is collected'), findsOneWidget);
        expect(
          find.textContaining(
            'Albanian, Spanish, Vietnamese, Chinese, and English',
          ),
          findsOneWidget,
        );
        expect(find.textContaining(r'$2.99'), findsNothing);
      }
      expect(tester.takeException(), isNull);
      await tester.pumpWidget(const SizedBox());
      tester.binding.defaultBinaryMessenger.setMockMethodCallHandler(
        SystemChannels.platform,
        null,
      );
      api.close();
    },
  );
}
