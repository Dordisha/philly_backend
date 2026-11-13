import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:philly_scan_app/src/main.dart';
import 'package:philly_scan_app/src/api_service.dart';
import 'package:philly_scan_app/src/models/property_result.dart';
import 'package:philly_scan_app/config/pro_preview_config.dart';
import 'package:philly_scan_app/widgets/premium_ui.dart';
import 'search_reliability_test.dart' show ControlledViolations;

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
    'free aggregate and clipboard stay usable in either preview mode',
    (tester) async {
      final api = FreeProperty();
      final violations = ControlledViolations()..fail = false;
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
          home: HomeScreen(apiService: api, violationsService: violations),
        ),
      );
      await tester.ensureVisible(find.text('Scan'));
      await tester.tap(find.text('Scan'));
      await tester.pumpAndSettle();
      await tester.ensureVisible(find.text('Property Condition & Enforcement'));
      expect(find.text('Active: 1'), findsOneWidget);
      expect(find.text('History: 2'), findsOneWidget);
      expect(find.textContaining('Center City Core'), findsOneWidget);
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
      violations.close();
    },
  );
}
