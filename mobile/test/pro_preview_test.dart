import 'package:philly_scan_app/screens/pro_overview_page.dart';
import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:philly_scan_app/widgets/pro_coming_soon.dart';

void main() {
  TestWidgetsFlutterBinding.ensureInitialized();
  testWidgets(
    'free records require a scanned property without a purchase screen',
    (tester) async {
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
      expect(find.textContaining('Scan a property first'), findsOneWidget);
      expect(find.byType(ProOverviewPage), findsNothing);
      expect(find.textContaining('SAMPLE DATA'), findsNothing);
    },
  );
}
