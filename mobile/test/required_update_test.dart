import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:philly_scan_app/widgets/required_update_gate.dart';

void main() {
  test('only valid newer required policy blocks', () {
    final p = {
      'schema': 1,
      'bundleId': 'com.dorian.phillyscan',
      'required': true,
      'minimumVersion': '1.2.0',
      'releasedVersion': '1.2.0',
    };
    expect(policyRequiresUpdate(p, '1.1.0'), true);
    expect(policyRequiresUpdate(p, '1.2.0'), false);
    expect(policyRequiresUpdate({...p, 'required': false}, '1.1.0'), false);
    expect(
      policyRequiresUpdate({...p, 'releasedVersion': '1.0.3'}, '1.1.0'),
      false,
    );
    expect(
      policyRequiresUpdate({...p, 'releasedVersion': 'oops'}, '1.1.0'),
      false,
    );
    expect(policyRequiresUpdate({...p, 'bundleId': 'other'}, '1.1.0'), false);
  });
  testWidgets('verified requirement blocks navigation and supports rollback', (
    tester,
  ) async {
    var block = true;
    await tester.pumpWidget(
      MaterialApp(
        home: RequiredUpdateGate(
          check: () async => block,
          child: const Scaffold(body: Text('Property screen')),
        ),
      ),
    );
    await tester.pumpAndSettle();
    expect(find.text('Update to continue'), findsOneWidget);
    expect(find.text('Property screen'), findsNothing);
    expect(find.text('Later'), findsNothing);
    block = false;
    await tester.tap(find.text('Check again'));
    await tester.pumpAndSettle();
    expect(find.text('Property screen'), findsOneWidget);
  });
  testWidgets('initial policy outage leaves app available', (tester) async {
    await tester.pumpWidget(
      MaterialApp(
        home: RequiredUpdateGate(
          check: () async => throw Exception('offline'),
          child: const Scaffold(body: Text('Property screen')),
        ),
      ),
    );
    await tester.pumpAndSettle();
    expect(find.text('Property screen'), findsOneWidget);
  });
}
