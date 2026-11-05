import 'package:flutter/material.dart';
import '../screens/property_records_page.dart';
import '../screens/pro_overview_page.dart';
import '../src/api_client.dart';
import 'pro_paywall.dart';

Future<void> showProComingSoonDialog(
  BuildContext context, {
  String? opa,
  String address = '',
  int initialSection = -1,
  ApiClient? recordsClient,
  String title = 'Property records',
  String message = '',
}) async {
  if (!await requirePro(context) || !context.mounted) return;
  if (opa == null || opa.isEmpty) {
    ScaffoldMessenger.of(context).showSnackBar(
      const SnackBar(
        content: Text('Scan a property first, then open its Pro records.'),
      ),
    );
    return;
  }
  await Navigator.of(context).push<void>(
    MaterialPageRoute(
      builder: (_) => ProAccessGate(
        child: initialSection < 0
            ? ProOverviewPage(opa: opa, address: address, client: recordsClient)
            : PropertyRecordsPage(
                opa: opa,
                address: address,
                initialSection: initialSection,
                client: recordsClient,
              ),
      ),
    ),
  );
}
