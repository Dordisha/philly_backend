import 'package:flutter/material.dart';
import '../screens/property_records_page.dart';
import '../screens/pro_overview_page.dart';
import '../src/api_client.dart';

Future<void> showProComingSoonDialog(
  BuildContext context, {
  String? opa,
  String address = '',
  int initialSection = -1,
  ApiClient? recordsClient,
  String title = 'Property records',
  String message = '',
}) async {
  if (opa == null || opa.isEmpty) {
    ScaffoldMessenger.of(context).showSnackBar(
      const SnackBar(
        content: Text('Scan a property first, then open its records.'),
      ),
    );
    return;
  }
  await Navigator.of(context).push<void>(
    MaterialPageRoute(
      builder: (_) => initialSection < 0
          ? ProOverviewPage(opa: opa, address: address, client: recordsClient)
          : PropertyRecordsPage(
              opa: opa,
              address: address,
              initialSection: initialSection,
              client: recordsClient,
            ),
    ),
  );
}
