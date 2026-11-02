import 'premium_ui.dart';
import 'package:flutter/material.dart';
import '../config/pro_preview_config.dart';
import '../screens/property_records_page.dart';
import '../screens/pro_overview_page.dart';
import '../src/api_client.dart';

Future<void> showProComingSoonDialog(
  BuildContext context, {
  String? opa,
  String address = '',
  int initialSection = -1,
  ApiClient? recordsClient,
  String title = "Enforcement Details",
  String message =
      "You can see the summary for free.\n\nDetailed case history and enforcement records will be available in the Pro version soon.",
}) async {
  if (ProPreviewConfig.enabled && opa != null && opa.isNotEmpty) {
    await Navigator.of(context).push<void>(
      MaterialPageRoute(builder: (_) => initialSection < 0
        ? ProOverviewPage(opa: opa, address: address, client: recordsClient)
        : PropertyRecordsPage(opa: opa, address: address, initialSection: initialSection, client: recordsClient)),
    );
    return;
  }
  if (ProPreviewConfig.enabled) {
    title = 'Scan a property first';
    message = 'Scan an address, then open 311 records, L&I records or Resale value beneath the property facts.';
  }
  return showDialog<void>(
    context: context,
    builder: (_) {
      return AlertDialog(
        backgroundColor: const Color(0xFFFFFDFC),
        shape: RoundedRectangleBorder(borderRadius: BorderRadius.circular(18)),
        titlePadding: const EdgeInsets.fromLTRB(16, 16, 16, 0),
        contentPadding: const EdgeInsets.fromLTRB(16, 12, 16, 0),
        actionsPadding: const EdgeInsets.fromLTRB(8, 6, 8, 10),
        title: Row(
          children: [
            const Icon(Icons.lock_outline, color: PremiumColors.secondary, size: 20),
            const SizedBox(width: 10),
            Expanded(
              child: Text(
                ProPreviewConfig.enabled ? "Property records" : "Coming Soon – Pro Feature",
                style: TextStyle(
                  color: PremiumColors.ink,
                  fontWeight: FontWeight.w800,
                  fontSize: 16,
                ),
              ),
            ),
          ],
        ),
        content: Text(
          "$title\n\n$message${ProPreviewConfig.enabled ? '' : '\n\nPro: \$2.99/month · AI Premium: \$9.99/month. Subscriptions are not available in this preview.'}",
          style: const TextStyle(
            color: PremiumColors.secondary,
            height: 1.25,
            fontSize: 13.5,
          ),
        ),
        actions: [
          TextButton(
            onPressed: () => Navigator.pop(context),
            child: const Text(
              "Got it",
              style: TextStyle(
                color: PremiumColors.ink,
                fontWeight: FontWeight.w700,
              ),
            ),
          ),
        ],
      );
    },
  );
}
