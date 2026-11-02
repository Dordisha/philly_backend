import 'package:flutter/material.dart';
import 'package:url_launcher/url_launcher.dart';

/// An outbound address search, never an inferred listing status or Zillow ID.
/// This app currently resolves Philadelphia properties only.
Uri? zillowAddressSearch(String address) {
  var query = address.trim().replaceAll(RegExp(r'\s+'), ' ');
  if (query.isEmpty) return null;
  if (!RegExp(r'\bPhiladelphia\b', caseSensitive: false).hasMatch(query)) {
    query = '$query, Philadelphia, PA';
  }
  return Uri(
    scheme: 'https',
    host: 'www.zillow.com',
    pathSegments: ['homes', '${query}_rb', ''],
  );
}

class PropertyStatusCard extends StatelessWidget {
  final String address;
  const PropertyStatusCard({super.key, required this.address});

  Future<void> _open(BuildContext context, Uri uri) async {
    try {
      if (await launchUrl(uri, mode: LaunchMode.externalApplication)) return;
    } catch (_) {
      // Keep the property visible and let the user retry.
    }
    if (!context.mounted) return;
    ScaffoldMessenger.of(context).showSnackBar(const SnackBar(
      content: Text('Could not open Zillow. Please try again.'),
    ));
  }

  @override
  Widget build(BuildContext context) {
    final uri = zillowAddressSearch(address);
    return Card(
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            const Row(children: [
              Icon(Icons.storefront_outlined),
              SizedBox(width: 8),
              Text('Check status',
                style: TextStyle(fontSize: 20, fontWeight: FontWeight.bold)),
            ]),
            const SizedBox(height: 8),
            const Text('Check sale or rental status on Zillow.'),
            const SizedBox(height: 4),
            const Text('Opens an address search. Confirm the address and unit on Zillow.'),
            const SizedBox(height: 8),
            FilledButton.tonalIcon(
              onPressed: uri == null ? null : () => _open(context, uri),
              icon: const Icon(Icons.open_in_new),
              label: const Text('Check on Zillow'),
            ),
            if (uri == null) const Text('A property address is needed.'),
          ],
        ),
      ),
    );
  }
}
