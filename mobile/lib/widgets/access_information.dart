import 'package:flutter/material.dart';
import 'package:url_launcher/url_launcher.dart';

/// Information only: this release has no StoreKit purchase or ad SDK.
class AccessInformationPage extends StatelessWidget {
  const AccessInformationPage({super.key});

  Future<void> _open(BuildContext context, String url) async {
    try {
      if (await launchUrl(
        Uri.parse(url),
        mode: LaunchMode.externalApplication,
      )) {
        return;
      }
    } catch (_) {
      // Keep the information visible if a source cannot be opened.
    }
    if (context.mounted) {
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(content: Text('Could not open link. Please try again.')),
      );
    }
  }

  @override
  Widget build(BuildContext context) {
    const privacy = String.fromEnvironment('PRIVACY_POLICY_URL');
    final privacyUri = Uri.tryParse(privacy);
    return Scaffold(
      appBar: AppBar(title: const Text('Free access, AI & sources')),
      body: ListView(
        padding: const EdgeInsets.all(24),
        children: [
          Text(
            'Public data, made easier to use',
            style: Theme.of(context).textTheme.headlineSmall,
          ),
          const SizedBox(height: 16),
          const Text(
            'Property facts, 311 details, L&I records, owner search, and Zillow links are free. No subscription is required.',
          ),
          const SizedBox(height: 16),
          const Text(
            'Philadelphia’s public records are also available free through their original City sources. Philly Scan brings them together with convenient searches and organized results.',
          ),
          const SizedBox(height: 16),
          const Text(
            'Philly Scan is independently operated and is not affiliated with or endorsed by the City of Philadelphia. Records may be incomplete or delayed. Verify information with the original source.',
          ),
          const SizedBox(height: 24),
          Text(
            'AI homebuyer assistant • Coming soon',
            style: Theme.of(context).textTheme.titleLarge,
          ),
          const Text(
            r'Under construction. Planned price: $12.99/month for market research, homebuying cost estimates, step-by-step guidance, and multilingual explanations. These capabilities are not available in this version. Not available to purchase yet. No payment is collected. Public records will remain free.',
          ),
          const SizedBox(height: 24),
          Text(
            'View free City sources',
            style: Theme.of(context).textTheme.titleLarge,
          ),
          for (final source in const [
            (
              'Property records (OPA)',
              'https://opendataphilly.org/datasets/philadelphia-properties-and-assessment-history/',
            ),
            (
              '311 service requests',
              'https://opendataphilly.org/datasets/311-service-and-information-requests/',
            ),
            (
              'L&I violations',
              'https://opendataphilly.org/datasets/licenses-and-inspections-code-violations/',
            ),
          ])
            TextButton(
              onPressed: () => _open(context, source.$2),
              child: Text(source.$1),
            ),
          if (privacyUri?.scheme == 'https' && privacyUri!.host.isNotEmpty)
            TextButton(
              onPressed: () => _open(context, privacy),
              child: const Text('Privacy Policy'),
            ),
          TextButton(
            onPressed: () => _open(
              context,
              'https://www.apple.com/legal/internet-services/itunes/dev/stdeula/',
            ),
            child: const Text('Terms of Use'),
          ),
          TextButton(
            onPressed: () =>
                _open(context, 'https://apps.apple.com/account/subscriptions'),
            child: const Text('Manage any existing Apple subscriptions'),
          ),
        ],
      ),
    );
  }
}
