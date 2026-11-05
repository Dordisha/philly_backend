import 'package:flutter/material.dart';
import 'package:url_launcher/url_launcher.dart';
import '../services/pro_subscription.dart';
import '../config/pro_preview_config.dart';

const proPrivacyUrl = String.fromEnvironment('PRIVACY_POLICY_URL');
bool get proLegalConfigured {
  final uri = Uri.tryParse(proPrivacyUrl);
  return uri != null &&
      uri.scheme == 'https' &&
      uri.host.isNotEmpty &&
      uri.userInfo.isEmpty;
}

Future<bool> requirePro(BuildContext context) async {
  if (ProPreviewConfig.enabled) return true;
  await ProSubscription.instance.refresh();
  if (!context.mounted) return false;
  if (ProSubscription.instance.active) return true;
  await Navigator.of(
    context,
  ).push<void>(MaterialPageRoute(builder: (_) => const ProPaywall()));
  return ProSubscription.instance.active;
}

class ProPaywall extends StatefulWidget {
  const ProPaywall({super.key});
  @override
  State<ProPaywall> createState() => _ProPaywallState();
}

class _ProPaywallState extends State<ProPaywall> {
  final store = ProSubscription.instance;
  @override
  void initState() {
    super.initState();
    store.start();
    store.loadProduct();
  }

  Future<void> link(String url) async {
    if (!await launchUrl(
          Uri.parse(url),
          mode: LaunchMode.externalApplication,
        ) &&
        mounted) {
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(content: Text('Could not open link. Please try again.')),
      );
    }
  }

  @override
  Widget build(BuildContext context) => AnimatedBuilder(
    animation: store,
    builder: (context, _) => Scaffold(
      appBar: AppBar(title: const Text('Philly Scan Pro')),
      body: ListView(
        padding: const EdgeInsets.all(24),
        children: [
          Text(
            store.active ? 'Pro is active' : 'Get more from every property',
            style: Theme.of(context).textTheme.headlineSmall,
          ),
          const SizedBox(height: 16),
          const Text(
            'Pro includes detailed 311 complaints, L&I records, Zillow property links, and search by recorded owner. Public records may be incomplete or delayed.',
          ),
          const SizedBox(height: 16),
          if (store.price != null)
            Text(
              '${store.price} / month',
              style: Theme.of(context).textTheme.headlineSmall,
            ),
          const Text(
            'Monthly auto-renewing subscription. Payment is charged to your Apple Account. Renews automatically unless cancelled at least 24 hours before the end of the current period. Manage or cancel in your Apple subscription settings.',
          ),
          const SizedBox(height: 12),
          const Text(
            'AI features are coming later and are not included in this subscription.',
          ),
          if (!proLegalConfigured)
            const Text(
              'Subscriptions are unavailable until the app’s privacy policy is configured.',
            ),
          if (store.message != null)
            Padding(
              padding: const EdgeInsets.symmetric(vertical: 12),
              child: Text(store.message!),
            ),
          if (store.busy) const Center(child: CircularProgressIndicator()),
          if (!store.active)
            FilledButton(
              onPressed:
                  store.busy || store.price == null || !proLegalConfigured
                  ? null
                  : () => store.transact(),
              child: Text(
                store.price == null
                    ? 'Subscription unavailable'
                    : 'Subscribe — ${store.price} / month',
              ),
            ),
          if (store.active)
            FilledButton(
              onPressed: () => Navigator.pop(context),
              child: const Text('Continue with Pro'),
            ),
          TextButton(
            onPressed: store.busy ? null : store.redeem,
            child: const Text('Redeem promo code'),
          ),
          TextButton(
            onPressed: store.busy ? null : () => store.transact(restore: true),
            child: const Text('Restore Purchases'),
          ),
          TextButton(
            onPressed: () =>
                link('https://apps.apple.com/account/subscriptions'),
            child: const Text('Manage Subscription'),
          ),
          if (proLegalConfigured)
            TextButton(
              onPressed: () => link(proPrivacyUrl),
              child: const Text('Privacy Policy'),
            ),
          TextButton(
            onPressed: () => link(
              'https://www.apple.com/legal/internet-services/itunes/dev/stdeula/',
            ),
            child: const Text('Terms of Use'),
          ),
          TextButton(
            onPressed: store.busy ? null : store.loadProduct,
            child: const Text('Refresh App Store'),
          ),
        ],
      ),
    ),
  );
}

/// Rechecks access when Apple's transaction listener or expiry timer changes it.
class ProAccessGate extends StatelessWidget {
  const ProAccessGate({super.key, required this.child});
  final Widget child;
  @override
  Widget build(BuildContext context) => AnimatedBuilder(
    animation: ProSubscription.instance,
    builder: (_, __) =>
        ProPreviewConfig.enabled || ProSubscription.instance.active
        ? child
        : const ProPaywall(),
  );
}
