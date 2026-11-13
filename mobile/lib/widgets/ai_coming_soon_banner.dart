import 'package:flutter/material.dart';
import 'premium_ui.dart';

class AiComingSoonBanner extends StatefulWidget {
  const AiComingSoonBanner({super.key});
  @override
  State<AiComingSoonBanner> createState() => _AiComingSoonBannerState();
}

class _AiComingSoonBannerState extends State<AiComingSoonBanner> {
  bool dismissed = false;
  @override
  Widget build(BuildContext context) {
    if (dismissed) return const SizedBox.shrink();
    return Card(
      color: const Color(0xFFF4F1E5),
      child: Padding(
        padding: const EdgeInsets.fromLTRB(14, 6, 6, 10),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Row(
              children: [
                const Icon(
                  Icons.auto_awesome,
                  color: PremiumColors.green,
                  size: 20,
                ),
                const SizedBox(width: 8),
                const Expanded(
                  child: Text(
                    'AI homebuyer assistant • Coming soon',
                    style: TextStyle(fontWeight: FontWeight.bold),
                  ),
                ),
                IconButton(
                  tooltip: 'Dismiss announcement',
                  onPressed: () => setState(() => dismissed = true),
                  icon: const Icon(Icons.close, size: 18),
                ),
              ],
            ),
            const Text(
              'Under construction. Understand the home, the costs, and your next steps—in your language.',
            ),
            TextButton(
              onPressed: () => showModalBottomSheet<void>(
                context: context,
                isScrollControlled: true,
                builder: (context) => SafeArea(
                  child: SingleChildScrollView(
                    padding: const EdgeInsets.all(24),
                    child: Column(
                      mainAxisSize: MainAxisSize.min,
                      crossAxisAlignment: CrossAxisAlignment.start,
                      children: [
                        Text(
                          'What’s planned',
                          style: Theme.of(context).textTheme.titleLarge,
                        ),
                        const SizedBox(height: 12),
                        const Text(
                          'Planned capabilities—not available in this version:\n\n• Research on similar nearby home sales\n• Estimates of buying and ownership costs\n• Step-by-step guidance for first-time buyers\n• Questions and checklists for agents and lenders\n• Explanations in Albanian, Spanish, Vietnamese, Chinese, and English',
                          style: TextStyle(height: 1.6),
                        ),
                        const SizedBox(height: 16),
                        const Text(
                          r'Planned AI subscription: $12.99/month. Not available to purchase yet. No payment is collected. Public records remain free.',
                        ),
                        TextButton(
                          onPressed: () => Navigator.pop(context),
                          child: const Text('Got it'),
                        ),
                      ],
                    ),
                  ),
                ),
              ),
              child: const Text('See what’s planned'),
            ),
          ],
        ),
      ),
    );
  }
}
