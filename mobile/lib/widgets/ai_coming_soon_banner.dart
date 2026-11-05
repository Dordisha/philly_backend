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
                    'AI insights • Coming soon',
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
              'Property insights and smarter questions for your agent.',
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
                          '• Resale insights from comparable sales\n• Clear explanations of property records\n• Questions to ask your real-estate agent\n• Language support, planned for later',
                          style: TextStyle(height: 1.6),
                        ),
                        const SizedBox(height: 16),
                        const Text(
                          'Not included in current Pro. Launch date to be announced.',
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
              child: const Text('Learn more'),
            ),
          ],
        ),
      ),
    );
  }
}
