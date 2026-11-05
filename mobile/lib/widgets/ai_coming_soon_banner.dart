import 'package:flutter/material.dart';
import 'premium_ui.dart';

/// Roadmap announcement only. No purchase or availability promise.
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
        padding: const EdgeInsets.all(14),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Row(
              children: [
                const Icon(Icons.auto_awesome, color: PremiumColors.green),
                const SizedBox(width: 10),
                const Expanded(
                  child: Text(
                    'AI-powered insights are coming',
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
              'We’re building tools to help you understand a property and prepare for conversations with your real-estate agent.',
            ),
            const ExpansionTile(
              tilePadding: EdgeInsets.zero,
              title: Text('See what’s planned'),
              children: [
                ListTile(
                  leading: Icon(Icons.home_work_outlined),
                  title: Text('Resale insights'),
                  subtitle: Text(
                    'Explore comparable sales and clearly explained assumptions.',
                  ),
                ),
                ListTile(
                  leading: Icon(Icons.manage_search),
                  title: Text('Understand the records'),
                  subtitle: Text(
                    'Plain-language explanations and property signals linked to their sources.',
                  ),
                ),
                ListTile(
                  leading: Icon(Icons.question_answer_outlined),
                  title: Text('Prepare questions for your agent'),
                  subtitle: Text(
                    'Suggested questions based on available property records—for example, which violations need clarification or what to ask during an inspection.',
                  ),
                ),
                ListTile(
                  leading: Icon(Icons.translate),
                  title: Text('Language support, planned for later'),
                  subtitle: Text(
                    'Bilingual explanations and a shareable question list for buyers and their agents.',
                  ),
                ),
              ],
            ),
            const Text(
              'Coming later • Not included in the current Pro subscription. Availability and launch date will be announced after testing.',
              style: TextStyle(fontSize: 12, color: PremiumColors.secondary),
            ),
          ],
        ),
      ),
    );
  }
}
