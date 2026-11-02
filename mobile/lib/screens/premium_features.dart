import 'crime_trends_page.dart';
import 'package:flutter/material.dart';
import '../config/pro_preview_config.dart';
import '../src/api_client.dart';
import '../widgets/premium_ui.dart';
import '../widgets/pro_coming_soon.dart';
import '../widgets/property_status_card.dart';

class FeatureMenu extends StatelessWidget {
  final String opa;
  final String address;
  final ApiClient? client;
  const FeatureMenu({
    super.key,
    required this.opa,
    required this.address,
    this.client,
  });

  void _open(BuildContext context, String title, {int? section}) {
    if (!ProPreviewConfig.enabled || opa.isEmpty) {
      showProComingSoonDialog(
        context,
        opa: opa,
        address: address,
        title: title,
        message: section != null && section < 2 || title == 'Zillow'
            ? 'Open detailed property records with Pro.'
            : 'This feature belongs to AI Premium, including Pro.',
        initialSection: section ?? -1,
        recordsClient: client,
      );
      return;
    }
    if (section != null) {
      showProComingSoonDialog(
        context,
        opa: opa,
        address: address,
        initialSection: section,
        recordsClient: client,
      );
    } else {
      Navigator.of(context).push(
        MaterialPageRoute<void>(
          builder: (_) => title == 'Zillow'
              ? BrandedScaffold(
                  appBar: AppBar(title: const Text('Check status')),
                  body: Padding(
                    padding: const EdgeInsets.all(20),
                    child: PropertyStatusCard(address: address),
                  ),
                )
              : title == 'Crime trends'
              ? CrimeTrendsPage(opa: opa, address: address, client: client)
              : PremiumFeaturePage(title: title, address: address),
        ),
      );
    }
  }

  Widget _heading(String label, String price) => Padding(
    padding: const EdgeInsets.only(top: 16, bottom: 8),
    child: Wrap(
      alignment: WrapAlignment.spaceBetween,
      spacing: 20,
      runSpacing: 5,
      children: [
        Text(
          label,
          style: const TextStyle(fontSize: 19, fontWeight: FontWeight.w700),
        ),
        Text(price, style: const TextStyle(color: PremiumColors.secondary)),
      ],
    ),
  );
  Widget _grid(BuildContext context, List<(String, IconData, int?)> items) =>
      LayoutBuilder(
        builder: (_, constraints) => Wrap(
          spacing: 10,
          runSpacing: 0,
          children: items
              .map(
                (item) => SizedBox(
                  width: constraints.maxWidth < 280
                      ? constraints.maxWidth
                      : (constraints.maxWidth - 10) / 2,
                  child: FeatureTile(
                    title: item.$1,
                    icon: item.$2,
                    onTap: () => _open(context, item.$1, section: item.$3),
                  ),
                ),
              )
              .toList(),
        ),
      );
  @override
  Widget build(BuildContext context) => Column(
    crossAxisAlignment: CrossAxisAlignment.stretch,
    children: [
      _heading('Pro', '\$2.99 / month'),
      _grid(context, [
        ('311', Icons.notifications_none, 0),
        ('L&I', Icons.account_balance_outlined, 1),
        ('Zillow', Icons.home_outlined, null),
      ]),
      _heading('AI Premium', '\$9.99 / month'),
      _grid(context, [
        ('Resale', Icons.trending_up, 2),
        ('Signals', Icons.bar_chart_outlined, 3),
        ('Crime trends', Icons.shield_outlined, null),
      ]),
      const SizedBox(height: 10),
      FilledButton.icon(
        onPressed: () => _open(context, 'Ask AI'),
        icon: const Icon(Icons.chat_bubble_outline),
        label: const Padding(
          padding: EdgeInsets.all(10),
          child: Text('Ask AI'),
        ),
      ),
      const SizedBox(height: 8),
      const Text(
        'AI Premium includes Pro',
        style: TextStyle(fontSize: 12, color: PremiumColors.secondary),
      ),
    ],
  );
}

class PremiumFeaturePage extends StatelessWidget {
  final String title;
  final String address;
  const PremiumFeaturePage({
    super.key,
    required this.title,
    required this.address,
  });
  @override
  Widget build(BuildContext context) {
    final questions = title == 'Ask AI';
    final items = questions
        ? [
            'What should I investigate first?',
            'What do these violations mean?',
            'Which sales support the estimate?',
            'What information is missing?',
          ]
        : title == 'Schools'
        ? [
            'School assignment',
            'Performance measures',
            'School boundaries',
            'Sources & dates',
          ]
        : [
            'Reported incidents',
            'Compare periods',
            'Incident map',
            'Sources & coverage',
          ];
    return BrandedScaffold(
      appBar: AppBar(title: Text(title)),
      body: ListView(
        padding: const EdgeInsets.all(20),
        children: [
          Text(address, style: const TextStyle(fontWeight: FontWeight.w600)),
          const SizedBox(height: 18),
          FactBox(
            label: 'Preview · Not connected',
            value: questions
                ? 'AI answers are not available yet.'
                : title == 'Schools'
                ? 'School data is not connected yet.'
                : 'Crime data is not connected yet.',
          ),
          const SizedBox(height: 12),
          for (final item in items)
            Card(
              child: ListTile(
                title: Text(item),
                trailing: const Icon(Icons.chevron_right),
                onTap: () => Navigator.of(context).push(
                  MaterialPageRoute<void>(
                    builder: (_) => BrandedScaffold(
                      appBar: AppBar(title: Text(item)),
                      body: ListView(
                        padding: const EdgeInsets.all(20),
                        children: [
                          Text(address),
                          const SizedBox(height: 20),
                          FactBox(
                            label: questions
                                ? 'Your question'
                                : 'Data availability',
                            value: questions ? item : 'Not connected',
                          ),
                          const SizedBox(height: 14),
                          Text(
                            questions
                                ? 'Once connected, answers will use this property’s records and link to the supporting evidence. No answer has been generated.'
                                : title == 'Schools'
                                ? 'This view needs verified school assignment and dated performance data. No school rating has been calculated.'
                                : 'This view needs dated incident records, a defined area and a comparison period. No crime rate or safety rating has been calculated.',
                          ),
                        ],
                      ),
                    ),
                  ),
                ),
              ),
            ),
        ],
      ),
    );
  }
}
