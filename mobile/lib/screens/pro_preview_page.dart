import 'package:flutter/material.dart';

/// Deliberately has no property ID, transport, entitlement or purchase service.
class ProPreviewPage extends StatelessWidget {
  const ProPreviewPage({super.key});

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(title: const Text('Pro preview')),
      body: ListView(
        padding: const EdgeInsets.all(16),
        children: const [
          Card(
            child: Padding(
              padding: EdgeInsets.all(16),
              child: Text(
                'SAMPLE DATA • No payment\n'
                'Design preview only. These records and prices are fictional '
                'and are not about your scanned property.',
              ),
            ),
          ),
          ExpansionTile(
            title: Text('311 complaint details'),
            subtitle: Text('Sample complaint • not a confirmed violation'),
            children: [
              ListTile(
                title: Text('Sample: illegal dumping'),
                subtitle: Text(
                  'Report DEMO-311-001 • Reported October 1, 2026\n'
                  'Status: Open • Location: Sample parcel\n'
                  'Public description: Debris reported near property.\n'
                  'Live version: official record link and checked date.\n'
                  'No live 311 records have been retrieved in this preview.',
                ),
              ),
            ],
          ),
          ExpansionTile(
            title: Text('Open violation details'),
            subtitle: Text('Sample case, dates and resolution history'),
            children: [
              ListTile(
                title: Text('Sample: exterior maintenance'),
                subtitle: Text(
                  'Case DEMO-LI-001 • Violation DEMO-V-001\n'
                  'Case status: Open • Violation status: Unresolved\n'
                  'Issued October 2, 2026 • Priority: Sample normal\n'
                  'Resolution: None recorded in this fictional example.\n'
                  'Live version: official record link and checked date.',
                ),
              ),
            ],
          ),
          ExpansionTile(
            title: Text('Renovation resale analysis'),
            subtitle: Text('Illustrative range and comparable-sales layout'),
            children: [
              ListTile(
                title: Text('Sample value scenarios • fictional'),
                subtitle: Text(
                  'Fictional layout example, not a calculated valuation.\n'
                  'Unrenovated: \$200,000–\$230,000\n'
                  'Partly renovated: \$250,000–\$280,000\n'
                  'Fully renovated: \$300,000–\$330,000\n'
                  'Live version: separate nearby sold comparables, sample count '
                  'and condition evidence for each group.\n'
                  'Assumption: kitchen, bathroom and cosmetic upgrades; '
                  'no structural work. Condition is unverified.\n'
                  'Evidence quality: Not assessed • Live analysis unavailable.',
                ),
              ),
              ListTile(
                title: Text('Fictional comparable sales'),
                subtitle: Text(
                  'Sample A • Fully renovated (fictional assumption)\n'
                  'Sold September 2026 • \$310,000\n'
                  '0.2 mi • Rowhome • 1,200 sq ft\n'
                  'Sample B • Partly renovated (fictional assumption)\n'
                  'Sold August 2026 • \$265,000\n'
                  '0.3 mi • Rowhome • 1,250 sq ft\n'
                  'These are not actual transactions. Live analysis must show '
                  'verified sources, matching criteria and exclusions.',
                ),
              ),
              ListTile(
                title: Text('Costs and assumptions'),
                subtitle: Text(
                  'Renovation budget, selling costs and carrying costs will be '
                  'shown separately. Resale value is not profit.\n'
                  'When comparable sales or condition evidence are insufficient, '
                  'the live feature must say insufficient data.',
                ),
              ),
            ],
          ),
        ],
      ),
    );
  }
}
