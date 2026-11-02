import 'package:flutter/material.dart';
import 'package:url_launcher/url_launcher.dart';
import '../src/api_client.dart';
import '../widgets/premium_ui.dart';

class CrimeTrendsPage extends StatefulWidget {
  const CrimeTrendsPage({
    super.key,
    required this.opa,
    required this.address,
    this.client,
  });
  final String opa;
  final String address;
  final ApiClient? client;
  @override
  State<CrimeTrendsPage> createState() => _CrimeTrendsPageState();
}

class _CrimeTrendsPageState extends State<CrimeTrendsPage> {
  late final ApiClient _client;
  late Future<Map<String, dynamic>> _future;
  int _radius = 402;
  @override
  void initState() {
    super.initState();
    _client = widget.client ?? ApiClient();
    _load();
  }

  void _load() {
    _future = _fetch();
  }

  Future<Map<String, dynamic>> _fetch() async {
    try {
      return await _client.get('/api/crime', {
        'opa': widget.opa,
        'radius_m': '$_radius',
      });
    } on ApiException {
      return {'ok': false};
    }
  }

  @override
  void dispose() {
    if (widget.client == null) _client.close();
    super.dispose();
  }

  String _period(dynamic p) => '${p['from']} – ${p['through']}';
  @override
  Widget build(BuildContext context) => BrandedScaffold(
    appBar: AppBar(title: const Text('Reported crime trends')),
    body: ListView(
      padding: const EdgeInsets.all(18),
      children: [
        Text(
          widget.address,
          style: const TextStyle(fontWeight: FontWeight.w600),
        ),
        const SizedBox(height: 12),
        Wrap(
          spacing: 8,
          children: [
            for (final option in [
              (402, '¼ mile'),
              (805, '½ mile'),
              (1609, '1 mile'),
            ])
              ChoiceChip(
                label: Text(option.$2),
                selected: _radius == option.$1,
                onSelected: (_) => setState(() {
                  _radius = option.$1;
                  _load();
                }),
              ),
          ],
        ),
        const SizedBox(height: 12),
        FutureBuilder<Map<String, dynamic>>(
          future: _future,
          builder: (context, snapshot) {
            if (snapshot.connectionState != ConnectionState.done)
              return const Padding(
                padding: EdgeInsets.all(40),
                child: Center(child: CircularProgressIndicator()),
              );
            final d = snapshot.data;
            if (snapshot.hasError || d == null || d['ok'] != true)
              return Column(
                children: [
                  const Text('Crime records unavailable. No counts are shown.'),
                  TextButton(
                    onPressed: () => setState(_load),
                    child: const Text('Retry'),
                  ),
                ],
              );
            final change = d['change_percent'];
            return Column(
              crossAxisAlignment: CrossAxisAlignment.stretch,
              children: [
                if (d['stale'] == true)
                  const Card(
                    child: Padding(
                      padding: EdgeInsets.all(12),
                      child: Text(
                        'Source is more than 14 days behind. Historical counts shown; trend percentage unavailable.',
                      ),
                    ),
                  ),
                FactBox(
                  label: 'Nearby reports · 90 days',
                  value: '${d['current_count']}',
                ),
                const SizedBox(height: 8),
                FactBox(
                  label: 'Previous 90 days',
                  value: '${d['previous_count']}',
                ),
                const SizedBox(height: 8),
                FactBox(
                  label: 'Change in published reports',
                  value: change == null
                      ? 'Not calculated'
                      : '${change > 0 ? '+' : ''}$change%',
                ),
                if (change == null && d['stale'] != true)
                  const Text('A percentage needs a nonzero previous count.'),
                if (d['current_count'] == 0)
                  const Padding(
                    padding: EdgeInsets.only(top: 12),
                    child: Text(
                      'No matching published reports in this period. This does not mean no crime occurred.',
                    ),
                  ),
                Card(
                  child: ExpansionTile(
                    title: const Text('Offense categories'),
                    children: [
                      for (final r in d['categories'])
                        ListTile(
                          title: Text('${r['category']}'),
                          trailing: Text('${r['count']}'),
                        ),
                    ],
                  ),
                ),
                Card(
                  child: ExpansionTile(
                    title: const Text('Monthly history'),
                    subtitle: const Text('12 complete months'),
                    children: [
                      for (final r in d['months'])
                        ListTile(
                          title: Text('${r['month']}'),
                          trailing: Text('${r['count']}'),
                        ),
                    ],
                  ),
                ),
                Card(
                  child: ExpansionTile(
                    title: const Text('Source, dates & limitations'),
                    children: [
                      Padding(
                        padding: const EdgeInsets.all(16),
                        child: Column(
                          crossAxisAlignment: CrossAxisAlignment.start,
                          children: [
                            Text(
                              'Current: ${_period(d['current_period'])}\nPrevious: ${_period(d['previous_period'])}\nLatest citywide incident date: ${d['latest_source_date']}\nRetrieved: ${d['retrieved_at']}',
                            ),
                            const SizedBox(height: 12),
                            Text('${d['note']}'),
                            TextButton(
                              onPressed: () async {
                                final opened = await launchUrl(
                                  Uri.parse(
                                    'https://opendataphilly.org/datasets/crime-incidents/',
                                  ),
                                  mode: LaunchMode.externalApplication,
                                );
                                if (!opened && context.mounted)
                                  ScaffoldMessenger.of(context).showSnackBar(
                                    const SnackBar(
                                      content: Text('Could not open source.'),
                                    ),
                                  );
                              },
                              child: const Text('Open police dataset'),
                            ),
                          ],
                        ),
                      ),
                    ],
                  ),
                ),
                const SizedBox(height: 8),
                const Text(
                  'Nearby published reports • Not a property history or safety score',
                  style: TextStyle(
                    fontSize: 12,
                    color: PremiumColors.secondary,
                  ),
                ),
              ],
            );
          },
        ),
      ],
    ),
  );
}
