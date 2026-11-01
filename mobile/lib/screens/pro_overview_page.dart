import 'package:flutter/material.dart';
import '../src/api_client.dart';
import 'property_records_page.dart';

/// One visible entry for each requested Pro feature; sections fail independently.
class ProOverviewPage extends StatefulWidget {
  final String opa;
  final String address;
  final ApiClient? client;
  const ProOverviewPage({super.key, required this.opa, required this.address, this.client});
  @override
  State<ProOverviewPage> createState() => _ProOverviewPageState();
}
class _ProOverviewPageState extends State<ProOverviewPage> {
  late final ApiClient _client;
  late List<Future<Map<String, dynamic>>> _sections;
  @override
  void initState() {
    super.initState();
    _client = widget.client ?? ApiClient();
    _sections = List.generate(4, _load);
  }
  Future<Map<String, dynamic>> _load(int i) async {
    final data = await _client.get('/api/records/${recordSections[i]}', {
      'opa': widget.opa, 'limit': '25', 'offset': '0',
    });
    if (data['ok'] != true || data['opa'] != widget.opa || data['rows'] is! List ||
        (data['rows'] as List).any((r) => r is! Map<String, dynamic>)) {
      throw ApiException(message: 'Records could not be read.');
    }
    return data;
  }
  @override
  void dispose() {
    if (widget.client == null) _client.close();
    super.dispose();
  }
  Future<void> _details(int i) => Navigator.of(context).push<void>(MaterialPageRoute(
    builder: (_) => PropertyRecordsPage(opa: widget.opa, address: widget.address,
      initialSection: i, client: _client),
  ));
  Widget _card(int i) => Card(child: Padding(padding: const EdgeInsets.all(16),
    child: Column(crossAxisAlignment: CrossAxisAlignment.start, children: [
      Text(recordTitles[i], style: const TextStyle(fontSize: 20, fontWeight: FontWeight.bold)),
      const SizedBox(height: 8),
      FutureBuilder<Map<String, dynamic>>(future: _sections[i], builder: (context, snap) {
        if (snap.connectionState != ConnectionState.done) {
          return const Padding(padding: EdgeInsets.all(8), child: LinearProgressIndicator());
        }
        if (snap.hasError) {
          return Column(crossAxisAlignment: CrossAxisAlignment.start, children: [
            Text(i == 2 ? 'A resale estimate is unavailable for this property.' : 'This data is currently unavailable.'),
            TextButton(onPressed: () => setState(() => _sections[i] = _load(i)),
              child: const Text('Retry')),
          ]);
        }
        final data = snap.data!;
        final rows = (data['rows'] as List).cast<Map<String, dynamic>>();
        final count = '${rows.length}${data['has_more'] == true ? '+' : ''}';
        final estimate = i == 2 ? saleBenchmark(rows, num.tryParse('${data['subject_area']}')) : null;
        return Column(crossAxisAlignment: CrossAxisAlignment.start, children: [
          if (i < 2) ...[
            Text('$count ${i == 0 ? 'public requests' : 'violation records'}',
              style: const TextStyle(fontSize: 24, fontWeight: FontWeight.bold)),
            if (rows.isNotEmpty) Text(recordValue(rows.first[i == 0 ? 'service_name' : 'violationcodetitle'])),
            if (rows.isEmpty) const Text('No published records matched. This does not establish a complete history.'),
          ],
          if (i == 2) ...[
            if (estimate != null) Text('${recordMoney(estimate['low']!)} – ${recordMoney(estimate['high']!)}',
              style: const TextStyle(fontSize: 24, fontWeight: FontWeight.bold))
            else const Text('Not enough matching sales for a range.'),
            Text('$count nearby recorded sales'),
            const Text('Experimental baseline at 0% adjustment. Open to inspect sales and change your assumptions.'),
          ],
          if (i == 3) ...[
            Text(recordValue((data['signals'] as Map?)?['label']),
              style: const TextStyle(fontSize: 22, fontWeight: FontWeight.bold)),
            Text(recordValue((data['signals'] as Map?)?['note'])),
          ],
          const SizedBox(height: 8),
          FilledButton.tonal(onPressed: () => _details(i), child: Text(
            ['View 311 records', 'View L&I records', 'Open resale analysis', 'View signal evidence'][i])),
        ]);
      }),
    ]),
  ));
  @override
  Widget build(BuildContext context) => Scaffold(
    appBar: AppBar(title: const Text('Philly Scan Pro • Live data')),
    body: ListView(padding: const EdgeInsets.all(16), children: [
      Text(widget.address, style: const TextStyle(fontSize: 20, fontWeight: FontWeight.bold)),
      Text('OPA ${widget.opa} • Free test access'),
      const SizedBox(height: 12),
      const Card(child: ListTile(
        leading: Icon(Icons.storefront_outlined),
        title: Text('Market status'),
        subtitle: Text('Unknown — listing data is not connected. No active-listing check has been performed.'),
      )),
      for (var i = 0; i < 4; i++) _card(i),
    ]),
  );
}
