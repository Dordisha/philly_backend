import 'package:flutter/material.dart';
import 'package:url_launcher/url_launcher.dart';
import '../src/api_client.dart';

const recordSections = ['311', 'violations', 'sales', 'facts'];
const recordTitles = ['311 records', 'L&I records', 'Resale value', 'Signals'];

String recordValue(dynamic value) => value == null || value.toString().isEmpty
    ? 'Not recorded'
    : value.toString();
String recordMoney(num value) => '\$${value.round().toString().replaceAllMapped(RegExp(r'\B(?=(\d{3})+(?!\d))'), (_) => ',')}';

const _recordLabels = {
  'service_request_id': '311 request ID', 'service_name': 'Request type',
  'service_code': 'Service code', 'status': 'Status',
  'requested_datetime': 'Reported', 'updated_datetime': 'Updated',
  'closed_datetime': 'Closed', 'agency_responsible': 'Responsible agency',
  'status_notes': 'Status notes', 'casenumber': 'Case number',
  'casestatus': 'Case status', 'caseprioritydesc': 'Priority', 'casetype': 'Case type',
  'casecreateddate': 'Case opened', 'casecompleteddate': 'Case completed',
  'violationnumber': 'Violation number', 'violationstatus': 'Violation status',
  'violationdate': 'Violation date', 'violationresolutiondate': 'Resolution date',
  'violationresolutioncode': 'Resolution code', 'violationcode': 'Violation code',
  'violationcodetitle': 'Violation title', 'parcel_number': 'OPA parcel ID',
  'location': 'Address', 'sale_price': 'Recorded sale price', 'sale_date': 'Sale date',
  'total_livable_area': 'Living area (sq ft)', 'total_area': 'Lot area (sq ft)',
  'number_of_bedrooms': 'Bedrooms', 'number_of_bathrooms': 'Bathrooms',
  'building_code_description_new': 'OPA building type', 'building_code_new': 'OPA building code',
  'category_code': 'OPA category code', 'interior_condition': 'Interior condition (OPA code)',
  'exterior_condition': 'Exterior condition (OPA code)', 'assessment_date': 'OPA record assessment date',
  'distance_m': 'Distance from property', 'owner_1': 'Recorded owner',
  'owner_2': 'Additional recorded owner', 'mailing_street': 'Owner mailing street',
  'mailing_city_state': 'Owner mailing city/state', 'mailing_zip': 'Owner mailing ZIP',
  'zip_code': 'ZIP', 'zipcode': 'ZIP', 'address': 'Reported address', 'unit': 'Unit',
};
String _fieldValue(String key, dynamic value) {
  if (value == null) return 'Not recorded';
  final n = num.tryParse('$value');
  if (key == 'sale_price' && n != null) return recordMoney(n);
  if (key == 'distance_m' && n != null) return '${n.round()} m / ${(n / 1609.344).toStringAsFixed(2)} mi';
  if (key.contains('date') || key.endsWith('datetime')) {
    final date = DateTime.tryParse('$value');
    if (date != null) return date.toIso8601String().substring(0, 10);
  }
  return recordValue(value);
}

/// Descriptive sale benchmark, adjusted only by the owner's explicit assumption.
/// No condition classification or learned seller predictions are performed.
Map<String, double>? saleBenchmark(List<Map<String, dynamic>> rows, num? area) {
  if (area == null || !area.isFinite || area <= 0) return null;
  final prices = <double>[];
  for (final row in rows) {
    final price = num.tryParse('${row['sale_price']}');
    final size = num.tryParse('${row['total_livable_area']}');
    if (price != null && price.isFinite && price >= 50000 &&
        size != null && size.isFinite && size > 0) {
      prices.add((price / size).toDouble());
    }
  }
  if (prices.length < 3) return null;
  prices.sort();
  double quantile(double p) {
    final index = (prices.length - 1) * p;
    final lower = index.floor();
    final upper = index.ceil();
    return prices[lower] + (prices[upper] - prices[lower]) * (index - lower);
  }
  return {'low': quantile(.25) * area, 'mid': quantile(.5) * area,
    'high': quantile(.75) * area, 'count': prices.length.toDouble()};
}

class PropertyRecordsPage extends StatefulWidget {
  final String opa;
  final String address;
  final int initialSection;
  final ApiClient? client;
  const PropertyRecordsPage({super.key, required this.opa, required this.address,
    this.initialSection = 0, this.client});
  @override
  State<PropertyRecordsPage> createState() => _PropertyRecordsPageState();
}

class _PropertyRecordsPageState extends State<PropertyRecordsPage> {
  late final ApiClient _client;
  late int _section;
  int _offset = 0;
  double _adjustment = 0;
  String _condition = 'As-is';
  late Future<Map<String, dynamic>> _future;
  @override
  void initState() {
    super.initState();
    _client = widget.client ?? ApiClient();
    _section = widget.initialSection;
    _future = _load();
  }
  Future<Map<String, dynamic>> _load() async {
    final data = await _client.get('/api/records/${recordSections[_section]}', {
      'opa': widget.opa, 'limit': '25', 'offset': '$_offset',
    });
    if (data['ok'] != true || data['opa'] != widget.opa || data['rows'] is! List ||
        (data['rows'] as List).any((row) => row is! Map<String, dynamic>)) {
      throw ApiException(message: 'Records could not be read. Please retry.');
    }
    return data;
  }
  void _reload({int? section, int? offset}) => setState(() {
    _section = section ?? _section;
    _offset = offset ?? _offset;
    _future = _load();
  });
  @override
  void dispose() {
    if (widget.client == null) _client.close();
    super.dispose();
  }
  Future<void> _open(String url) async {
    final uri = Uri.tryParse(url);
    if (uri != null && uri.scheme == 'https') {
      await launchUrl(uri, mode: LaunchMode.externalApplication);
    }
  }
  Widget _estimate(Map<String, dynamic> data, List<Map<String, dynamic>> rows) {
    final area = num.tryParse('${data['subject_area']}');
    final estimate = saleBenchmark(rows, area);
    return Card(child: Padding(padding: const EdgeInsets.all(16), child: Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        const Text('Resale estimate • sales + your assumptions',
          style: TextStyle(fontSize: 18, fontWeight: FontWeight.bold)),
        const SizedBox(height: 10),
        if (estimate == null)
          const Text('Insufficient matching sales or floor-area data to calculate a range.')
        else ...[
          Text('${recordMoney(estimate['low']! * (1 + _adjustment / 100))} – '
            '${recordMoney(estimate['high']! * (1 + _adjustment / 100))}',
            style: const TextStyle(fontSize: 24, fontWeight: FontWeight.bold)),
          Text('Midpoint: ${recordMoney(estimate['mid']! * (1 + _adjustment / 100))}'),
          Text('${estimate['count']!.toInt()} sales on this page • Subject: $area sq ft'),
          DropdownButton<String>(
            value: _condition,
            items: ['As-is', 'Partly renovated', 'Fully renovated'].map((v) =>
              DropdownMenuItem(value: v, child: Text(v))).toList(),
            onChanged: (v) => setState(() => _condition = v!),
          ),
          Text('Your assumed price adjustment: ${_adjustment.round()}%'),
          Slider(value: _adjustment, min: -30, max: 50, divisions: 80,
            label: '${_adjustment.round()}%',
            onChanged: (v) => setState(() => _adjustment = v)),
          const Text('Condition is your scenario; selecting it does not automatically add value. '
            'The adjustment is your assumption, not a verified renovation premium.'),
          const SizedBox(height: 8),
          const Text('Method: 25th–75th percentile of recorded sale price per sq ft × '
            'subject floor area × your adjustment. Uses only sales on the displayed page '
            'with price at least \$50,000 and valid floor area. Assumes those sales are '
            'representative market transactions; transfer type and condition are unverified. '
            'This experimental range is not an appraisal, a statistical confidence interval '
            'or profit; renovation and selling costs are not deducted.'),
        ],
      ],
    )));
  }
  @override
  Widget build(BuildContext context) => Scaffold(
    appBar: AppBar(title: const Text('Property records')),
    body: Column(children: [
      Padding(padding: const EdgeInsets.symmetric(horizontal: 16, vertical: 8),
        child: Text('${widget.address}\nOPA ${widget.opa}')),
      SingleChildScrollView(scrollDirection: Axis.horizontal, child: Row(
        children: List.generate(recordTitles.length, (i) => Padding(
          padding: const EdgeInsets.symmetric(horizontal: 4),
          child: ChoiceChip(label: Text(recordTitles[i]), selected: _section == i,
            onSelected: (_) => _reload(section: i, offset: 0)),
        )),
      )),
      Expanded(child: FutureBuilder<Map<String, dynamic>>(
        future: _future,
        builder: (context, snap) {
          if (snap.connectionState != ConnectionState.done) {
            return const Center(child: CircularProgressIndicator());
          }
          if (snap.hasError) {
            return Center(child: Column(mainAxisSize: MainAxisSize.min, children: [
              const Text('Records unavailable. Please retry.'),
              TextButton(onPressed: () => _reload(), child: const Text('Retry records')),
            ]));
          }
          final data = snap.data!;
          final rows = (data['rows'] as List).cast<Map<String, dynamic>>();
          final source = (data['source'] as Map?) ?? {};
          return ListView(padding: const EdgeInsets.all(16), children: [
            Text(recordValue(source['label'])),
            Text('Retrieved: ${recordValue(data['retrieved_at'])}'),
            if (source['url'] is String) TextButton(
              onPressed: () => _open(source['url']), child: const Text('Official data source')),
            Text(recordValue(data['note'])),
            const SizedBox(height: 12),
            if (_section == 2) _estimate(data, rows),
            if (_section == 3 && data['signals'] is Map) ...[
              Card(child: Padding(padding: const EdgeInsets.all(16), child: Column(
                crossAxisAlignment: CrossAxisAlignment.start, children: [
                  Text('Selling signals: ${data['signals']['label']}',
                    style: const TextStyle(fontSize: 18, fontWeight: FontWeight.bold)),
                  Text(recordValue(data['signals']['listing_status'])),
                  const SizedBox(height: 8),
                  Text(recordValue(data['signals']['note'])),
                  for (final factor in (data['signals']['factors'] as List? ?? [])) ...[
                    const Divider(),
                    Text('${factor['title']} • ${factor['strength']}',
                      style: const TextStyle(fontWeight: FontWeight.bold)),
                    Text(recordValue(factor['evidence'])),
                    Text(recordValue(factor['explanation'])),
                  ],
                ],
              ))),
              const Text('Underlying published property records'),
            ],
            if (rows.isEmpty) const Padding(padding: EdgeInsets.all(16),
              child: Text('No published records matched these filters.')),
            ...rows.map((row) => Card(child: Padding(padding: const EdgeInsets.all(14),
              child: Column(crossAxisAlignment: CrossAxisAlignment.start, children: [
                Text(recordValue(row['service_name'] ?? row['violationcodetitle'] ?? row['location']),
                  style: const TextStyle(fontSize: 17, fontWeight: FontWeight.bold)),
                const SizedBox(height: 8),
                ...row.entries.map((entry) => Padding(
                  padding: const EdgeInsets.symmetric(vertical: 3),
                  child: SelectableText('${_recordLabels[entry.key] ?? entry.key.replaceAll('_', ' ')}: ${_fieldValue(entry.key, entry.value)}'),
                )),
                if (row['parcel_number'] != null) TextButton(
                  onPressed: () => _open('https://property.phila.gov/?p=${Uri.encodeComponent('${row['parcel_number']}')}'),
                  child: const Text('Open official property record')),
              ]),
            ))),
            Row(mainAxisAlignment: MainAxisAlignment.spaceBetween, children: [
              TextButton(onPressed: _offset > 0 ? () => _reload(offset: _offset - 25) : null,
                child: const Text('Previous')),
              Text('Page ${_offset ~/ 25 + 1}'),
              TextButton(onPressed: data['has_more'] == true && _offset < 10000
                ? () => _reload(offset: _offset + 25) : null, child: const Text('Next')),
            ]),
          ]);
        },
      )),
    ]),
  );
}
