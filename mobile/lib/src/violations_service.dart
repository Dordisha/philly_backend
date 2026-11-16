import 'api_client.dart';

class ViolationStatusCount {
  final String? caseStatus;
  final String? violationStatus;
  final int count;

  const ViolationStatusCount({
    required this.caseStatus,
    required this.violationStatus,
    required this.count,
  });
}

class ViolationSummary {
  final String opa;
  final int totalCount;
  final List<ViolationStatusCount> statuses;
  final DateTime? lastActivity;

  ViolationSummary({
    required this.opa,
    required this.totalCount,
    required this.statuses,
    required this.lastActivity,
  });

  factory ViolationSummary.fromJson(Map<String, dynamic> json) {
    Never invalid() => throw ApiException(
      message: 'Enforcement records are unavailable. Please retry.',
    );
    final total = json['total_count'];
    final rows = json['status_counts'];
    final last = json['last_activity'];
    if (json['ok'] != true || json['summary_version'] != 2 ||
        json['status_mapping'] != 'unverified' ||
        json['opa'] is! String ||
        !RegExp(r'^\d{9}$').hasMatch(json['opa'] as String) ||
        total is! int || total < 0 || total > 9007199254740991 || rows is! List ||
        !json.containsKey('last_activity') ||
        (last != null && (last is! String || DateTime.tryParse(last) == null))) {
      invalid();
    }
    var sum = 0;
    final statuses = <ViolationStatusCount>[];
    final groups = <(String?, String?)>{};
    for (final row in rows) {
      if (row is! Map || !row.containsKey('casestatus') ||
          !row.containsKey('violationstatus') ||
          (row['casestatus'] != null && row['casestatus'] is! String) ||
          (row['violationstatus'] != null && row['violationstatus'] is! String) ||
          row['count'] is! int || (row['count'] as int) < 1) {
        invalid();
      }
      final caseStatus = row['casestatus'] as String?;
      final violationStatus = row['violationstatus'] as String?;
      if (!groups.add((caseStatus, violationStatus))) invalid();
      sum += row['count'] as int;
      if (sum > 9007199254740991) invalid();
      statuses.add(ViolationStatusCount(
        caseStatus: caseStatus,
        violationStatus: violationStatus,
        count: row['count'] as int,
      ));
    }
    if (sum != total || (total == 0 && last != null)) invalid();
    return ViolationSummary(
      opa: json['opa'] as String,
      totalCount: total,
      statuses: List.unmodifiable(statuses),
      lastActivity: last == null ? null : DateTime.parse(last as String),
    );
  }
}

class ViolationRow {
  final String? casenumber;
  final String? casestatus;
  final String? caseprioritydesc;
  final String? casetype;
  final DateTime? casecreateddate;
  final DateTime? casecompleteddate;

  final String? violationnumber;
  final String? violationstatus;
  final DateTime? violationdate;
  final DateTime? violationresolutiondate;
  final String? violationresolutioncode;
  final String? violationcode;
  final String? violationcodetitle;

  ViolationRow({
    required this.casenumber,
    required this.casestatus,
    required this.caseprioritydesc,
    required this.casetype,
    required this.casecreateddate,
    required this.casecompleteddate,
    required this.violationnumber,
    required this.violationstatus,
    required this.violationdate,
    required this.violationresolutiondate,
    required this.violationresolutioncode,
    required this.violationcode,
    required this.violationcodetitle,
  });

  factory ViolationRow.fromJson(Map<String, dynamic> j) => ViolationRow(
    casenumber: j['casenumber']?.toString(),
    casestatus: j['casestatus']?.toString(),
    caseprioritydesc: j['caseprioritydesc']?.toString(),
    casetype: j['casetype']?.toString(),
    casecreateddate: j['casecreateddate'] == null
        ? null
        : DateTime.tryParse(j['casecreateddate'].toString()),
    casecompleteddate: j['casecompleteddate'] == null
        ? null
        : DateTime.tryParse(j['casecompleteddate'].toString()),
    violationnumber: j['violationnumber']?.toString(),
    violationstatus: j['violationstatus']?.toString(),
    violationdate: j['violationdate'] == null
        ? null
        : DateTime.tryParse(j['violationdate'].toString()),
    violationresolutiondate: j['violationresolutiondate'] == null
        ? null
        : DateTime.tryParse(j['violationresolutiondate'].toString()),
    violationresolutioncode: j['violationresolutioncode']?.toString(),
    violationcode: j['violationcode']?.toString(),
    violationcodetitle: j['violationcodetitle']?.toString(),
  );
}

class ViolationsService {
  final ApiClient _client;
  ViolationsService({ApiClient? client}) : _client = client ?? ApiClient();

  Future<ViolationSummary> fetchSummary({required String opa}) async {
    final data = await _client.get('/api/violations/summary', {'opa': opa});
    final summary = ViolationSummary.fromJson(data);
    if (summary.opa != opa) {
      throw ApiException(message: 'Enforcement property identity is unavailable.');
    }
    return summary;
  }

  Future<List<ViolationRow>> fetchDetails({
    required String opa,
    int limit = 200,
  }) async {
    final data = await _client.get('/api/violations/details', {
      'opa': opa,
      'limit': '$limit',
    });
    final rows = data['rows'];
    if (data['ok'] != true ||
        rows is! List ||
        rows.any((r) => r is! Map<String, dynamic>)) {
      throw ApiException(
        message: 'Enforcement details are unavailable. Please retry.',
      );
    }
    return rows
        .cast<Map<String, dynamic>>()
        .map(ViolationRow.fromJson)
        .toList();
  }

  void close() => _client.close();
}
