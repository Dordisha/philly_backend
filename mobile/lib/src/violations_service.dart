import 'api_client.dart';

class ViolationSummary {
  final String opa;
  final int activeCount;
  final int historicalCount;
  final DateTime? lastActivity;
  final String riskLevel;
  final bool hasCourt;
  final bool hasStopWork;
  final bool hasUnsafeStructure;
  final bool hasHazardous;

  ViolationSummary({
    required this.opa,
    required this.activeCount,
    required this.historicalCount,
    required this.lastActivity,
    required this.riskLevel,
    required this.hasCourt,
    required this.hasStopWork,
    required this.hasUnsafeStructure,
    required this.hasHazardous,
  });

  factory ViolationSummary.fromJson(Map<String, dynamic> json) {
    final flags = (json['flags'] as Map?)?.cast<String, dynamic>() ?? {};
    final last = json['last_activity'];

    return ViolationSummary(
      opa: (json['opa'] ?? '').toString(),
      activeCount: (json['active_count'] ?? 0) is int
          ? json['active_count']
          : int.tryParse('${json['active_count']}') ?? 0,
      historicalCount: (json['historical_count'] ?? 0) is int
          ? json['historical_count']
          : int.tryParse('${json['historical_count']}') ?? 0,
      lastActivity: last == null ? null : DateTime.tryParse(last.toString()),
      riskLevel: (json['risk_level'] ?? 'Low').toString(),
      hasCourt: flags['has_court'] == true,
      hasStopWork: flags['has_stop_work'] == true,
      hasUnsafeStructure: flags['has_unsafe_structure'] == true,
      hasHazardous: flags['has_hazardous'] == true,
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
    if (data['ok'] != true ||
        data['active_count'] is! num ||
        data['historical_count'] is! num ||
        data['flags'] is! Map) {
      throw ApiException(
        message: 'Enforcement records are unavailable. Please retry.',
      );
    }
    return ViolationSummary.fromJson(data);
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
