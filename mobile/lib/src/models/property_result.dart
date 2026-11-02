class PropertyResult {
  final String opa;
  final String address;
  final String? owner;
  final int? marketValue;
  final int? salePrice;
  final String? saleDate;
  final String? zoning;

  PropertyResult({
    required this.opa,
    required this.address,
    this.owner,
    this.marketValue,
    this.salePrice,
    this.saleDate,
    this.zoning,
  });

  static int? _toInt(dynamic v) {
    if (v == null) return null;
    if (v is int) return v;
    if (v is num) return v.round();
    final s = v.toString().replaceAll(',', '').trim();
    if (s.isEmpty) return null;
    return int.tryParse(s);
  }

  factory PropertyResult.fromJson(Map<String, dynamic> json) {
    return PropertyResult(
      opa: (json['opa'] ?? '').toString().trim(),
      address: (json['address'] ?? '').toString().trim(),
      owner: (json['owner'] == null) ? null : json['owner'].toString().trim(),
      marketValue: _toInt(json['market_value']),
      salePrice: _toInt(json['sale_price']),
      saleDate: (json['sale_date'] == null)
          ? null
          : json['sale_date'].toString().trim(),
      zoning: (json['zoning'] == null)
          ? null
          : json['zoning'].toString().trim(),
    );
  }
}
