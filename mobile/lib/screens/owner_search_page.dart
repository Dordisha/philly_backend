import '../services/pro_subscription.dart';
import '../widgets/pro_paywall.dart';
import 'package:flutter/material.dart';
import 'package:flutter/foundation.dart';
import '../config/api_config.dart';
import '../src/api_client.dart';
import '../config/pro_preview_config.dart';

class OwnerSearchPage extends StatefulWidget {
  const OwnerSearchPage({
    super.key,
    required this.owner,
    this.opa,
    this.client,
  });
  final String owner;
  final String? opa;
  final ApiClient? client;
  @override
  State<OwnerSearchPage> createState() => _OwnerSearchPageState();
}

class _OwnerSearchPageState extends State<OwnerSearchPage> {
  late final ApiClient client = widget.client ?? ApiClient();
  final rows = <Map<String, dynamic>>[];
  bool loading = false, denied = false;
  String? error, cursor, note;
  // Direct public-data access is restricted to debug on the fixed test host.
  bool get preview => kDebugMode && ProPreviewConfig.enabled &&
      ApiConfig.environment == 'test' && ApiConfig.baseUrl == ApiConfig.testBaseUrl;
  int accessRevision = 0;
  bool get hasAccess => preview || ProSubscription.instance.active;
  late String selectedOwner = widget.owner;
  List<String> previewOwners = [];
  String quote(String value) => "'${value.replaceAll("'", "''")}'";
  Future<Map<String, dynamic>> previewOwnerSearch() async {
    if (!preview) throw StateError('Pro subscription required');
    final publicClient = ApiClient(baseUrl: 'https://phl.carto.com');
    try {
      if (previewOwners.isEmpty && widget.opa != null) {
        if (!RegExp(r'^\d{9}$').hasMatch(widget.opa!))
          throw const FormatException();
        final property = await publicClient.get('/api/v2/sql', {
          'q':
              'SELECT owner_1, owner_2 FROM opa_properties_public WHERE parcel_number = ${quote(widget.opa!)}',
        });
        final matches = property['rows'] as List;
        if (matches.length != 1) throw const FormatException();
        previewOwners = [matches.first['owner_1'], matches.first['owner_2']]
            .whereType<String>()
            .map((n) => n.trim())
            .where((n) => n.isNotEmpty)
            .toSet()
            .toList();
        if (previewOwners.isEmpty) throw const FormatException();
        if (!previewOwners.contains(selectedOwner))
          selectedOwner = previewOwners.first;
      }
      final name = selectedOwner
          .trim()
          .replaceAll(RegExp(r'\s+'), ' ')
          .toUpperCase();
      if (name.length < 3 ||
          name.length > 160 ||
          RegExp(r'[\x00-\x1f%_]').hasMatch(name))
        throw const FormatException();
      final names = <String>{name};
      final parts = name.split(' ');
      if (parts.length == 2 &&
          parts.every((p) => RegExp(r"^[A-Z'-]+$").hasMatch(p)) &&
          !RegExp(
            r'\b(LLC|INC|CORP|TRUST|LP|LLP|CO|COMPANY|ASSOCIATES|PARTNERS|HOLDINGS)\b',
          ).hasMatch(name))
        names.add(parts.reversed.join(' '));
      if (cursor != null && !RegExp(r'^\d{9}$').hasMatch(cursor!))
        throw const FormatException();
      final literals = names.map(quote).join(',');
      final data = await publicClient.get('/api/v2/sql', {
        'q':
            "SELECT DISTINCT ON (parcel_number) parcel_number, location, owner_1, owner_2 FROM opa_properties_public WHERE (UPPER(REGEXP_REPLACE(TRIM(owner_1), '\\s+', ' ', 'g')) IN ($literals) OR UPPER(REGEXP_REPLACE(TRIM(owner_2), '\\s+', ' ', 'g')) IN ($literals)) AND parcel_number ~ '^[0-9]{9}\$' ${cursor == null ? '' : 'AND parcel_number > ${quote(cursor!)}'} ORDER BY parcel_number, location, owner_1, owner_2 LIMIT 26",
      });
      final found = data['rows'] as List;
      String previous = cursor ?? '';
      for (final row in found) {
        final opa = row['parcel_number'].toString();
        final matches = [row['owner_1'], row['owner_2']]
            .whereType<String>()
            .any(
              (n) => names.contains(
                n.trim().replaceAll(RegExp(r'\s+'), ' ').toUpperCase(),
              ),
            );
        if (!RegExp(r'^\d{9}$').hasMatch(opa) ||
            opa.compareTo(previous) <= 0 ||
            !matches ||
            row['location'] == null)
          throw const FormatException();
        previous = opa;
      }
      final page = found.take(25).toList();
      return {
        'rows': page,
        'next_cursor': found.length > 25
            ? page.last['parcel_number'].toString()
            : null,
        'note':
            'Live Philadelphia OPA records. Same-name matches may be different people.',
      };
    } finally {
      publicClient.close();
    }
  }

  @override
  void initState() {
    super.initState();
    ProSubscription.instance.addListener(accessChanged);
    if (!hasAccess) {
      denied = true;
      error = 'Pro subscription required.';
    } else {
      load();
    }
  }

  void accessChanged() {
    if (!mounted || preview || ProSubscription.instance.active) return;
    accessRevision++;
    setState(() {
      rows.clear();
      cursor = null;
      note = null;
      loading = false;
      denied = true;
      error = 'Pro subscription required.';
    });
  }

  @override
  void dispose() {
    accessRevision++;
    ProSubscription.instance.removeListener(accessChanged);
    if (widget.client == null) client.close();
    super.dispose();
  }

  Future<void> load() async {
    final revision = ++accessRevision;
    setState(() {
      loading = true;
      error = null;
      denied = false;
    });
    try {
      if (!hasAccess) {
        throw ApiException(message: 'Pro subscription required.', statusCode: 403);
      }
      // A local StoreKit state only permits attempting the request. The server
      // must authorize every page. No client tier, shared code or CARTO fallback.
      // Authenticated session/purchase exchange remains a release dependency.
      final data = preview
          ? await previewOwnerSearch()
          : await client.get('/api/owners', {
              'name': selectedOwner,
              if (cursor != null) 'after': cursor!,
            });
      if (!preview && data['ok'] != true) throw const FormatException();
      final incoming = (data['rows'] as List)
          .map((r) => Map<String, dynamic>.from(r as Map))
          .toList();
      if (!mounted || revision != accessRevision) return;
      setState(() {
        final ids = rows.map((r) => r['parcel_number'].toString()).toSet();
        rows.addAll(
          incoming.where((r) => ids.add(r['parcel_number'].toString())),
        );
        cursor = data['next_cursor'] as String?;
        note = data['note'] as String?;
      });
    } catch (e) {
      if (!mounted || revision != accessRevision) return;
      setState(() {
        denied =
            e is ApiException && (e.statusCode == 401 || e.statusCode == 403);
        error = denied
            ? 'Pro subscription required.'
            : 'Owner records unavailable. Please retry.';
        if (denied) {
          rows.clear();
          cursor = null;
          note = null;
        }
      });
    } finally {
      if (mounted && revision == accessRevision) {
        setState(() => loading = false);
      }
    }
  }

  @override
  Widget build(BuildContext context) => Scaffold(
    appBar: AppBar(title: const Text('Search by owner')),
    body: ListView(
      padding: const EdgeInsets.all(20),
      children: [
        Text(selectedOwner, style: Theme.of(context).textTheme.titleLarge),
        if (preview) const Text('TEST PREVIEW • Full access'),
        if (previewOwners.length > 1)
          DropdownButton<String>(
            isExpanded: true,
            value: selectedOwner,
            items: [
              for (final name in previewOwners)
                DropdownMenuItem(
                  value: name,
                  child: Text(name, overflow: TextOverflow.ellipsis),
                ),
            ],
            onChanged: loading
                ? null
                : (name) {
                    if (name == null) return;
                    setState(() {
                      selectedOwner = name;
                      rows.clear();
                      cursor = null;
                    });
                    load();
                  },
          ),
        const SizedBox(height: 12),
        const Text(
          'Philadelphia recorded owner and co-owner matches. People with the same name may be different individuals.',
        ),
        if (error != null) ...[
          const SizedBox(height: 16),
          Text(error!),
          if (denied) ...[
            FilledButton(
              onPressed: () async {
                if (await requirePro(context) && mounted) await load();
              },
              child: const Text('Upgrade to Pro'),
            ),
          ] else
            TextButton(
              onPressed: loading ? null : load,
              child: const Text('Retry'),
            ),
        ],
        for (final row in rows)
          Card(
            child: ListTile(
              title: Text(row['location'].toString()),
              subtitle: Text(
                '${row['owner_1'] ?? ''}\n${row['owner_2'] ?? ''}\nOPA: ${row['parcel_number']}',
              ),
              isThreeLine: true,
              trailing: const Icon(Icons.chevron_right),
              onTap: () =>
                  Navigator.pop(context, row['parcel_number'].toString()),
            ),
          ),
        if (!loading && error == null && rows.isEmpty)
          const Text('No matching properties found.'),
        if (note != null) Text(note!),
        if (loading) const Center(child: CircularProgressIndicator()),
        if (!loading && error == null && cursor != null)
          TextButton(onPressed: load, child: const Text('Load more')),
      ],
    ),
  );
}
