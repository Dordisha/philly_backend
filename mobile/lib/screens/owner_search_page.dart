import 'package:flutter/material.dart';
import '../src/api_client.dart';

/// Credentials remain in memory and are never embedded in builds or URLs.
class OwnerAccessSession { static String? token; }

class OwnerSearchPage extends StatefulWidget {
  const OwnerSearchPage({super.key, required this.owner, this.client});
  final String owner;
  final ApiClient? client;
  @override
  State<OwnerSearchPage> createState() => _OwnerSearchPageState();
}

class _OwnerSearchPageState extends State<OwnerSearchPage> {
  late final ApiClient client = widget.client ?? ApiClient();
  final rows = <Map<String, dynamic>>[];
  bool loading = false, denied = false;
  String? error, cursor, note;
  @override
  void initState() {
    super.initState();
    if (OwnerAccessSession.token == null) { denied = true; error = 'Pro subscription required.'; } else { load(); }
  }
  @override
  void dispose() { if (widget.client == null) client.close(); super.dispose(); }
  Future<void> load() async {
    setState(() { loading = true; error = null; denied = false; });
    try {
      final data = await client.get('/api/owners', {'name': widget.owner, if (cursor != null) 'after': cursor!}, accessToken: OwnerAccessSession.token);
      final incoming = (data['rows'] as List).map((r) => Map<String, dynamic>.from(r as Map)).toList();
      if (!mounted) return;
      setState(() {
        final ids = rows.map((r) => r['parcel_number'].toString()).toSet();
        rows.addAll(incoming.where((r) => ids.add(r['parcel_number'].toString())));
        cursor = data['next_cursor'] as String?;
        note = data['note'] as String?;
      });
    } catch (e) {
      if (!mounted) return;
      setState(() {
        denied = e is ApiException && (e.statusCode == 401 || e.statusCode == 403);
        error = denied ? 'Pro subscription required.' : 'Owner records unavailable. Please retry.';
        if (denied) { rows.clear(); cursor = null; OwnerAccessSession.token = null; }
      });
    } finally { if (mounted) setState(() => loading = false); }
  }
  Future<void> enterAccess() async {
    final controller = TextEditingController();
    final token = await showDialog<String>(context: context, builder: (context) => AlertDialog(
      title: const Text('Activate granted access'),
      content: TextField(controller: controller, obscureText: true, autocorrect: false, enableSuggestions: false, decoration: const InputDecoration(labelText: 'Private access code')),
      actions: [TextButton(onPressed: () => Navigator.pop(context), child: const Text('Cancel')), TextButton(onPressed: () => Navigator.pop(context, controller.text.trim()), child: const Text('Verify'))],
    ));
    // Dispose after the dialog's exit animation releases its TextField.
    await Future<void>.delayed(const Duration(milliseconds: 300));
    controller.dispose();
    if (!mounted || token == null || token.isEmpty) return;
    OwnerAccessSession.token = token;
    await load();
  }
  @override
  Widget build(BuildContext context) => Scaffold(
    appBar: AppBar(title: const Text('Search by owner')),
    body: ListView(padding: const EdgeInsets.all(20), children: [
      Text(widget.owner, style: Theme.of(context).textTheme.titleLarge),
      const SizedBox(height: 12),
      const Text('Philadelphia recorded owner and co-owner matches. People with the same name may be different individuals.'),
      if (error != null) ...[
        const SizedBox(height: 16), Text(error!),
        if (denied) ...[
          FilledButton(onPressed: () => showDialog<void>(context: context, builder: (context) => AlertDialog(
            title: const Text('Upgrade to Pro'), content: const Text('Pro is $2.99/month. Purchases are not available in this preview. Existing granted access can be verified with your private access code.'),
            actions: [TextButton(onPressed: () => Navigator.pop(context), child: const Text('OK'))],
          )), child: const Text('Upgrade')),
          TextButton(onPressed: enterAccess, child: const Text('I have granted access')),
        ] else TextButton(onPressed: loading ? null : load, child: const Text('Retry')),
      ],
      for (final row in rows) Card(child: ListTile(
        title: Text(row['location'].toString()),
        subtitle: Text('${row['owner_1'] ?? ''}\n${row['owner_2'] ?? ''}\nOPA: ${row['parcel_number']}'),
        isThreeLine: true, trailing: const Icon(Icons.chevron_right),
        onTap: () => Navigator.pop(context, row['parcel_number'].toString()),
      )),
      if (!loading && error == null && rows.isEmpty) const Text('No matching properties found.'),
      if (note != null) Text(note!),
      if (loading) const Center(child: CircularProgressIndicator()),
      if (!loading && error == null && cursor != null) TextButton(onPressed: load, child: const Text('Load more')),
    ]),
  );
}
