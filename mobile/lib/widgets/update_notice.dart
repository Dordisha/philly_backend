import 'dart:convert';
import 'package:flutter/foundation.dart';
import 'package:flutter/material.dart';
import 'package:http/http.dart' as http;
import 'package:url_launcher/url_launcher.dart';

const appStoreUrl = 'https://apps.apple.com/us/app/philly-scan/id6758272125';
const currentMarketingVersion = '1.1.0';
bool isNewerVersion(String latest, String installed) {
  List<int>? parse(String v) => RegExp(r'^\d+(\.\d+){0,2}$').hasMatch(v)
      ? v.split('.').map(int.parse).toList()
      : null;
  final a = parse(latest), b = parse(installed);
  if (a == null || b == null) return false;
  for (var i = 0; i < 3; i++) {
    final difference = (i < a.length ? a[i] : 0) - (i < b.length ? b[i] : 0);
    if (difference != 0) return difference > 0;
  }
  return false;
}

class AppUpdateService {
  final http.Client client;
  AppUpdateService(this.client);
  Future<String?> newerVersion() async {
    final response = await client
        .get(
          Uri.https('itunes.apple.com', '/lookup', {
            'id': '6758272125',
            'country': 'us',
          }),
        )
        .timeout(const Duration(seconds: 8));
    if (response.statusCode != 200)
      throw const FormatException('Store unavailable');
    final data = jsonDecode(response.body);
    if (data is! Map || data['results'] is! List) throw const FormatException();
    for (final row in data['results']) {
      if (row is Map &&
          row['bundleId'] == 'com.dorian.phillyscan' &&
          row['trackId'] == 6758272125 &&
          row['version'] is String) {
        return isNewerVersion(row['version'], currentMarketingVersion)
            ? row['version']
            : null;
      }
    }
    throw const FormatException('App not found');
  }
}

/// Non-blocking in-app notice, not remote push notification delivery.
class UpdateNotice extends StatefulWidget {
  final bool showControls;
  const UpdateNotice({super.key, this.showControls = false});
  @override
  State<UpdateNotice> createState() => _UpdateNoticeState();
}

class _UpdateNoticeState extends State<UpdateNotice> {
  final _client = http.Client();
  String? _version;
  bool _checking = false;
  bool _demo = false;
  @override
  void initState() {
    super.initState();
    _check();
  }

  @override
  void dispose() {
    _client.close();
    super.dispose();
  }

  Future<void> _check({bool manual = false}) async {
    if (_checking) return;
    _checking = true;
    try {
      final version = await AppUpdateService(_client).newerVersion();
      if (!mounted) return;
      setState(() {
        _version = version;
        _demo = false;
      });
      if (manual && version == null)
        _message('No newer version is listed in the US App Store.');
    } catch (_) {
      if (manual && mounted)
        _message('Unable to check for updates. Try again later.');
    } finally {
      _checking = false;
    }
  }

  void _message(String text) =>
      ScaffoldMessenger.of(context).showSnackBar(SnackBar(content: Text(text)));
  Future<void> _open() async {
    try {
      if (await launchUrl(
        Uri.parse(appStoreUrl),
        mode: LaunchMode.externalApplication,
      ))
        return;
    } catch (_) {}
    if (mounted) _message('Could not open the App Store. Please try again.');
  }

  @override
  Widget build(BuildContext context) => Column(
    children: [
      if (_version != null)
        Card(
          child: Padding(
            padding: const EdgeInsets.all(14),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Text(
                  _demo
                      ? 'Update notice · Simulator preview'
                      : 'A new version is available',
                  style: const TextStyle(fontWeight: FontWeight.bold),
                ),
                Text(
                  _demo
                      ? 'Example only. This does not announce a released update.'
                      : 'Philly Scan $_version is ready.',
                ),
                Wrap(
                  spacing: 12,
                  children: [
                    FilledButton(
                      onPressed: _open,
                      child: const Text('Open App Store'),
                    ),
                    TextButton(
                      onPressed: () => setState(() => _version = null),
                      child: const Text('Later'),
                    ),
                  ],
                ),
              ],
            ),
          ),
        ),
      if (widget.showControls)
        Wrap(
          alignment: WrapAlignment.center,
          children: [
            TextButton(
              onPressed: () => _check(manual: true),
              child: const Text('Check for updates'),
            ),
            if (kDebugMode)
              TextButton(
                onPressed: () => setState(() {
                  _demo = true;
                  _version = 'preview';
                }),
                child: const Text('Preview update notice'),
              ),
          ],
        ),
    ],
  );
}
