import 'dart:convert';
import 'package:flutter/material.dart';
import 'package:http/http.dart' as http;
import 'package:url_launcher/url_launcher.dart';
import '../config/api_config.dart';
import 'update_notice.dart';

bool policyRequiresUpdate(Map<String, dynamic> data, String installed) =>
    data['schema'] == 1 &&
    data['bundleId'] == 'com.dorian.phillyscan' &&
    data['required'] == true &&
    data['minimumVersion'] is String &&
    data['releasedVersion'] is String &&
    RegExp(r'^\d{1,4}(\.\d{1,4}){0,2}$').hasMatch(data['minimumVersion']) &&
    RegExp(r'^\d{1,4}(\.\d{1,4}){0,2}$').hasMatch(data['releasedVersion']) &&
    isNewerVersion(data['minimumVersion'], installed) &&
    !isNewerVersion(data['minimumVersion'], data['releasedVersion']);

class RequiredUpdateGate extends StatefulWidget {
  final Widget child;
  final Future<bool> Function()? check;
  const RequiredUpdateGate({super.key, required this.child, this.check});
  @override
  State<RequiredUpdateGate> createState() => _RequiredUpdateGateState();
}

class _RequiredUpdateGateState extends State<RequiredUpdateGate>
    with WidgetsBindingObserver {
  final _client = http.Client();
  bool _blocked = false, _checking = false;
  String? _error;
  @override
  void initState() {
    super.initState();
    WidgetsBinding.instance.addObserver(this);
    _check();
  }

  @override
  void dispose() {
    WidgetsBinding.instance.removeObserver(this);
    _client.close();
    super.dispose();
  }

  @override
  void didChangeAppLifecycleState(AppLifecycleState state) {
    if (state == AppLifecycleState.resumed) _check();
  }

  Future<bool> _fetch() async {
    final r = await _client
        .get(Uri.parse('${ApiConfig.baseUrl}/api/app-policy'))
        .timeout(const Duration(seconds: 8));
    if (r.statusCode != 200) throw const FormatException();
    final data = jsonDecode(r.body);
    if (data is! Map<String, dynamic> ||
        data['schema'] != 1 ||
        data['bundleId'] != 'com.dorian.phillyscan' ||
        data['required'] is! bool)
      throw const FormatException();
    if (!policyRequiresUpdate(data, currentMarketingVersion)) return false;
    // Require independent proof that the minimum version is available to download.
    final listed = await AppUpdateService(_client).newerVersion();
    return listed != null && !isNewerVersion(data['minimumVersion'], listed);
  }

  Future<void> _check() async {
    if (_checking) return;
    setState(() {
      _checking = true;
      _error = null;
    });
    try {
      final blocked = await (widget.check?.call() ?? _fetch());
      if (mounted) setState(() => _blocked = blocked);
    } catch (_) {
      if (mounted)
        setState(
          () => _error = 'Unable to check. Please retry when connected.',
        );
      // Initial outage fails open; an already verified requirement remains visible.
    } finally {
      if (mounted) setState(() => _checking = false);
    }
  }

  Future<void> _open() async {
    try {
      if (await launchUrl(
        Uri.parse(appStoreUrl),
        mode: LaunchMode.externalApplication,
      ))
        return;
    } catch (_) {}
    if (mounted)
      setState(() => _error = 'Could not open the App Store. Please retry.');
  }

  @override
  Widget build(BuildContext context) => Stack(
    children: [
      Offstage(offstage: _blocked, child: widget.child),
      if (_blocked)
        Positioned.fill(
          child: PopScope(
            canPop: false,
            child: Scaffold(
              body: SafeArea(
                child: Center(
                  child: SingleChildScrollView(
                    padding: const EdgeInsets.all(28),
                    child: Column(
                      mainAxisSize: MainAxisSize.min,
                      children: [
                        const Icon(Icons.system_update, size: 48),
                        const SizedBox(height: 20),
                        const Text(
                          'Update to continue',
                          style: TextStyle(
                            fontSize: 26,
                            fontWeight: FontWeight.bold,
                          ),
                        ),
                        const SizedBox(height: 12),
                        const Text(
                          'Please install the latest Philly Scan from the App Store.',
                          textAlign: TextAlign.center,
                        ),
                        const SizedBox(height: 20),
                        FilledButton(
                          onPressed: _open,
                          child: const Text('Update Philly Scan'),
                        ),
                        TextButton(
                          onPressed: _checking ? null : _check,
                          child: Text(_checking ? 'Checking…' : 'Check again'),
                        ),
                        if (_error != null)
                          Text(_error!, textAlign: TextAlign.center),
                      ],
                    ),
                  ),
                ),
              ),
            ),
          ),
        ),
    ],
  );
}
