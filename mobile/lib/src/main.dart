// lib/src/main.dart
import 'dart:async';
import '../widgets/premium_ui.dart';
import '../widgets/update_notice.dart';
import '../screens/premium_features.dart';

import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:url_launcher/url_launcher.dart';

import 'api_service.dart';
import 'api_client.dart' show ApiClient;
import '../config/pro_preview_config.dart';
import 'models/property_result.dart';

// ✅ Violations
import 'violations_service.dart';
import '../widgets/violation_summary_card.dart';
import '../widgets/pro_coming_soon.dart';

/// ===============================
/// APP DISPLAY AND FEEDBACK
/// ===============================
/// API configuration lives in config/api_config.dart.

/// Source archive version; this preview does not create a store build.
const String kAppVersion = "1.1.0-preview.5 (8)";

/// Support contact
const String kFeedbackEmail = "dorian.dishani@gmail.com";

void main() {
  runApp(const PhillyScanApp());
}

class PhillyScanApp extends StatelessWidget {
  const PhillyScanApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'Philly Scan',
      debugShowCheckedModeBanner: false,
      theme: premiumTheme(),
      home: const HomeScreen(),
    );
  }
}

class HomeScreen extends StatefulWidget {
  const HomeScreen({
    super.key,
    this.apiService,
    this.violationsService,
    this.recordsClient,
  });
  final ApiService? apiService;
  final ViolationsService? violationsService;
  final ApiClient? recordsClient;

  @override
  State<HomeScreen> createState() => _HomeScreenState();
}

class _HomeScreenState extends State<HomeScreen> {
  final _controller = TextEditingController(
    text: "1400 John F Kennedy Blvd, Philadelphia, PA 19107",
  );
  final _focusNode = FocusNode();

  bool _loading = false;
  PropertyResult? _result;

  String? _error;
  List<Map<String, dynamic>> _errorSuggestions = const [];

  List<Map<String, dynamic>> _suggestions = const [];
  bool _showSuggestions = false;
  Timer? _debounce;

  late final ApiService _api;
  late final ViolationsService _violationsService;
  int _suggestionGeneration = 0;
  int _scanGeneration = 0;
  late String _lastInputText;

  static const _titleStyle = TextStyle(
    fontSize: 34,
    fontWeight: FontWeight.w500,
    letterSpacing: 0.25,
    height: 1.05,
  );

  static const _subtitleStyle = TextStyle(
    fontSize: 15,
    fontWeight: FontWeight.w400,
    letterSpacing: 0.35,
    color: PremiumColors.secondary,
  );

  static const _bodyStyle = TextStyle(
    fontSize: 15,
    fontWeight: FontWeight.w400,
    letterSpacing: 0.25,
    color: PremiumColors.secondary,
  );

  static const _valueStyle = TextStyle(
    fontSize: 17,
    fontWeight: FontWeight.w500,
    letterSpacing: 0.25,
    color: PremiumColors.ink,
  );

  @override
  void initState() {
    super.initState();
    _api = widget.apiService ?? ApiService();
    _violationsService = widget.violationsService ?? ViolationsService();
    _lastInputText = _controller.text;
    _controller.addListener(_onInputChanged);
    _focusNode.addListener(() {
      if (!_focusNode.hasFocus) {
        setState(() => _showSuggestions = false);
      } else {
        if (_suggestions.isNotEmpty) setState(() => _showSuggestions = true);
      }
    });
  }

  @override
  void dispose() {
    _debounce?.cancel();
    _suggestionGeneration++;
    _scanGeneration++;
    if (widget.apiService == null) _api.close();
    if (widget.violationsService == null) _violationsService.close();
    _controller.removeListener(_onInputChanged);
    _controller.dispose();
    _focusNode.dispose();
    super.dispose();
  }

  String _normalizePhillyAddress(String input) {
    final q = input.trim();
    if (q.isEmpty) return q;

    final up = q.toUpperCase();
    final hasPhilly = up.contains("PHILADELPHIA");
    final hasPA = RegExp(r'\bPA\b').hasMatch(up);
    final looksLikeStreet = RegExp(r'^\d+\s+').hasMatch(q);

    if (looksLikeStreet && (!hasPhilly || !hasPA)) {
      return "$q PHILADELPHIA PA";
    }
    return q;
  }

  void _onInputChanged() {
    // Cursor/composing changes must not invalidate a request for the same text.
    if (_controller.text == _lastInputText) return;
    _lastInputText = _controller.text;
    final q = _controller.text.trim();
    final generation = ++_suggestionGeneration;
    _scanGeneration++;
    setState(() {
      _loading = false;
      _suggestions = const [];
      _showSuggestions = false;
    });

    if (_result != null || _error != null) {
      setState(() {
        _result = null;
        _error = null;
        _errorSuggestions = const [];
      });
    }

    _debounce?.cancel();
    _debounce = Timer(const Duration(milliseconds: 250), () async {
      if (!mounted || generation != _suggestionGeneration) return;

      if (q.isEmpty) {
        setState(() {
          _suggestions = const [];
          _showSuggestions = false;
        });
        return;
      }

      final looksUseful = q.length >= 2 || RegExp(r'^\d').hasMatch(q);
      if (!looksUseful) {
        setState(() {
          _suggestions = const [];
          _showSuggestions = false;
        });
        return;
      }

      try {
        final list = await _api.suggest(q, limit: 10);
        if (!mounted || generation != _suggestionGeneration) return;

        setState(() {
          _suggestions = list;
          _showSuggestions = _focusNode.hasFocus && list.isNotEmpty;
        });
      } catch (_) {
        if (!mounted || generation != _suggestionGeneration) return;
        setState(() {
          _suggestions = const [];
          _showSuggestions = false;
        });
      }
    });
  }

  Future<void> _scan({String? opaOverride}) async {
    _debounce?.cancel();
    _suggestionGeneration++;
    final generation = ++_scanGeneration;
    var input = _controller.text.trim();
    if (input.isEmpty) return;

    if (opaOverride == null || opaOverride.trim().isEmpty) {
      input = _normalizePhillyAddress(input);
    }

    setState(() {
      _loading = true;
      _result = null;
      _error = null;
      _errorSuggestions = const [];
      _showSuggestions = false;
    });

    try {
      final result = await _api.scan(input, opaOverride: opaOverride);
      if (!mounted || generation != _scanGeneration) return;
      setState(() => _result = result);
    } catch (e) {
      if (!mounted || generation != _scanGeneration) return;

      if (e is ApiException) {
        setState(() {
          _error = e.message.isEmpty ? 'Address not found' : e.message;
          _errorSuggestions = e.suggestions;
        });
      } else {
        final msg = e.toString().replaceFirst('Exception: ', '').trim();
        setState(() {
          _error = msg.isEmpty ? 'Address not found' : msg;
          _errorSuggestions = const [];
        });
      }
    } finally {
      if (!mounted || generation != _scanGeneration) return;
      setState(() => _loading = false);
    }
  }

  Future<void> _openPro() async {
    if (ProPreviewConfig.enabled && _result == null) {
      if (_controller.text.trim().isEmpty) {
        ScaffoldMessenger.of(context).showSnackBar(
          const SnackBar(content: Text('Enter a property address first.')),
        );
        return;
      }
      await _scan();
      if (!mounted || _result == null) return;
    }
    if (!mounted) return;
    await showProComingSoonDialog(
      context,
      opa: _result?.opa,
      address: _result?.address ?? '',
      recordsClient: widget.recordsClient,
    );
  }

  void _applySuggestion(Map<String, dynamic> s) async {
    final address = (s['address'] ?? '').toString().trim();
    final opa = (s['opa'] ?? '').toString().trim();

    if (address.isNotEmpty) {
      _controller.value = TextEditingValue(
        text: address,
        selection: TextSelection.collapsed(offset: address.length),
      );
    }

    await _scan(opaOverride: opa.isEmpty ? null : opa);
  }

  String _money(int? v) {
    if (v == null) return "—";
    return "\$${v.toString().replaceAllMapped(RegExp(r'\B(?=(\d{3})+(?!\d))'), (m) => ',')}";
  }

  Future<void> _copy(String text, String label) async {
    await Clipboard.setData(ClipboardData(text: text));
    if (!mounted) return;
    ScaffoldMessenger.of(context).showSnackBar(SnackBar(content: Text(label)));
  }

  Future<void> _open(String url) async {
    await launchUrl(Uri.parse(url), mode: LaunchMode.externalApplication);
  }

  Future<void> _emailFeedback() async {
    final uri = Uri(
      scheme: 'mailto',
      path: kFeedbackEmail,
      queryParameters: {'subject': 'Philly Scan $kAppVersion'},
    );
    try {
      if (await launchUrl(uri)) return;
    } catch (_) {}
    if (!mounted) return;
    await _copy(kFeedbackEmail, 'Support email copied');
  }

  Widget _suggestionsBox() {
    if (!_showSuggestions || _suggestions.isEmpty)
      return const SizedBox.shrink();

    return Container(
      margin: const EdgeInsets.only(top: 8),
      decoration: BoxDecoration(
        borderRadius: BorderRadius.circular(16),
        color: const Color(0xFFFFFDFC),
        border: Border.all(color: PremiumColors.green.withOpacity(0.35)),
      ),
      child: ListView.separated(
        shrinkWrap: true,
        physics: const NeverScrollableScrollPhysics(),
        itemCount: _suggestions.length,
        separatorBuilder: (_, __) =>
            Divider(height: 1, color: PremiumColors.line),
        itemBuilder: (context, i) {
          final s = _suggestions[i];
          final addr = (s['address'] ?? '').toString().trim();
          final zip = (s['zip'] ?? '').toString().trim();
          return InkWell(
            onTap: _loading ? null : () => _applySuggestion(s),
            child: Padding(
              padding: const EdgeInsets.symmetric(horizontal: 14, vertical: 12),
              child: Row(
                children: [
                  const Icon(
                    Icons.location_on_outlined,
                    size: 18,
                    color: PremiumColors.secondary,
                  ),
                  const SizedBox(width: 10),
                  Expanded(
                    child: Text(
                      zip.isEmpty ? addr : '$addr  •  $zip',
                      maxLines: 1,
                      overflow: TextOverflow.ellipsis,
                      style: const TextStyle(
                        fontSize: 14.5,
                        fontWeight: FontWeight.w600,
                        color: PremiumColors.ink,
                      ),
                    ),
                  ),
                  const SizedBox(width: 10),
                  const Icon(
                    Icons.chevron_right,
                    color: PremiumColors.secondary,
                  ),
                ],
              ),
            ),
          );
        },
      ),
    );
  }

  Widget _errorSuggestionsBox() {
    if (_errorSuggestions.isEmpty) return const SizedBox.shrink();

    return Container(
      margin: const EdgeInsets.only(top: 10),
      decoration: BoxDecoration(
        borderRadius: BorderRadius.circular(18),
        color: PremiumColors.ink.withOpacity(0.04),
        border: Border.all(color: PremiumColors.line),
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.stretch,
        children: [
          const Padding(
            padding: EdgeInsets.fromLTRB(14, 12, 14, 8),
            child: Text(
              "Suggestions",
              style: TextStyle(
                color: PremiumColors.secondary,
                fontSize: 13,
                fontWeight: FontWeight.w700,
              ),
            ),
          ),
          Divider(height: 1, color: PremiumColors.line),
          ListView.separated(
            shrinkWrap: true,
            physics: const NeverScrollableScrollPhysics(),
            itemCount: _errorSuggestions.length,
            separatorBuilder: (_, __) =>
                Divider(height: 1, color: PremiumColors.line),
            itemBuilder: (context, i) {
              final s = _errorSuggestions[i];
              final addr = (s['address'] ?? '').toString().trim();
              final zip = (s['zip'] ?? '').toString().trim();
              return InkWell(
                onTap: _loading ? null : () => _applySuggestion(s),
                child: Padding(
                  padding: const EdgeInsets.symmetric(
                    horizontal: 14,
                    vertical: 12,
                  ),
                  child: Row(
                    children: [
                      const Icon(
                        Icons.search,
                        size: 18,
                        color: PremiumColors.secondary,
                      ),
                      const SizedBox(width: 10),
                      Expanded(
                        child: Text(
                          zip.isEmpty ? addr : '$addr  •  $zip',
                          maxLines: 1,
                          overflow: TextOverflow.ellipsis,
                          style: const TextStyle(
                            fontSize: 14.5,
                            fontWeight: FontWeight.w600,
                            color: PremiumColors.ink,
                          ),
                        ),
                      ),
                      const Icon(
                        Icons.chevron_right,
                        color: PremiumColors.secondary,
                      ),
                    ],
                  ),
                ),
              );
            },
          ),
        ],
      ),
    );
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      body: GestureDetector(
        behavior: HitTestBehavior.translucent,
        onTap: () => FocusScope.of(context).unfocus(),
        child: Stack(
          children: [
            SafeArea(
              child: SingleChildScrollView(
                padding: const EdgeInsets.all(18),
                child: Column(
                  crossAxisAlignment: CrossAxisAlignment.stretch,
                  children: [
                    const SizedBox(height: 16),
                    const Center(
                      child: Text("Philly Scan", style: _titleStyle),
                    ),
                    const SizedBox(height: 6),
                    const Center(
                      child: Text(
                        "Property Facts & Insights",
                        style: _subtitleStyle,
                      ),
                    ),
                    if (ProPreviewConfig.enabled)
                      const Center(
                        child: Text(
                          'LIVE DATA TEST • $kAppVersion',
                          style: TextStyle(color: PremiumColors.green),
                        ),
                      ),
                    const SizedBox(height: 18),

                    const UpdateNotice(),
                    Align(
                      alignment: Alignment.centerRight,
                      child: IconButton(
                        tooltip: 'Settings & updates',
                        icon: const Icon(Icons.settings_outlined),
                        onPressed: () => showModalBottomSheet<void>(
                          context: context,
                          isScrollControlled: true,
                          builder: (_) => SafeArea(
                            child: SingleChildScrollView(
                              padding: const EdgeInsets.all(24),
                              child: Column(
                                mainAxisSize: MainAxisSize.min,
                                crossAxisAlignment: CrossAxisAlignment.stretch,
                                children: [
                                  const Text(
                                    'Philly Scan',
                                    style: TextStyle(
                                      fontSize: 24,
                                      fontWeight: FontWeight.w700,
                                    ),
                                  ),
                                  const Text(kAppVersion),
                                  const SizedBox(height: 18),
                                  const UpdateNotice(showControls: true),
                                  TextButton.icon(
                                    onPressed: _emailFeedback,
                                    icon: const Icon(Icons.mail_outline),
                                    label: const Text('Contact support'),
                                  ),
                                ],
                              ),
                            ),
                          ),
                        ),
                      ),
                    ),

                    TextButton.icon(
                      onPressed: _loading ? null : _openPro,
                      icon: const Icon(Icons.workspace_premium_outlined),
                      label: Text(
                        ProPreviewConfig.enabled
                            ? 'Open Pro • Live data'
                            : 'Upgrade to Pro',
                      ),
                    ),

                    Container(
                      padding: const EdgeInsets.symmetric(horizontal: 16),
                      decoration: BoxDecoration(
                        borderRadius: BorderRadius.circular(16),
                        border: Border.all(
                          color: PremiumColors.green,
                          width: 2,
                        ),
                        color: PremiumColors.paper,
                      ),
                      child: TextField(
                        controller: _controller,
                        focusNode: _focusNode,
                        onTap: () {
                          if (_suggestions.isNotEmpty) {
                            setState(() => _showSuggestions = true);
                          }
                        },
                        onSubmitted: (_) => _scan(),
                        decoration: const InputDecoration(
                          border: InputBorder.none,
                          hintText: "Enter Philly address or OPA",
                        ),
                      ),
                    ),
                    _suggestionsBox(),
                    const SizedBox(height: 14),
                    SizedBox(
                      height: 56,
                      child: ElevatedButton(
                        onPressed: _loading ? null : () => _scan(),
                        style: ElevatedButton.styleFrom(
                          backgroundColor: PremiumColors.line,
                          elevation: 0,
                          shape: RoundedRectangleBorder(
                            borderRadius: BorderRadius.circular(18),
                          ),
                        ),
                        child: _loading
                            ? const CircularProgressIndicator(strokeWidth: 2)
                            : const Text("Scan"),
                      ),
                    ),
                    if (_error != null) ...[
                      const SizedBox(height: 14),
                      _ErrorCard(message: _error!),
                      _errorSuggestionsBox(),
                    ],
                    if (_result != null) ...[
                      const SizedBox(height: 22),
                      _ResultCard(
                        result: _result!,
                        money: _money,
                        copy: _copy,
                        open: _open,
                        bodyStyle: _bodyStyle,
                        valueStyle: _valueStyle,
                        violationsService: _violationsService,
                        recordsClient: widget.recordsClient,
                      ),
                    ],
                  ],
                ),
              ),
            ),
          ],
        ),
      ),
    );
  }
}

class _ErrorCard extends StatelessWidget {
  final String message;
  const _ErrorCard({required this.message});

  @override
  Widget build(BuildContext context) {
    return Container(
      padding: const EdgeInsets.all(16),
      decoration: BoxDecoration(
        borderRadius: BorderRadius.circular(18),
        color: Colors.redAccent.withOpacity(0.10),
        border: Border.all(color: Colors.redAccent.withOpacity(0.25)),
      ),
      child: Row(
        children: [
          const Icon(Icons.error_outline, color: Colors.redAccent),
          const SizedBox(width: 10),
          Expanded(
            child: Text(
              message,
              style: const TextStyle(
                color: PremiumColors.ink,
                fontSize: 14.5,
                fontWeight: FontWeight.w600,
              ),
            ),
          ),
        ],
      ),
    );
  }
}

class _ResultCard extends StatelessWidget {
  final PropertyResult result;
  final String Function(int?) money;
  final Future<void> Function(String, String) copy;
  final Future<void> Function(String) open;
  final TextStyle bodyStyle;
  final TextStyle valueStyle;

  final ViolationsService violationsService;
  final ApiClient? recordsClient;

  const _ResultCard({
    required this.result,
    required this.money,
    required this.copy,
    required this.open,
    required this.bodyStyle,
    required this.valueStyle,
    required this.violationsService,
    this.recordsClient,
  });

  Widget _copyCard({
    required VoidCallback onTap,
    required String title,
    required IconData icon,
  }) {
    return Expanded(
      child: InkWell(
        borderRadius: BorderRadius.circular(18),
        onTap: onTap,
        child: Container(
          constraints: const BoxConstraints(minHeight: 94),
          padding: const EdgeInsets.symmetric(vertical: 12, horizontal: 4),
          decoration: BoxDecoration(
            borderRadius: BorderRadius.circular(18),
            color: PremiumColors.paper,
            border: Border.all(color: PremiumColors.line),
          ),
          child: Column(
            mainAxisAlignment: MainAxisAlignment.center,
            children: [
              Icon(icon, size: 20, color: PremiumColors.secondary),
              const SizedBox(height: 12),
              Text(
                title,
                textAlign: TextAlign.center,
                style: const TextStyle(
                  fontSize: 13,
                  fontWeight: FontWeight.w700,
                  color: PremiumColors.ink,
                ),
              ),
            ],
          ),
        ),
      ),
    );
  }

  @override
  Widget build(BuildContext context) {
    final address = result.address.trim();
    final owner = (result.owner ?? '').trim();
    final opa = result.opa.trim();

    return Container(
      padding: const EdgeInsets.all(18),
      decoration: BoxDecoration(
        borderRadius: BorderRadius.circular(22),
        color: PremiumColors.paper,
        border: Border.all(color: PremiumColors.line),
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.stretch,
        children: [
          Text(
            address,
            style: const TextStyle(
              fontSize: 23,
              fontWeight: FontWeight.w700,
              letterSpacing: 0.45,
            ),
          ),
          const SizedBox(height: 12),
          Text("Owner: ${owner.isEmpty ? '—' : owner}", style: bodyStyle),
          const SizedBox(height: 6),
          Text("OPA: $opa", style: bodyStyle),
          const SizedBox(height: 18),
          Divider(color: PremiumColors.ink.withOpacity(0.10)),
          const SizedBox(height: 18),
          LayoutBuilder(
            builder: (context, constraints) => Wrap(
              spacing: 10,
              runSpacing: 10,
              children: [
                for (final item in [
                  ('Last recorded sale', money(result.salePrice)),
                  ('Sale date', result.saleDate ?? '—'),
                  ('City assessment', money(result.marketValue)),
                  ('Zoning', zoningDescription(result.zoning)),
                ])
                  SizedBox(
                    width: constraints.maxWidth < 280
                        ? constraints.maxWidth
                        : (constraints.maxWidth - 10) / 2,
                    child: FactBox(label: item.$1, value: item.$2),
                  ),
              ],
            ),
          ),
          if (opa.isNotEmpty)
            ViolationSummaryCard(opa: opa, service: violationsService),
          const SizedBox(height: 18),
          Row(
            children: [
              _copyCard(
                onTap: () => copy(address, "Address copied"),
                title: "Copy Address",
                icon: Icons.copy,
              ),
              const SizedBox(width: 12),
              _copyCard(
                onTap: () => copy(owner, "Owner copied"),
                title: "Copy Owner",
                icon: Icons.person_outline,
              ),
              const SizedBox(width: 12),
              _copyCard(
                onTap: () => copy(opa, "OPA copied"),
                title: "Copy OPA",
                icon: Icons.confirmation_number_outlined,
              ),
            ],
          ),
          const SizedBox(height: 14),
          InkWell(
            borderRadius: BorderRadius.circular(18),
            onTap: () async {
              await copy(address, "Address copied");
              await open("https://tax-services.phila.gov/_/");
            },
            child: Container(
              padding: const EdgeInsets.symmetric(horizontal: 18, vertical: 16),
              decoration: BoxDecoration(
                borderRadius: BorderRadius.circular(18),
                color: PremiumColors.paper,
                border: Border.all(color: PremiumColors.line),
              ),
              child: Row(
                children: [
                  const Expanded(
                    child: Column(
                      crossAxisAlignment: CrossAxisAlignment.start,
                      children: [
                        Text(
                          "Visit Philadelphia Tax Website",
                          style: TextStyle(
                            fontSize: 17,
                            fontWeight: FontWeight.w700,
                            color: PremiumColors.ink,
                          ),
                        ),
                        SizedBox(height: 6),
                        Text(
                          "Opens the official City of Philadelphia tax portal",
                          style: TextStyle(
                            fontSize: 13,
                            color: PremiumColors.secondary,
                          ),
                        ),
                      ],
                    ),
                  ),
                  Icon(
                    Icons.account_balance,
                    size: 22,
                    color: PremiumColors.secondary,
                  ),
                ],
              ),
            ),
          ),
          const SizedBox(height: 20),
          FeatureMenu(opa: opa, address: address, client: recordsClient),
        ],
      ),
    );
  }
}
