// lib/src/main.dart
import 'dart:async';
import 'dart:ui';

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
const String kAppVersion = "1.0.3 (4) • reliability preview";

/// Feedback destinations (NO backend required)
const String kFeedbackFormUrl = "https://forms.gle/uiQnojGLdfNE94bV6";

/// Optional: fallback email (kept, even if you mostly use the form)
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
      theme: ThemeData(brightness: Brightness.dark, useMaterial3: true),
      home: const HomeScreen(),
    );
  }
}

class HomeScreen extends StatefulWidget {
  const HomeScreen({super.key, this.apiService, this.violationsService, this.recordsClient});
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

  // ✅ Feedback banner (session-only dismiss)
  bool _showFeedbackBanner = true;

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
    color: Colors.white60,
  );

  static const _bodyStyle = TextStyle(
    fontSize: 15,
    fontWeight: FontWeight.w400,
    letterSpacing: 0.25,
    color: Colors.white70,
  );

  static const _valueStyle = TextStyle(
    fontSize: 17,
    fontWeight: FontWeight.w500,
    letterSpacing: 0.25,
    color: Colors.white,
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
    await showProComingSoonDialog(context, opa: _result?.opa,
      address: _result?.address ?? '', recordsClient: widget.recordsClient);
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

  Future<void> _openFeedbackForm() async {
    final url = kFeedbackFormUrl.trim();
    if (url.isEmpty) {
      if (!mounted) return;
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(content: Text("Feedback form URL not set yet.")),
      );
      return;
    }
    await _open(url);
  }

  Future<void> _emailFeedback() async {
    final to = kFeedbackEmail.trim();
    if (to.isEmpty) return;

    final subject = Uri.encodeComponent("Philly Scan Feedback (v$kAppVersion)");
    final body = Uri.encodeComponent(
      "Quick feedback:\n\n"
      "What worked:\n- \n\n"
      "What didn’t:\n- \n\n"
      "Suggestion:\n- \n\n"
      "App Version: $kAppVersion\n"
      "Backend: TEST (Render)\n",
    );

    final mailto = "mailto:$to?subject=$subject&body=$body";
    await launchUrl(Uri.parse(mailto), mode: LaunchMode.externalApplication);
  }

  void _showFeedbackOptions() {
    showModalBottomSheet(
      context: context,
      backgroundColor: const Color(0xFF12111A),
      shape: const RoundedRectangleBorder(
        borderRadius: BorderRadius.vertical(top: Radius.circular(22)),
      ),
      builder: (_) {
        return SafeArea(
          child: Padding(
            padding: const EdgeInsets.fromLTRB(18, 14, 18, 18),
            child: Column(
              mainAxisSize: MainAxisSize.min,
              crossAxisAlignment: CrossAxisAlignment.stretch,
              children: [
                const Text(
                  "Your Voice Matters",
                  style: TextStyle(
                    fontSize: 18,
                    fontWeight: FontWeight.w800,
                    color: Colors.white,
                  ),
                ),
                const SizedBox(height: 10),
                const Text(
                  "Help us improve Philly Scan.",
                  style: TextStyle(color: Colors.white70, height: 1.25),
                ),
                const SizedBox(height: 14),
                ElevatedButton(
                  onPressed: () async {
                    Navigator.pop(context);
                    await _openFeedbackForm();
                  },
                  child: const Text("Open Feedback Form"),
                ),
                const SizedBox(height: 10),
                OutlinedButton(
                  onPressed: () async {
                    Navigator.pop(context);
                    await _emailFeedback();
                  },
                  child: const Text("Email Feedback"),
                ),
              ],
            ),
          ),
        );
      },
    );
  }

  Widget _feedbackBanner() {
    if (!_showFeedbackBanner) return const SizedBox.shrink();

    return Container(
      margin: const EdgeInsets.only(bottom: 14),
      padding: const EdgeInsets.fromLTRB(14, 12, 12, 12),
      decoration: BoxDecoration(
        borderRadius: BorderRadius.circular(18),
        color: Colors.deepPurpleAccent.withOpacity(0.12),
        border: Border.all(color: Colors.deepPurpleAccent.withOpacity(0.30)),
      ),
      child: Row(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          const Icon(Icons.feedback_outlined, color: Colors.white70, size: 22),
          const SizedBox(width: 10),
          Expanded(
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                const Text(
                  "Your Voice Matters",
                  style: TextStyle(
                    color: Colors.white,
                    fontSize: 16,
                    fontWeight: FontWeight.w800,
                  ),
                ),
                const SizedBox(height: 4),
                const Text(
                  "Help us improve Philly Scan.",
                  style: TextStyle(
                    color: Colors.white70,
                    fontSize: 13,
                    height: 1.15,
                  ),
                ),
                const SizedBox(height: 10),
                InkWell(
                  onTap: _showFeedbackOptions,
                  borderRadius: BorderRadius.circular(14),
                  child: Container(
                    padding: const EdgeInsets.symmetric(
                      horizontal: 12,
                      vertical: 10,
                    ),
                    decoration: BoxDecoration(
                      borderRadius: BorderRadius.circular(14),
                      color: Colors.white.withOpacity(0.06),
                      border: Border.all(color: Colors.white10),
                    ),
                    child: const Row(
                      mainAxisSize: MainAxisSize.min,
                      children: [
                        Icon(
                          Icons.edit_outlined,
                          size: 18,
                          color: Colors.white70,
                        ),
                        SizedBox(width: 8),
                        Text(
                          "Share Feedback",
                          style: TextStyle(
                            color: Colors.white,
                            fontWeight: FontWeight.w700,
                          ),
                        ),
                      ],
                    ),
                  ),
                ),
              ],
            ),
          ),
          IconButton(
            onPressed: () => setState(() => _showFeedbackBanner = false),
            icon: const Icon(Icons.close, color: Colors.white60, size: 18),
          ),
        ],
      ),
    );
  }

  Widget _suggestionsBox() {
    if (!_showSuggestions || _suggestions.isEmpty)
      return const SizedBox.shrink();

    return Container(
      margin: const EdgeInsets.only(top: 8),
      decoration: BoxDecoration(
        borderRadius: BorderRadius.circular(16),
        color: const Color(0xFF12111A),
        border: Border.all(color: Colors.deepPurpleAccent.withOpacity(0.35)),
      ),
      child: ListView.separated(
        shrinkWrap: true,
        physics: const NeverScrollableScrollPhysics(),
        itemCount: _suggestions.length,
        separatorBuilder: (_, __) =>
            Divider(height: 1, color: Colors.white.withOpacity(0.06)),
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
                    color: Colors.white70,
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
                        color: Colors.white,
                      ),
                    ),
                  ),
                  const SizedBox(width: 10),
                  const Icon(Icons.chevron_right, color: Colors.white38),
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
        color: Colors.white.withOpacity(0.04),
        border: Border.all(color: Colors.white10),
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.stretch,
        children: [
          const Padding(
            padding: EdgeInsets.fromLTRB(14, 12, 14, 8),
            child: Text(
              "Suggestions",
              style: TextStyle(
                color: Colors.white70,
                fontSize: 13,
                fontWeight: FontWeight.w700,
              ),
            ),
          ),
          Divider(height: 1, color: Colors.white.withOpacity(0.06)),
          ListView.separated(
            shrinkWrap: true,
            physics: const NeverScrollableScrollPhysics(),
            itemCount: _errorSuggestions.length,
            separatorBuilder: (_, __) =>
                Divider(height: 1, color: Colors.white.withOpacity(0.06)),
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
                      const Icon(Icons.search, size: 18, color: Colors.white60),
                      const SizedBox(width: 10),
                      Expanded(
                        child: Text(
                          zip.isEmpty ? addr : '$addr  •  $zip',
                          maxLines: 1,
                          overflow: TextOverflow.ellipsis,
                          style: const TextStyle(
                            fontSize: 14.5,
                            fontWeight: FontWeight.w600,
                            color: Colors.white,
                          ),
                        ),
                      ),
                      const Icon(Icons.chevron_right, color: Colors.white38),
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
            const DecoratedBox(
              decoration: BoxDecoration(
                gradient: RadialGradient(
                  center: Alignment(0, -0.6),
                  radius: 1.2,
                  colors: [
                    Color(0xFF1B1A23),
                    Color(0xFF0F0E14),
                    Color(0xFF0B0A10),
                  ],
                ),
              ),
              child: SizedBox.expand(),
            ),
            IgnorePointer(
              child: Align(
                alignment: const Alignment(0, 0.18),
                child: Opacity(
                  opacity: 0.075,
                  child: ImageFiltered(
                    imageFilter: ImageFilter.blur(sigmaX: 0.6, sigmaY: 0.6),
                    child: Image.asset(
                      'assets/images/finallogo.png',
                      width: 420,
                      height: 420,
                      fit: BoxFit.contain,
                    ),
                  ),
                ),
              ),
            ),
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
                    if (ProPreviewConfig.enabled) const Center(
                      child: Text('LIVE DATA TEST • R2', style: TextStyle(color: Colors.greenAccent)),
                    ),
                    const SizedBox(height: 18),

                    // ✅ Feedback banner
                    _feedbackBanner(),

                    TextButton.icon(
                      onPressed: _loading ? null : _openPro,
                      icon: const Icon(Icons.workspace_premium_outlined),
                      label: Text(ProPreviewConfig.enabled ? 'Open Pro • Live data' : 'Upgrade to Pro'),
                    ),

                    Container(
                      padding: const EdgeInsets.symmetric(horizontal: 16),
                      decoration: BoxDecoration(
                        borderRadius: BorderRadius.circular(16),
                        border: Border.all(
                          color: Colors.deepPurpleAccent,
                          width: 2,
                        ),
                        color: Colors.white.withOpacity(0.03),
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
                          backgroundColor: Colors.white.withOpacity(0.06),
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
                color: Colors.white,
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

  const _ResultCard({
    required this.result,
    required this.money,
    required this.copy,
    required this.open,
    required this.bodyStyle,
    required this.valueStyle,
    required this.violationsService,
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
          height: 86,
          decoration: BoxDecoration(
            borderRadius: BorderRadius.circular(18),
            color: Colors.white.withOpacity(0.05),
            border: Border.all(color: Colors.white10),
          ),
          child: Column(
            mainAxisAlignment: MainAxisAlignment.center,
            children: [
              Icon(icon, size: 20, color: Colors.white70),
              const SizedBox(height: 12),
              Text(
                title,
                textAlign: TextAlign.center,
                style: const TextStyle(
                  fontSize: 16,
                  fontWeight: FontWeight.w700,
                  color: Colors.white,
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
        color: Colors.white.withOpacity(0.05),
        border: Border.all(color: Colors.white10),
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.stretch,
        children: [
          Text(
            address.toUpperCase(),
            style: const TextStyle(
              fontSize: 26,
              fontWeight: FontWeight.w700,
              letterSpacing: 0.45,
            ),
          ),
          const SizedBox(height: 12),
          Text("Owner: ${owner.isEmpty ? '—' : owner}", style: bodyStyle),
          const SizedBox(height: 6),
          Text("OPA: $opa", style: bodyStyle),
          const SizedBox(height: 18),
          Divider(color: Colors.white.withOpacity(0.10)),
          const SizedBox(height: 18),
          Text("Sale Price: ${money(result.salePrice)}", style: valueStyle),
          const SizedBox(height: 8),
          Text("Sale Date: ${result.saleDate ?? '—'}", style: valueStyle),
          const SizedBox(height: 12),
          Text("Market Value: ${money(result.marketValue)}", style: valueStyle),
          const SizedBox(height: 8),
          Text("Zoning: ${result.zoning ?? '—'}", style: valueStyle),
          const SizedBox(height: 14),
          if (opa.isNotEmpty) ...[
            Wrap(spacing: 8, runSpacing: 8, children: [
              for (final item in [(0, '311 records'), (1, 'L&I records'), (2, 'Resale value'), (3, 'Signals')])
                OutlinedButton(
                  onPressed: () => showProComingSoonDialog(context,
                    opa: opa, address: address, initialSection: item.$1),
                  child: Text(item.$2),
                ),
            ]),
            ViolationSummaryCard(opa: opa, service: violationsService),
          ],
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
                color: Colors.white.withOpacity(0.05),
                border: Border.all(color: Colors.white10),
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
                            color: Colors.white,
                          ),
                        ),
                        SizedBox(height: 6),
                        Text(
                          "Opens the official City of Philadelphia tax portal",
                          style: TextStyle(fontSize: 13, color: Colors.white60),
                        ),
                      ],
                    ),
                  ),
                  Icon(Icons.account_balance, size: 22, color: Colors.white54),
                ],
              ),
            ),
          ),
        ],
      ),
    );
  }
}
