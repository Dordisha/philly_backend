import 'package:flutter/material.dart';

import '../src/violations_service.dart';
import 'pro_coming_soon.dart';

class ViolationSummaryCard extends StatefulWidget {
  final String opa;
  final ViolationsService service;

  const ViolationSummaryCard({
    super.key,
    required this.opa,
    required this.service,
  });

  @override
  State<ViolationSummaryCard> createState() => _ViolationSummaryCardState();
}

class _ViolationSummaryCardState extends State<ViolationSummaryCard> {
  late Future<ViolationSummary> _future;

  @override
  void initState() {
    super.initState();
    _future = widget.service.fetchSummary(opa: widget.opa);
  }

  @override
  void didUpdateWidget(covariant ViolationSummaryCard oldWidget) {
    super.didUpdateWidget(oldWidget);
    if (oldWidget.opa != widget.opa || oldWidget.service != widget.service) {
      _future = widget.service.fetchSummary(opa: widget.opa);
    }
  }

  String _fmtDate(DateTime? d) {
    if (d == null) return "—";
    final y = d.year.toString().padLeft(4, '0');
    final m = d.month.toString().padLeft(2, '0');
    final day = d.day.toString().padLeft(2, '0');
    return "$y-$m-$day";
  }

  @override
  Widget build(BuildContext context) {
    return FutureBuilder<ViolationSummary>(
      future: _future,
      builder: (context, snap) {
        Widget body;

        if (snap.connectionState != ConnectionState.done) {
          body = const Padding(
            padding: EdgeInsets.all(16),
            child: Row(
              children: [
                SizedBox(
                  width: 18,
                  height: 18,
                  child: CircularProgressIndicator(strokeWidth: 2),
                ),
                SizedBox(width: 10),
                Text(
                  "Loading violations…",
                  style: TextStyle(color: Colors.white70),
                ),
              ],
            ),
          );
        } else if (snap.hasError) {
          body = Padding(
            padding: const EdgeInsets.all(16),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                const Text(
                  'Enforcement records unavailable. This does not mean there are no violations.',
                ),
                TextButton(
                  onPressed: () => setState(() {
                    _future = widget.service.fetchSummary(opa: widget.opa);
                  }),
                  child: const Text('Retry enforcement'),
                ),
              ],
            ),
          );
        } else {
          final s = snap.data!;
          body = Padding(
            padding: const EdgeInsets.all(16),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                const Text(
                  "Property Condition & Enforcement",
                  style: TextStyle(
                    color: Colors.white,
                    fontSize: 16,
                    fontWeight: FontWeight.w800,
                  ),
                ),
                const SizedBox(height: 10),
                Wrap(
                  spacing: 10,
                  runSpacing: 8,
                  children: [
                    _pill(
                      "Active",
                      s.activeCount.toString(),
                      strong: s.activeCount > 0,
                    ),
                    _pill("History", s.historicalCount.toString()),
                    _pill("Risk", s.riskLevel, strong: s.riskLevel != "Low"),
                    _pill("Last", _fmtDate(s.lastActivity)),
                  ],
                ),
                const SizedBox(height: 12),
                _flagsRow(s),
                const SizedBox(height: 14),
                SizedBox(
                  width: double.infinity,
                  child: ElevatedButton(
                    style: ElevatedButton.styleFrom(
                      backgroundColor: const Color(0xFF1B1B1B),
                      foregroundColor: Colors.white,
                      padding: const EdgeInsets.symmetric(vertical: 12),
                      shape: RoundedRectangleBorder(
                        borderRadius: BorderRadius.circular(12),
                      ),
                      side: const BorderSide(color: Color(0xFF2A2A2A)),
                    ),
                    onPressed: () async {
                      await showProComingSoonDialog(
                        context,
                        opa: widget.opa, initialSection: 1,
                        title: "Enforcement Details",
                        message:
                            "You can see the summary for free.\n\nDetailed case history and enforcement records will be available in the Pro version soon.",
                      );
                    },
                    child: const Text("View enforcement details"),
                  ),
                ),
              ],
            ),
          );
        }

        return Container(
          margin: const EdgeInsets.only(top: 12),
          decoration: BoxDecoration(
            color: const Color(0xFF0F0F0F),
            borderRadius: BorderRadius.circular(18),
            border: Border.all(color: const Color(0xFF2A2A2A)),
          ),
          child: body,
        );
      },
    );
  }

  Widget _flagsRow(ViolationSummary s) {
    final flags = <String>[];
    if (s.hasHazardous) flags.add("Hazardous");
    if (s.hasCourt) flags.add("Court");
    if (s.hasStopWork) flags.add("Stop Work");
    if (s.hasUnsafeStructure) flags.add("Unsafe Structure");

    if (flags.isEmpty) {
      return const Text(
        "No severity flags detected.",
        style: TextStyle(color: Colors.white54),
      );
    }

    return Wrap(
      spacing: 8,
      runSpacing: 8,
      children: flags.map((f) => _tag(f)).toList(),
    );
  }

  static Widget _pill(String k, String v, {bool strong = false}) {
    return Container(
      padding: const EdgeInsets.symmetric(horizontal: 10, vertical: 7),
      decoration: BoxDecoration(
        color: strong ? const Color(0xFF2A1A00) : const Color(0xFF161616),
        borderRadius: BorderRadius.circular(999),
        border: Border.all(
          color: strong ? const Color(0xFF8A5A00) : const Color(0xFF2A2A2A),
        ),
      ),
      child: Text(
        "$k: $v",
        style: TextStyle(
          color: strong ? Colors.white : Colors.white70,
          fontSize: 12,
        ),
      ),
    );
  }

  static Widget _tag(String label) {
    return Container(
      padding: const EdgeInsets.symmetric(horizontal: 10, vertical: 7),
      decoration: BoxDecoration(
        color: const Color(0xFF121212),
        borderRadius: BorderRadius.circular(999),
        border: Border.all(color: const Color(0xFF2A2A2A)),
      ),
      child: Text(
        label,
        style: const TextStyle(color: Colors.white70, fontSize: 12),
      ),
    );
  }
}
