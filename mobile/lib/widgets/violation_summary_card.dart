import 'premium_ui.dart';
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
                  style: TextStyle(color: PremiumColors.secondary),
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
                  'We couldn’t load City violation records. Try again. This does not mean there are no violations.',
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
                  "Recorded L&I Violations",
                  style: TextStyle(
                    color: PremiumColors.ink,
                    fontSize: 16,
                    fontWeight: FontWeight.w800,
                  ),
                ),
                const SizedBox(height: 10),
                Wrap(
                  spacing: 10,
                  runSpacing: 8,
                  children: [
                    _pill("Records", s.totalCount.toString()),
                    _pill("Last", _fmtDate(s.lastActivity)),
                  ],
                ),
                const SizedBox(height: 12),
                const Text(
                  'Records are not unique cases or a current condition assessment. '
                  'Active violations and risk are not inferred.',
                  style: TextStyle(color: PremiumColors.secondary),
                ),
                const SizedBox(height: 8),
                if (s.totalCount == 0)
                  const Text(
                    'This lookup returned no City violation records for this property. '
                    'This is not proof of safety or condition.',
                  )
                else
                  ...s.statuses.map((status) => Padding(
                    padding: const EdgeInsets.only(bottom: 6),
                    child: Text(
                      'Case: ${status.caseStatus ?? "Not reported"} · '
                      'Violation: ${status.violationStatus ?? "Not reported"} · '
                      'Records: ${status.count}',
                    ),
                  )),
                const Text('Source: Philadelphia L&I violations (CARTO).'),
                const SizedBox(height: 14),
                SizedBox(
                  width: double.infinity,
                  child: ElevatedButton(
                    style: ElevatedButton.styleFrom(
                      backgroundColor: const Color(0xFFF0F3EF),
                      foregroundColor: PremiumColors.ink,
                      padding: const EdgeInsets.symmetric(vertical: 12),
                      shape: RoundedRectangleBorder(
                        borderRadius: BorderRadius.circular(12),
                      ),
                      side: const BorderSide(color: Color(0xFFE1DED7)),
                    ),
                    onPressed: () async {
                      await showProComingSoonDialog(
                        context,
                        opa: widget.opa,
                        initialSection: 1,
                        title: "Enforcement Details",
                        message:
                            "Detailed enforcement records are free. Verify published statuses and dates with the City.",
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
            color: const Color(0xFFFFFDFC),
            borderRadius: BorderRadius.circular(18),
            border: Border.all(color: const Color(0xFFE1DED7)),
          ),
          child: body,
        );
      },
    );
  }

  static Widget _pill(String k, String v, {bool strong = false}) {
    return Container(
      padding: const EdgeInsets.symmetric(horizontal: 10, vertical: 7),
      decoration: BoxDecoration(
        color: strong ? const Color(0xFFFFEED9) : const Color(0xFFF4F2ED),
        borderRadius: BorderRadius.circular(999),
        border: Border.all(
          color: strong ? const Color(0xFF8A5A00) : const Color(0xFFE1DED7),
        ),
      ),
      child: Text(
        "$k: $v",
        style: TextStyle(
          color: strong ? PremiumColors.ink : PremiumColors.secondary,
          fontSize: 12,
        ),
      ),
    );
  }

}
