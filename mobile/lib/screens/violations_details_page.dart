import 'package:flutter/material.dart';
import '../src/violations_service.dart';

class ViolationsDetailsPage extends StatefulWidget {
  final String opa;
  final ViolationsService service;

  const ViolationsDetailsPage({
    super.key,
    required this.opa,
    required this.service,
  });

  @override
  State<ViolationsDetailsPage> createState() => _ViolationsDetailsPageState();
}

class _ViolationsDetailsPageState extends State<ViolationsDetailsPage> {
  late Future<List<ViolationRow>> _future;

  @override
  void initState() {
    super.initState();
    _future = widget.service.fetchDetails(opa: widget.opa, limit: 200);
  }

  String _fmtDate(DateTime? d) {
    if (d == null) return "—";
    final y = d.year.toString().padLeft(4, '0');
    final m = d.month.toString().padLeft(2, '0');
    final day = d.day.toString().padLeft(2, '0');
    return "$y-$m-$day";
  }

  bool _isActiveCase(ViolationRow r) {
    final s = (r.casestatus ?? "").toUpperCase().trim();
    return s.isNotEmpty && s != "CLOSED";
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      backgroundColor: Colors.black,
      appBar: AppBar(
        backgroundColor: Colors.black,
        foregroundColor: Colors.white,
        title: Text("Violations • OPA ${widget.opa}"),
      ),
      body: FutureBuilder<List<ViolationRow>>(
        future: _future,
        builder: (context, snap) {
          if (snap.connectionState != ConnectionState.done) {
            return const Center(child: CircularProgressIndicator());
          }
          if (snap.hasError) {
            return Padding(
              padding: const EdgeInsets.all(16),
              child: Text(
                "Error: ${snap.error}",
                style: const TextStyle(color: Colors.white),
              ),
            );
          }

          final rows = snap.data ?? [];
          final active = rows.where(_isActiveCase).toList();
          final closed = rows.where((r) => !_isActiveCase(r)).toList();

          return ListView(
            padding: const EdgeInsets.all(16),
            children: [
              _SectionHeader(title: "Active / Enforcement (${active.length})"),
              const SizedBox(height: 8),
              if (active.isEmpty)
                const _EmptyBlock(text: "No active violations found."),
              ...active.map((r) => _ViolationTile(row: r, fmtDate: _fmtDate)),

              const SizedBox(height: 18),
              _SectionHeader(title: "History (Closed) (${closed.length})"),
              const SizedBox(height: 8),
              if (closed.isEmpty)
                const _EmptyBlock(text: "No historical violations found."),
              ...closed.map((r) => _ViolationTile(row: r, fmtDate: _fmtDate)),
            ],
          );
        },
      ),
    );
  }
}

class _SectionHeader extends StatelessWidget {
  final String title;
  const _SectionHeader({required this.title});

  @override
  Widget build(BuildContext context) {
    return Text(
      title,
      style: const TextStyle(
        color: Colors.white,
        fontSize: 16,
        fontWeight: FontWeight.w700,
      ),
    );
  }
}

class _EmptyBlock extends StatelessWidget {
  final String text;
  const _EmptyBlock({required this.text});

  @override
  Widget build(BuildContext context) {
    return Container(
      padding: const EdgeInsets.all(14),
      margin: const EdgeInsets.only(bottom: 10),
      decoration: BoxDecoration(
        color: const Color(0xFF121212),
        borderRadius: BorderRadius.circular(14),
        border: Border.all(color: const Color(0xFF2A2A2A)),
      ),
      child: Text(text, style: const TextStyle(color: Colors.white70)),
    );
  }
}

class _ViolationTile extends StatelessWidget {
  final ViolationRow row;
  final String Function(DateTime?) fmtDate;

  const _ViolationTile({required this.row, required this.fmtDate});

  @override
  Widget build(BuildContext context) {
    final isActive = (row.casestatus ?? "").toUpperCase() != "CLOSED";
    final title = row.violationcodetitle ?? "(No title)";
    final status = row.casestatus ?? "—";
    final priority = row.caseprioritydesc ?? "—";

    return Container(
      padding: const EdgeInsets.all(14),
      margin: const EdgeInsets.only(bottom: 10),
      decoration: BoxDecoration(
        color: const Color(0xFF0F0F0F),
        borderRadius: BorderRadius.circular(14),
        border: Border.all(
          color: isActive ? const Color(0xFF5A3A00) : const Color(0xFF2A2A2A),
        ),
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Text(
            title,
            style: const TextStyle(
              color: Colors.white,
              fontSize: 15,
              fontWeight: FontWeight.w700,
            ),
          ),
          const SizedBox(height: 6),
          Wrap(
            spacing: 10,
            runSpacing: 6,
            children: [
              _Pill(label: "Case: $status", strong: isActive),
              _Pill(label: "Priority: $priority"),
              _Pill(label: "Violation Date: ${fmtDate(row.violationdate)}"),
              if (row.violationstatus != null)
                _Pill(label: "Violation: ${row.violationstatus}"),
              if (row.violationresolutioncode != null)
                _Pill(label: "Resolution: ${row.violationresolutioncode}"),
            ],
          ),
        ],
      ),
    );
  }
}

class _Pill extends StatelessWidget {
  final String label;
  final bool strong;

  const _Pill({required this.label, this.strong = false});

  @override
  Widget build(BuildContext context) {
    return Container(
      padding: const EdgeInsets.symmetric(horizontal: 10, vertical: 6),
      decoration: BoxDecoration(
        color: strong ? const Color(0xFF2A1A00) : const Color(0xFF161616),
        borderRadius: BorderRadius.circular(999),
        border: Border.all(
          color: strong ? const Color(0xFF8A5A00) : const Color(0xFF2A2A2A),
        ),
      ),
      child: Text(
        label,
        style: TextStyle(
          color: strong ? Colors.white : Colors.white70,
          fontSize: 12,
        ),
      ),
    );
  }
}
