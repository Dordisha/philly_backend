import 'package:flutter/material.dart';

abstract final class PremiumColors {
  static const background = Color(0xFFF6F3EF);
  static const paper = Color(0xFFFFFDFC);
  static const ink = Color(0xFF192B28);
  static const secondary = Color(0xFF626B65);
  static const green = Color(0xFF205C4B);
  static const line = Color(0xFFE1DED7);
}

ThemeData premiumTheme() => ThemeData(
  useMaterial3: true,
  brightness: Brightness.light,
  scaffoldBackgroundColor: PremiumColors.background,
  colorScheme: ColorScheme.fromSeed(seedColor: PremiumColors.green,
    primary: PremiumColors.green, surface: PremiumColors.paper,
    onSurface: PremiumColors.ink),
  appBarTheme: const AppBarTheme(backgroundColor: PremiumColors.background,
    foregroundColor: PremiumColors.ink, centerTitle: true, elevation: 0),
  cardTheme: CardThemeData(elevation: 0, color: PremiumColors.paper,
    margin: const EdgeInsets.symmetric(vertical: 6),
    shape: RoundedRectangleBorder(borderRadius: BorderRadius.circular(16),
      side: const BorderSide(color: PremiumColors.line))),
  dividerTheme: const DividerThemeData(color: PremiumColors.line),
  inputDecorationTheme: const InputDecorationTheme(filled: true,
    fillColor: PremiumColors.paper, border: OutlineInputBorder()),
);

// Explicit supported labels only. Unknown districts retain the source code.
String zoningDescription(String? value) {
  final code = value?.trim() ?? '';
  if (code.isEmpty) return 'Not recorded';
  const labels = {
    'CMX5': 'Center City Core Commercial Mixed-Use',
    'RSA5': 'Residential Single-Family Attached',
  };
  final label = labels[code.toUpperCase().replaceAll('-', '')];
  return label == null ? '$code · Description unavailable' : '$code · $label';
}

class FactBox extends StatelessWidget {
  final String label;
  final String value;
  const FactBox({super.key, required this.label, required this.value});
  @override
  Widget build(BuildContext context) => Container(
    padding: const EdgeInsets.all(14),
    decoration: BoxDecoration(color: PremiumColors.paper,
      border: Border.all(color: PremiumColors.line), borderRadius: BorderRadius.circular(13)),
    child: Column(crossAxisAlignment: CrossAxisAlignment.start, children: [
      Text(label, style: const TextStyle(fontSize: 12, color: PremiumColors.secondary)),
      const SizedBox(height: 7),
      Text(value, style: const TextStyle(fontSize: 16, fontWeight: FontWeight.w600)),
    ]),
  );
}

class FeatureTile extends StatelessWidget {
  final String title;
  final IconData icon;
  final VoidCallback onTap;
  final String? subtitle;
  const FeatureTile({super.key, required this.title, required this.icon,
    required this.onTap, this.subtitle});
  @override
  Widget build(BuildContext context) => Card(child: InkWell(
    borderRadius: BorderRadius.circular(16), onTap: onTap,
    child: Padding(padding: const EdgeInsets.all(16), child: Column(
      crossAxisAlignment: CrossAxisAlignment.start, children: [
        Row(children: [Icon(icon, size: 23, color: PremiumColors.green),
          const Spacer(), const Icon(Icons.chevron_right, size: 18)]),
        const SizedBox(height: 12),
        Text(title, style: const TextStyle(fontSize: 16, fontWeight: FontWeight.w600)),
        if (subtitle != null) ...[const SizedBox(height: 5),
          Text(subtitle!, style: const TextStyle(fontSize: 12, color: PremiumColors.secondary))],
      ])),
  ));
}
