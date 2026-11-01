import 'package:flutter/material.dart';

Future<void> showProComingSoonDialog(
  BuildContext context, {
  String title = "Enforcement Details",
  String message =
      "You can see the summary for free.\n\nDetailed case history and enforcement records will be available in the Pro version soon.",
}) async {
  return showDialog<void>(
    context: context,
    builder: (_) {
      return AlertDialog(
        backgroundColor: const Color(0xFF12111A),
        shape: RoundedRectangleBorder(borderRadius: BorderRadius.circular(18)),
        titlePadding: const EdgeInsets.fromLTRB(16, 16, 16, 0),
        contentPadding: const EdgeInsets.fromLTRB(16, 12, 16, 0),
        actionsPadding: const EdgeInsets.fromLTRB(8, 6, 8, 10),
        title: Row(
          children: const [
            Icon(Icons.lock_outline, color: Colors.white70, size: 20),
            SizedBox(width: 10),
            Expanded(
              child: Text(
                "Coming Soon – Pro Feature",
                style: TextStyle(
                  color: Colors.white,
                  fontWeight: FontWeight.w800,
                  fontSize: 16,
                ),
              ),
            ),
          ],
        ),
        content: Text(
          "$title\n\n$message",
          style: const TextStyle(
            color: Colors.white70,
            height: 1.25,
            fontSize: 13.5,
          ),
        ),
        actions: [
          TextButton(
            onPressed: () => Navigator.pop(context),
            child: const Text(
              "Got it",
              style: TextStyle(
                color: Colors.white,
                fontWeight: FontWeight.w700,
              ),
            ),
          ),
        ],
      );
    },
  );
}
