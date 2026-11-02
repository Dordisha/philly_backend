# Philly Scan source previews

## 1.1.0-preview.4 — build 7 — 2026-11-02
- Replace Unknown market-status box with Check status and outbound Zillow address search.
- Require Pro access for the action; keep debug preview available without payment.
- Preserve address/unit text and show browser launch errors.
- Show the preview version and build together on the home screen and in feedback.
- Align Xcode instructions with the updated feature.

Validation: prior feature commit c668f2011f9dd93d171c94738e95c9982ee67706 passed Flutter CI. This version's CI is recorded on PR #4. Xcode/iPhone and Zillow destination acceptance remain unverified. Billing verification remains unfinished.

## 1.1.0-preview.3 — build 6
Previous source preview. Pro live-data overview and debug testing access.

## Versioning policy
Each delivered source preview increments the preview number and build number.
Keep pubspec.yaml, kAppVersion, VERSION.txt, and OPEN_IN_XCODE.txt aligned.
Use an exact commit ZIP link for each handoff, not a moving branch ZIP.
A source preview does not establish a successful Xcode build or public release.
