# 1.1.0 (19) — Free launch candidate

- All public-record features remain free; no Pro purchase flow.
- Production AI feature buttons replaced with an informational Coming soon card.
- Planned AI homebuyer research is $12.99/month; no purchase or AI service available yet.
- Planned languages and buying-cost guidance described without claiming availability.
- Xcode build number follows Flutter; Mac preparation runs analysis and free-mode tests.

## 1.1.0-preview.8 (11)

- Hide Schools from the feature menu; deferred to January 2027 data validation, otherwise drop it.

- Connect reported crime trends to PPD CARTO data using exact OPA parcel coordinates.
- Add selectable radii, equal 90-day comparisons, categories, 12 complete months and source limitations.
- Backend deployment is required before the mobile feature can load from its configured server.

## 1.1.0-preview.7 (10)

- Add a green/gold symbol and PHILLY SCAN wordmark to the home header.
- Add subtle, non-interactive green/gold watermarks behind home, records and premium screens.
- Apply the approved green/gold app icon to every iOS AppIcon size.
- Preserve the dedicated icon master in launcher-generation configuration.

# 1.1.0-preview.6 (9)

- Record approved green/gold/ivory identity; production icon export pending.
- Server-controlled minimum version gate with App Store availability check.
- Native iOS notification permission, Keychain installation identity and token registration.
- Encrypted backend registration store, APNs transport and protected draft/preview/send CLI.
- Sending disabled by default; no deployments or announcements.

# 1.1.0-preview.5 (8)

- Ivory/green theme, compact tier boxes and drill-through record details.
- Free enforcement aggregates, copy actions and tax link retained.
- Pro $2.99/month and AI Premium $9.99/month displayed; release purchases remain unavailable.
- Schools, crime and AI question screens disclose unconnected services.
- Optional App Store update notice with debug preview; no remote pushes configured.
- iOS project, plugins, logo assets and setup helper unchanged. Fresh-checkout instructions corrected.

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
