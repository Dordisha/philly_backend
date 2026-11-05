# Philly Scan Pro — purchase integration candidate (1.1.0 build 15)

This replaces the code-unlock preview with Apple subscriptions. It is SOURCE, not a signed IPA. Native compilation and StoreKit testing are not yet verified. Do not publish until the gates below pass.

## Apple setup before running
1. Open the EXISTING Philly Scan app in App Store Connect. Confirm its bundle ID is `com.dorian.phillyscan` (do not create a second app).
2. Monetization → Subscriptions: create one subscription group for Philly Scan. Create **Philly Scan Pro**, product ID `com.dorian.phillyscan.pro.monthly`, auto-renewable, duration 1 month, US price $2.99. If a Pro product already exists with another ID, tell the developer before creating a duplicate: the ID must match AppDelegate.swift.
3. Complete subscription localization, review information/screenshot, availability, and applicable paid-app agreement, tax and banking setup. Leave Billing Grace Period off for this first implementation; the app currently grants through verified expiration only.
4. Publish the actual privacy policy and put its HTTPS URL into `release-settings.json`. The purchase button deliberately stays disabled without it. Standard Apple Terms of Use are linked in the paywall; use the same in the listing unless you have a custom EULA.
5. Double-click PREPARE_TESTFLIGHT.command, select your correct Apple signing team in Xcode, select Any iOS Device, Archive, then upload via App Store Connect. Do NOT choose TestFlight Internal Only if you intend that uploaded build to be eligible for a later public release. Increment BUILD_NUMBER if 15 is already used.
6. Assign the processed build to testers and install through TestFlight on a real iPhone. Apple sandbox purchases do not charge testers.

## What this build does
- Real StoreKit 2 checkout, Apple-verified current entitlements, restore, pending/cancelled handling, updates and expiration. iOS 15+ required for billing (older iOS shows unavailable).
- No test code or automatic preview entitlement in the release runner.
- Pro: detailed 311, L&I, Zillow links, exact recorded-owner search. Upcoming AI features stay unavailable to paid Pro. Descriptive crime trends remain visible separately while AI is deferred.
- Owner search uses Philadelphia's public CARTO endpoint after on-device entitlement verification, retaining exact-name validation and pagination. It does not create a server grant. This protects app features, not the underlying public data/API. Any future private or costly server capability still requires server-side authentication and transaction verification.
- Restores use the Apple Account; no Philly Scan account is required. Cancellation retains access until expiration. Refund/revocation updates remove access. No locally persisted entitlement toggle.

## Critical release gate: server
Current production `https://philly-backend-main.onrender.com` returns HTTP 404 for `/api/records/311?opa=363258000` (checked Nov 5). Therefore this candidate defaults to the working test service. DO NOT publicly release the test-service build.
Before final release: merge/deploy the reviewed data repairs to production with owner approval, verify exact-address 311/L&I/crime on production, change APP_ENV to prod in release-settings.json, increment build number, then repeat TestFlight tests on THAT final binary before submitting it. An already-uploaded binary cannot have its API environment changed.

## Proposed acceptance scenarios — owner review before execution
1. Fresh install: Free summary works; every Pro entry point requires purchase.
2. Apple monthly product displays localized price; subscription details, Privacy Policy, Terms and Restore are visible.
3. Successful sandbox purchase unlocks all four Pro features for the correct property.
4. Cancelled, pending, failed and unverified purchases do not unlock.
5. Restore on reinstall/second device using same Apple Account restores active Pro; another account stays Free.
6. Accelerated sandbox expiration/revocation removes access, including an already-open records page; turning off auto-renew retains access until expiry.
7. Offline/store-unavailable/missing-product states show an actionable message and never simulate success.
8. Pro does not unlock AI/Resale/Signals; public build has no testing-code UI.
9. Production address/data regression checks pass on final build.
10. Confirm signing, app ID, subscription product, privacy URLs and Apple review metadata.

Suggested local commands after review: flutter analyze; flutter test --dart-define=PRO_PREVIEW=false; flutter test --dart-define=PRO_PREVIEW=true. Device StoreKit cases are separate and mandatory; unit tests cannot prove Apple checkout works.

## Validation in this delivery
Dart formatting/parser and shell syntax/diff checks only. Flutter setup in the prior session was blocked by automatic review because it contacted a cloud metadata endpoint. That setup was not retried or bypassed. No native Apple SDK is available here, so native compile, Flutter tests, and device purchase tests remain outstanding. This package is an integration candidate, not a claim of launch readiness.
