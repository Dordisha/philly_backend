# Philly Scan Pro — release candidate
## Two distinct builds
- Preview 10 (13): existing unlocked simulator for inspecting features. Keep this available.
- Pro candidate 16: production API, preview and internal bypass disabled, StoreKit subscription purchase/restore/redemption. A simulator artifact checks native compilation and UI; actual Apple checkout requires signed iPhone/TestFlight testing.

## Current verified state
- Production records repair was deployed and verified in the prior session; do not repeat the old claim that the repair is still pending.
- Published privacy policy is configured in release-settings.json.
- Apple subscription product was created: com.dorian.phillyscan.pro.monthly, one month. Description and US availability were entered. Verify saved USD 2.99 price.
- Flutter analysis and both existing test configurations passed on source commit fcf1a8b05d32e686e8c628c2420003892abb6247. Mac compilation was cancelled; that is not evidence of a compiler error.
- No signed TestFlight upload or end-to-end purchase success has been verified.

## Offers agreed with Dorian
- One lifetime owner entitlement, proposed label PROBRO77: NOT IMPLEMENTED OR ACTIVE.
- Five offers: one month free, then USD 2.99/month unless cancelled.
- Fourteen offers: one week free, then USD 2.99/month unless cancelled.
- Apple redemption UI exists in source. No offer has been confirmed created/activated in App Store Connect. Proposed funny labels are not functioning codes.
- Confirm Apple-supported custom-code redemption limits and durations before promising individual single-use labels.
- Clearly disclose automatic renewal at redemption.

## Next delivery steps
1. Obtain a successful native build of the Pro candidate.
2. Prepare signed iPhone archive in Xcode using production settings, build 16 or a higher unused build number.
3. Upload through App Store Connect, NOT TestFlight Internal Only, to preserve eligibility for eventual App Store submission.
4. Install on iPhone through TestFlight; confirm product price loads and capture genuine subscription screen for review.
5. Finish subscription review metadata and activate agreed offers.
6. Review device acceptance scenarios with Dorian before running them: purchase, cancellation/pending, restore, expiration/revocation, redemption, all four Pro features, and AI exclusion.
7. Resolve lifetime owner access, complete device validation, then submit for App Review. Do not claim readiness based on simulator compilation alone.

## Scope
Pro: detailed 311, L&I, recorded-owner search, Zillow links.
AI features remain coming soon and excluded from paid Pro.
StoreKit verifies Apple transactions on device. This is not server-side authorization for private or costly APIs.
