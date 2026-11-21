# Philly Scan 1.1.0 (19): free launch candidate

## Included
- Free property facts, 311 details, L&I records, owner and co-owner searches, Zillow links, and reported neighborhood crime trends.
- No $2.99 Pro upgrade, purchase, redemption, or StoreKit bridge. No ads or advertising SDK in this candidate.
- AI homebuyer assistant has an informational “Coming soon / Under construction” card and planned capability details. No AI checkout or active AI chat in production.
- Planned AI price: $12.99/month, with no payment collected in this release. Planned research includes similar sales, buying/ownership costs, first-time buyer steps, questions/checklists, and Albanian, Spanish, Vietnamese, Chinese and English explanations. These are plans, not currently delivered functionality.
- Native build numbering now follows Flutter's build number instead of the stale Xcode project value 11.

## Test on your Mac and iPhone
1. Unzip the full source package. Open `mobile/PREPARE_TESTFLIGHT.command` (right-click > Open if macOS asks).
2. The script runs dependency setup, static analysis and automated free-mode tests, prepares production configuration, and opens `ios/Runner.xcworkspace`. If a check fails, stop and retain the error.
3. Select Runner, the correct signing team and Any iOS Device; Product > Archive. Verify version 1.1.0 and build 19. If 19 has already been uploaded, use a higher BUILD_NUMBER.
4. Distribute through App Store Connect. Add the processed build to the internal testing group and install it through TestFlight.
5. On iPhone, verify these acceptance cases before public submission:
   - Fresh/no-subscription launch and property lookup; unknown address and retry.
   - Correct property/OPA retained when opening 311 and L&I; source dates/statuses and next page work.
   - Tap owner; owner/co-owner selection, matching results, load more, and selecting another property work.
   - Zillow opens externally; crime page loads or explains an unavailable source without inventing results.
   - No $2.99, Subscribe, upgrade, or promo-code screen; AI card opens plans only and states no charge.
   - Dismiss AI banner; continue scanning. Check small-screen and large-text layout and Settings source/privacy links.
   - Upgrade from installed build 17 without reinstalling; confirm the old purchase flow is gone.

## Store preparation
Remove the old Pro product from the pending version submission, update descriptions and screenshots to match free access, and submit without an AI in-app purchase. Check existing real subscription status separately: removing code does not cancel renewals. Do not reuse the Pro product ID for future AI.

Suggested What's New: “Explore Philadelphia property records free, including 311, L&I, owner search and neighborhood reports. We’ve removed the Pro upgrade. Get a preview of our upcoming AI homebuyer assistant—no purchase required.”

Review note: “All public-record features in this version are free and require no subscription. The AI information card describes a future service and cannot accept payment or generate AI responses. No in-app purchase is submitted with this version.”

## Release limits
The ZIP is source code, not a signed IPA or an installed TestFlight build. Signed iOS archive and physical-device acceptance require a Mac/Xcode and the owner's Apple account. A public launch within 24 hours is a target, not a promise of Apple review timing. Source permissions remain an unresolved review; this package is not legal clearance.

The existing production host responds to core data routes. Its app-policy endpoint returned 404 during this review; the app already fails open on a missing policy, so it does not block property use. This package does not claim new server notification/update infrastructure is deployed.

## Validation recorded November 13, 2026
- Node backend tests: 48/48 passed.
- Flutter 3.35.6 analysis: no issues found after dependency setup.
- Flutter tests: runner reported 30 passing with PRO_PREVIEW=false and 30 with PRO_PREVIEW=true. Some legacy mode-specific tests return early in the opposite mode; these are not 60 distinct acceptance scenarios.
- Production read-only probes: property lookup, 311, L&I, violation summary and crime returned HTTP 200 / ok=true. City owner lookup returned one subject and an ordered 26-row organization-owner result. These are sampled availability checks, not complete citywide data validation.
- Release preparation shell syntax and git whitespace checks passed.
- No signed iOS build, physical-device test, TestFlight upload, subscription availability change or App Store submission performed in this environment.
