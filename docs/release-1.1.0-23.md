# Philly Scan 1.1.0 build 23 — preparation, not release approval

Prepared November 24, 2026. Build 22 remains the accepted simulator baseline at 54794f394eebeee5647f579e4272801937aa4580. Build 23 adds an app privacy manifest, explicit privacy-link configuration and device-archive packaging; it does not add paid data or new product features.

## Contents and opening

The compiled `PhillyScan-1.1.0-23.xcarchive` opens in Xcode Organizer. It is an unsigned iPhoneOS release archive, not a simulator app, signed IPA or TestFlight build. Signing and export must be verified with the owner's Apple team; no certificate/private key is bundled. Source is tracked in PR #59. Prepared archive and test results belong to the exact commit recorded in BUILD-EVIDENCE.txt.

## Required before upload or public release

| Gate | Evidence / next action |
| --- | --- |
| Native regression | Build22 passed one live Lambert test on iPhone17ProMax/iOS26.5. Final build23 focused rerun and compile results must be linked in #18. Full five-case #19 approval is separately requested; no assumed >=95% pass. |
| Production compatibility | Archive uses philly-backend-main.onrender.com. Production is older than the v2 summary client. The pre-Nov16 source parser requires numeric legacy fields; exact public1.0.3 source/host mapping remains unknown. Determine old-client behavior and prepare versioned API/compatibility rollout before any deployment. Do not invent legacy counts/risk or deploy only one side. |
| Privacy | Embed the UserDefaults CA92.1 manifest, confirm actual archive resource and third-party manifests. Reconcile optional notification identifiers, provider logs and retention with policy and App Store disclosures. Signed-out links must be rechecked after authorized edits. |
| App Store links and copy | Draft only: Privacy Policy points to the owner-supplied policy URL after corrections; Privacy Choices may use that URL if request instructions remain usable; Support retains a working support destination. Remove old paid-data marketing from pending-version copy/screenshots. No Connect changes yet. |
| Legacy subscriptions | Inventory monthly/annual products, territories/status, offers/codes and dated subscription/event/redemption reports. Missing access is unknown, not zero customers. No sale/renewal/entitlement/refund/notice changes without owner approval. |
| Licensing and crash visibility | Retain existing release review. Confirm source use/attribution and operational retention; verify diagnostic/crash visibility rather than assuming no SDK means no crash evidence needed. |
| Apple signing | Authenticated Apple team, signing/provisioning and Organizer validation/export still pending. No distribution certificate or profile available to this build job. Confirm build23 has not been uploaded already. |
| Final approval | Owner decides production rollout and Store upload/submission only after a concrete tested candidate and resolved gates. No release date promised. |

## Existing five native scenarios for approval

On iPhone17ProMax, final source and matched test server, preview/internal false:
1. Exact property and free records: Lambert OPA363258000, copy address/owner/OPA, raw-status summary, real311/L&I pages, valid zero versus failed lookup and source attribution.
2. Free owner/co-owner search: names/addresses/OPAs, pagination where enough live matches exist, no duplicates, exact property navigation and distinct error/empty states.
3. Public presentation: Schools absent throughout results; AI clearly coming soon; no Pro/$2.99/purchase flow.
4. Crime: Lambert at402/805/1609m; dated counts agree with current City queries; unavailable never zero.
5. Recovery and identity: offline/retry and property switching do not show stale or neighboring records.

Include the buyer comprehension observation within case1, not as another gate. Source attribution in the pinned violation card is plain text; do not claim it is a clickable official link. These cases require the existing #19 approval, which is not implied by its administrative closure.

## Proposed App Store copy (not published)

**What's New**
Philadelphia property records are now free, including owner search, 311 requests, L&I records and nearby reported crime trends. This update improves record loading and keeps source dates and limitations visible. Our AI homebuyer assistant is coming soon and is not available to purchase.

**Review notes**
No account or subscription is required for public records. Search 1539 S Lambert St, confirm property account number363258000, then open311, L&I, owner search and crime trends. The AI section is informational only. Existing Apple subscriptions can be managed through Apple's subscription settings. Public records do not establish safety, property condition or owner intent. Review-note data counts must be refreshed before submission.

**Privacy Policy URL after content approval**
https://smoggy-silence-f88.notion.site/Philly-Scan-Privacy-Policy-4107f2ef2cfa81d7b614d0600521d606

No production, App Store, public policy, billing, outreach, paid-service or schedule change is authorized by this preparation document.
