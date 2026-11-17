# Free release reconciliation — November 17, 2026

Owner direction November 13 supersedes historical Pro-first documentation. All City records, including owner/co-owner, detailed 311/L&I, raw nearby sales and reported crime aggregates, are free. The immediate public app removes Pro billing and displays future AI as coming soon. Planned AI is $12.99/month, not purchasable. No charges activated.

This candidate reconciles PR48 e0b2ba0 with PR58 8a81248 and its preceding reliability fixes. Raw /api/records/facts remains free without signals; the separate /api/records/signals interpretation layer requires AI authorization; it is not a public-record entitlement. Grant validation and verification timeout protections remain for future AI. Public routes never invoke subscription verification. Superseded paid-owner UI tests are replaced by free access/identity tests. Owner search preserves strict OPA, conservative owner/co-owner matching, cursor checks and stale-request guards. Public app owner lookup currently uses City CARTO directly, as in PR48; server owner route is also free. Review both paths before release.

Version 1.1.0+20 identifies source only. No download, simulator runtime acceptance, signed archive or production release is implied. The version-2 L&I summary requires matching backend/client rollout; old staging is incompatible. Do not deploy either side alone. Existing build19 instructions are historical; use this candidate only after gates pass.

## Proposed iPhone 17 Pro Max cases (awaiting #19 approval)
1. Matching backend/app: Lambert exact OPA363258000; Free copy address/owner/OPA, real311 and L&I pages; preserve record statuses and error versus valid zero.
2. Owner/co-owner selection, two pages, deduplicated OPAs and exact property navigation, with PRO_PREVIEW=false and no subscription. Invalid source identity displays unavailable.
3. Schools tile absent after scrolling entire results; AI coming-soon is informational, no buy/subscribe flow; no $2.99 offer.
4. Staging crime402/805/1609m agrees with City data; unavailable never displays zero. Verify app flow in staging separately from hosted API.
5. Offline/retry and property changes do not show stale records. Privacy/signing/crash visibility review before release.

No approved denominator, no iOS pass percentage. Production merge/deployment, App Store submission and charges require owner final approval. Apple purchases/authentication are future AI gates, not blockers to free City records. Render control plane requires confirmed existing workspace; read-only hosted checks are independent evidence.
