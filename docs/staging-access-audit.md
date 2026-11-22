# Free-record release audit

Run `node scripts/check-staging-access.mjs <full-candidate-commit-sha>` against the
existing staging service. A different hosted commit fails; omitting the SHA leaves
`source_match: null`, not verified. No deployment is performed.

The November 13 owner decision supersedes the historical Pro-first audit:
all City records, including owner search, sales and crime aggregates, are free.
Only `/api/records/signals` interpretation requires AI authorization. AI remains
coming soon and unavailable for purchase; no billing is activated by this audit.

The checker makes 20 sequential GET requests to the fixed staging host with
15-second deadlines, no retries or redirects, and no real credentials. Controls
check deployment, exact Lambert OPA and complaint/L&I summaries. Seven public
record routes and one interpretation route are each probed anonymously and with
forged bearer/tier/preview claims. Public records must return HTTP 200 with the
expected envelope; interpretation must return the exact generic HTTP 403 denial
with no-store and no data leakage. HTTP 404 means missing, not protected.
Outages/timeouts remain unavailable; results omit raw bodies and exception text.

The L&I summary must satisfy the mobile version-2 contract: exact OPA,
nonnegative integer total equal to unique raw case/violation status group counts,
valid nullable activity date, explicit unverified status mapping, and null legacy
active/history/risk fields. Valid empty records are zero with no activity date;
missing, incompatible or malformed data cannot pass as zero. A matching commit
stamp alone is insufficient. This check catches a healthy older deployment whose
summary would be rejected by the refreshed app.

The separate `check-staging.mjs` data checks do not establish this contract or AI
protection. Even all 20 audit passes do not establish simulator runtime, signing,
privacy, licensing, crash visibility, Apple purchases or release acceptance.
Future paid-AI purchase/authentication gates must not block free public records.

Follow the proposed iPhone 17 Pro Max scenarios in
`free-release-reconciliation.md`, awaiting #19 approval, and #17 delivery gates.
Do not deliver a simulator package or deploy either side of an incompatible
app/server contract based on this audit alone. Final production/submission
approval remains required.

The owner API probes validate a bounded page of unique ascending nine-digit OPAs,
nonblank addresses, matching recorded owner or co-owner (both simple name orders),
source attribution, retrieval date, ambiguity note and consistent continuation
cursor. Empty success requires explicit end-of-results metadata. This is a
first-page API check, not proof of complete ownership coverage, cross-page
stability, identity certainty or native navigation. The current mobile owner
screen queries City CARTO directly; backend API success does not validate that
separate transport, which still requires the approved native review.
