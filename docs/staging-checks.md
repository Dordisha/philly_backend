# Read-only staging checks

## Current build 24 release review — November 25, 2026

This section supersedes conflicting summary-card, Pro-gate, old-build and
"no package ready" instructions below. Older material remains as history.

Delivered app: **1.1.0 (24)** at
`2688de44e53a7dde6d2e3a7c92837f79373cfaee`. The subsequent
`2c9c575e882b991e26e93a38325bfd5ba624cb36` changes only CI error propagation
and opening instructions, not app/native/backend/dependency inputs.
[Delivery evidence and hashes](https://github.com/Dordisha/philly_backend/issues/18#issuecomment-7152239763).

The owner removed only the recorded-violation summary and its automatic request.
Do not restore it, require its zero/error copy or comprehension observation, or
deploy a backend solely to satisfy that removed UI. Keep separate L&I/311 details,
Zillow, free owner/co-owner search, crime, property facts and copy actions.

### Actual retained-feature destinations

Source inspection at the delivered commit establishes the following routing.
It does **not** establish current server availability or an in-app pass.

| Feature | Candidate destination | Required evidence |
|---|---|---|
| Address suggestion / selected-property lookup | Configured API host, `/api/opa/suggest` and `/api/opa/search` | Correct address and OPA; owner-result navigation must retain exact OPA |
| Separate property records including 311/L&I | Configured API host, `/api/records/{section}` | Correct selected property, raw statuses/IDs, pagination, completed-empty versus error, attribution |
| Crime | Configured API host, `/api/crime` | Exact OPA/address and 402/805/1609m radii; compare real City counts and periods |
| Owner/co-owner search | Direct `https://phl.carto.com/api/v2/sql`, `opa_properties_public` | Recorded owner/co-owner, conservative name matching, pagination/deduplication and exact-property navigation |
| Zillow | Retained outbound UI action | Verify actual destination from the final app; not a claim of licensed listing data |

The signing package's `release-settings.json` sets `APP_ENV=prod`,
`PRO_PREVIEW=false`, `INTERNAL_TESTING=false`, with no API override.
`ApiConfig.prodBaseUrl` is **https://philly-backend-main.onrender.com**.
Do not substitute the other production host or successful TEST-host results for
this package's production acceptance. The delivered simulator uses TEST; its
success is separate from release-host acceptance.

### Interpret existing audit results correctly

- `check-staging.mjs` remains a 12-check, fixed-TEST-host data audit. It is not a
  production or native test and cannot independently recompute City crime counts.
- `check-staging-access.mjs` remains a 20-check broader backend audit. Its
  `free-violations` summary-v2 contract is a legacy/backend check, **not a build24
  UI release dependency**. Report it separately; never change a failure into a pass
  or reuse the old denominator as a retained-feature acceptance score.
- Likewise, hosted `/api/owners` checks do not exercise the actual direct-City
  mobile owner search. Do not gate that free screen on obsolete Pro authorization.
- Preserve genuinely protected future AI interpretation checks separately.
  AI remains informational coming soon, planned $12.99/month, without purchase.

### Existing #19 review, reconciled to summary removal

The proposed five groups remain: (1) exact property, copy and separate311/L&I;
(2) free owner/co-owner pagination/navigation; (3) Schools hidden, AI coming soon,
no purchase; (4) all three crime radii against City counts; (5) offline/retry and
property switching without stale records. Remove summary-only steps from group1.
Comprehension of retained-record empty/error states and available official sources
may be observed within that review, not added as a new gate.

Dorian accepted working builds22/23. Build24 has one focused iPhone17ProMax native
case, fresh extracted Simulator launch and standalone Xcode archive evidence.
Neither that case nor the earlier owner acceptance establishes build24 owner
acceptance, the full five-case denominator or >=95% overall success. Execute only
authorized scenarios; record passed/failed/blocked/skipped and actual runtime.

Apple-team signing, Organizer validation, physical-device/TestFlight acceptance,
retained-feature production checks, privacy/Store/legacy-purchase review and final
release authorization remain separate. Linux tests do not prove native execution.
No replacement ZIP or rebuild is needed for this documentation correction.

---

Run `node scripts/check-staging.mjs` after the cycle's source changes and regression tests finish. No cron, services, subscriptions, dashboard feeds or alerts are created. Fixed TEST host; twelve sequential GETs, 45-second deadline each, no redirects/retries. Exit 1 on failure or unavailable source. Output contains the hosted commit, timestamp, latency and individual outcomes, not raw owner/complaint data.

This checks the existing staging data contract. Under the November 13 owner direction,
all City records (including owner search and crime aggregates) are free.
Run `node scripts/check-staging-access.mjs` as well to verify the version-2 summary
contract, free record routes and separate future AI interpretation protection.
An older server passing the data checker is not proof of candidate compatibility.
See [paid-route-access.md](paid-route-access.md) for current rollout blockers.

Covers deployment identity, exact OPA lookup, 311 address/ZIP, L&I identifiers/raw statuses, sales radius/subject exclusion, facts parcel, second 311 page, invalid identifiers, and crime at 402/805/1609 meters for Lambert OPA 363258000. Crime checks validate address/radius, equal consecutive 90-day periods, nonnegative integer counts, category totals, twelve distinct monthly buckets and unavailable percentage handling. A 404 crime route fails; 503 is unavailable. Valid zero counts pass. These checks do not independently recompute City counts or prove in-app behavior. Does not pin mutable record counts. Empty pages can pass schema checks; unavailable sources cannot. Repeated records across pages fail but may require investigation if upstream records changed between calls. L&I row parcel identity cannot be independently checked because the current response omits that field; envelope validation is not proof of every upstream row's identity. Retrieval timestamp is not a source-update timestamp.

Node fixture tests deliberately exercise success, wrong identity, malformed JSON, upstream unavailability, timeout, neighboring address and repeated pages. This is contract monitoring, not an appraisal, paid entitlement test or iOS acceptance.

# Proposed iOS scenario supplement for issue #19 — awaiting approval

Do not execute until the owner approves the scenario list. Environment: iPhone 17 Pro Max **simulator**, record Xcode, SDK/runtime, app version and exact PR #4 commit. The historical cases below were drafted for 1.1.0+6 / 32c1def7; reconcile them with the current candidate and the newer supplement in #26 before approval. No new package is ready; #17 clean extraction, dependencies/privacy manifests and Xcode build/run must pass first. These are proposed steps for the tester, not requests for the owner to repair the build.

| Case | Preconditions/data and steps | Expected |
|---|---|---|
| IOS-01 | TEST/debug; search 1539 S LAMBERT ST, select OPA 363258000, open Pro | Selected property retained; four sections reachable without payment; preview label visible |
| IOS-02 | Same property; open 311, next page, back | Real IDs/dates/source; no repeated row or neighboring address; complaints clearly labeled |
| IOS-03 | Same property; open L&I and record case/violation IDs | Separate raw case/violation statuses; unknown not treated as closed |
| IOS-04 | Same property; open resale; record sales/prices/areas; change adjustment | Arithmetic matches displayed inputs; assumption explicit; unverified condition and insufficient data disclosed |
| IOS-05 | Open signals | Supporting facts/rules shown; unknown listing and no verified owner intent or sale probability |
| IOS-06 | Search invalid address then select another property while first request is pending | No automatic neighboring property; stale response cannot replace current selection |
| IOS-07 | Disconnect simulator network, open records, restore network and retry | Unavailable distinct from zero; retry recovers; working sections remain reachable |
| IOS-08 | PRO_PREVIEW=false; open Pro | Paid records remain locked; no local button unlock. Full purchase/restore matrix #10 blocked until implemented |
| IOS-09 | Future Apple sandbox and AI implementation, controlled test account | #10 purchase/restore/revoke/30-analysis limit matrix must be separately expanded and approved before execution; currently blocked |

Record passed/failed/blocked/skipped per approved case with evidence. Overall acceptance requires >=95% of all approved cases passed; blocked/skipped remain in denominator and critical property/payment/data-integrity failures prevent release. No approved denominator yet, so no overall iOS pass rate can be reported.


## November 21 — current free-release native review clarification (#19)

This supersedes the historical Pro gates in IOS-01/08 above: City records and owner
search must open without a subscription in both preview modes; AI is informational
coming soon, with no purchase. Test the exact final PR #59 commit and matching hosted
backend, not the historical PR #4 baseline. Scenario approval is still pending;
administrative closure of #19 is not approval or runtime evidence.

Within the existing exact-property/L&I and failure review, record a comprehension
observation (not a new case, denominator or release gate): can a first-time buyer
identify the selected address, distinguish a completed zero-result lookup from an
unsuccessful lookup, and find the official source when available?

- Keep the selected address and exact property account number (OPA) matched to the
  response. Do not substitute a neighboring property. Introduce the expanded OPA
  label only where the number is useful; no additional identifier field is added.
- For a valid zero response, observe: “This lookup returned no City violation records
  for this property. This is not proof of safety or condition.”
- For a failed lookup, observe the unavailable message and Retry enforcement action;
  no zero count should be rendered. Retry after restoring the source.
- Successful summary loads show plain-text attribution “Source: Philadelphia L&I
  violations (CARTO).” This pinned card does not implement a clickable source link.
  Inspect any links implemented elsewhere separately and record their actual
  destinations; do not count plain text as link navigation.
- Preserve raw City statuses, technical error/empty distinctions, source attribution
  and the existing safety/condition caution. No translation work or outreach added.

Record observed comprehension separately from the approved scenario results. No
native pass is claimed from source inspection or widget tests.
