# Read-only staging checks

Run `node scripts/check-staging.mjs` after the cycle's source changes and regression tests finish. No cron, services, subscriptions, dashboard feeds or alerts are created. Fixed TEST host; twelve sequential GETs, 45-second deadline each, no redirects/retries. Exit 1 on failure or unavailable source. Output contains the hosted commit, timestamp, latency and individual outcomes, not raw owner/complaint data.

This checks the existing staging data contract, not paid-route authorization.
The November 9 protected source has not been deployed. Its paid routes require
authenticated access; do not treat anonymous successes on older staging as a
release pass. See [paid-route-access.md](paid-route-access.md) for integration
and rollout blockers before replacing the staging source.

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
