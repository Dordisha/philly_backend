# Legacy L&I complaints repair — November 14, 2026

The legacy `/api/complaints/summary` and `/details` queries referenced nonexistent
`status`, `complaint`, `complainttype`, `agency` and `department` columns. A live
CARTO `SELECT * FROM complaints LIMIT 0` probe confirmed `complaintstatus`,
`casestatus`, `complaintcode`, `complaintcodename` and `unitresponsible` instead.
This dataset is L&I complaints, not the separate public 311 service-request data.

The summary stays Free and returns total records, latest complaint date and raw
complaint/case status pairs with counts. `open_count` and `risk_level` are null:
there is no validated classification map, and unknown statuses must not become
closed records or low risk. This is an intentional legacy contract correction.
The current Flutter source does not call these complaints endpoints; any other
client must handle null classification before deployment.

Details remain protected by server Pro authorization. Legacy `status`, `complaint`,
`complainttype`, and `department` aliases refer to raw source fields; raw case and
complaint statuses remain separate. No agency is invented. Exact OPA is included
and checked on each row. Pagination accepts integer limits 1–100 and nonnegative
safe-integer offsets, with cartodb_id as a same-date ordering tie-breaker.
Offset paging can still shift if upstream records change between requests.

Malformed data, property mismatch and source errors return failure, never an
empty success. Upstream calls have a 15-second deadline. Retrieval time is not
source freshness. No risk assessment or property safety claim is provided.

Validation: six new regression cases cover status preservation, true empty data,
source/malformed failures, exact property identity, pagination and authorization.
Run `npm test`. Live candidate checks must exercise the local router against
CARTO, using an injected test-only authorizer on a loopback server; this is not
Apple purchase verification or deployed access acceptance. Test Lambert OPA
363258000 summary and two one-row detail pages; ensure distinct record IDs and
matching OPA. Also run `node scripts/check-staging.mjs` and the separate staging
access audit. Old hosted errors do not invalidate a local source repair, nor does
local success establish deployment.

No simulator package is produced. Proposed iPhone 17 Pro Max cases remain in
`docs/staging-checks.md` and issue #26, awaiting approval under #19 and a Mac/Xcode
runtime. Required sequence: Lambert lookup, Schools absent, real 311/L&I pages,
crime at 402/805/1609m in test mode, owner/co-owner exact navigation, Free copying
and paid denial, then sandbox purchase/restore/revocation after session integration.
Do not deploy paid gates before authenticated app transport is ready.
