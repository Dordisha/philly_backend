# Legacy L&I detail integrity — November 15, 2026

`/api/violations/details` now checks every returned row (including the pagination
lookahead) against the requested nine-digit OPA and requires unique numeric
CARTO row IDs. Missing/mismatched identity or malformed results return a generic
503, never a successful empty list. Source timeouts retain 504.

The existing `ok`, `opa`, `count`, `rows` and raw case/violation fields remain.
Added source/retrieval metadata, `limit`, `offset`, `has_more`, `next_offset`,
and the source OPA/row ID. Default limit remains 200, maximum 500. Invalid limits
are now rejected rather than silently truncated/clamped. Each page requires
current server Pro/AI authorization. Ordering adds CARTO row ID as a tie-breaker;
offset pages are not a frozen snapshot and source changes can shift rows.
The existing mobile legacy service still reads a first page only; this backend
change does not claim mobile pagination UI or runtime acceptance.

Read-only CARTO verification confirmed numeric cartodb_id and exact OPA for
1539 S LAMBERT ST (363258000). Tests cover identity/lookahead, pagination,
malformed requests, raw null statuses, empty/error/timeout and revoked access.

Scope: source only, no deployment or purchases. Apple verification, authenticated
sessions/transport, matching staging source and approved iPhone 17 Pro Max
runtime acceptance remain prerequisites. Do not deploy access gates alone.
The legacy summary risk/status mapping is unchanged and still needs review:
non-CLOSED is not independently verified as active, and missing aggregate data
can currently imply Low risk. This is an unresolved release-data gate, not a
validated safety assessment. Summary schema/client changes need joint testing.
