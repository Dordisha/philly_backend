# Remove AWS from the active Philly Scan backend

This branch replaces the active OPA Athena lookup with the City's public CARTO SQL API.
It is prepared for staging; it has not been deployed.

## What changes

- Existing /api/opa/search, /suggest and OPA-detail JSON shapes remain supported.
- Suggestions accept both query and q.
- Property results include owner, sale date/price, market value, zoning and tax link.
- Public-data calls have a 15-second deadline and a 60-second, 256-entry per-process cache.
- Concurrent identical queries share one request.
- City errors return 503/504 instead of false successful empty results.
- Enforcement uses the same checked/timed upstream client; invalid OPA values are rejected.
- Runtime Athena imports and debug endpoints are removed, as is the AWS SDK dependency.
- Dormant legacy files elsewhere in the repository are not a supported startup path.

## Matching decisions

Search resolves an exact normalized street address or an OPA ID. It does not silently
select a nearby property. Multiple exact parcels return HTTP 409 and require selection.
Autocomplete supports exact-house, range-containing and same-block alternatives;
directional prefixes are constrained. A selected suggestion should be scanned by its OPA.

A 404 address miss returns an empty suggestions list deliberately: the existing mobile
client automatically scans the first 404 suggestion, which can select another property.
Autocomplete remains available before submitting a search.
Partial street-only scans and limit>1 fuzzy result lists are not supported by this branch;
the recovered mobile client uses full-address/OPA scans with limit=1.

City data is an upstream dependency. Availability is not guaranteed. Cache is in memory,
is not shared between instances, is lost on restart and does not serve expired data
during an outage. A cold request may take several seconds.

## Staging

1. Deploy this branch to a staging Render service using Node 22 or newer.
2. Build command: npm ci
3. Start command: npm start
4. Health check: /healthz
5. No AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN, ATHENA_* or S3 settings
   are needed for the active server. Keep old configuration available for rollback until
   testing is complete; do not delete AWS data/resources as part of this change.
6. /api/opa/_ping must report dataSource: "city-carto" and the deployed commit.
7. Verify the checks below, then point a test mobile build at this service with API_BASE_URL.

The Flutter source has separate property/violations base URLs. For full staging testing,
both must use the staging service; changing API_BASE_URL alone does not update the
hardcoded violations URL. Consolidating that client configuration is a separate task.

## Checks

Run npm test for deterministic unit/HTTP contract tests.

Live GET checks:
- /api/opa/search?opa=363258000
- /api/opa/search?address=1400%20John%20F%20Kennedy%20Blvd%20Philadelphia%20PA%2019107&limit=1
- /api/opa/suggest?query=1539%20S%20Lam
- /api/opa/suggest?q=526%20MARK
- /api/opa?opa=363258000
- /api/violations/summary?opa=112273500

Check exact returned identity and field shapes; values/counts can change.
For nonexistent addresses verify 404 with no automatic substitution.
For upstream errors verify 503/504, not "no records"/Low risk.

## Production and rollback

Identify the hostname actually called by the installed App Store app before promotion.
Historical hosts include philly-backend-main.onrender.com and
philly-backend-main-q3hv.onrender.com. Do not assume they are interchangeable.
Deploy the tested commit to that existing service to preserve the mobile endpoint.

Rollback requires deploying the prior commit and valid AWS configuration; the old path
currently has an AWS authentication failure, so rollback is not a verified working restore.

## Scope boundaries

No AWS account/resources were deleted. Existing billing remains separate.
No Render settings, production deployment or App Store binary changed.
No monetization, complaints dataset migration, risk-score redesign or frontend redesign.
The complaints route remains existing experimental code and is not claimed to be Philly311.

Official data catalog:
https://opendataphilly.org/datasets/philadelphia-properties-and-assessment-history/

