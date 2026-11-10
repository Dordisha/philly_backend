# Paid route enforcement — November 9, 2026

Source-only increment on the Pro candidate, after PR #50. Implements part of
#2/#10; it does **not** complete purchase verification, account authentication,
session issuance, permanent owner access, or release acceptance.

Every request to these routes requires an approved server decision before any
property/source query. Pagination does not reuse a previous approval.

| Routes | Required access |
| --- | --- |
| `/api/records/311`, `/api/records/violations` | Pro or AI |
| `/api/complaints/details`, `/api/violations/details` (legacy paths) | Pro or AI |
| `/api/owners` | Pro or AI |
| `/api/records/sales`, `/api/records/facts`, `/api/crime` | AI |
| `/api/opa`, `/api/opa/search`, suggestions, enforcement summaries | Free, unchanged |

`facts` includes the signals response; Free property facts and copy fields
remain available from OPA lookup. AI includes Pro; Pro does not include crime,
resale or signals. No AI purchase availability is added.

`requirePaidAccess` awaits a strictly boolean `true` result, denies other values
with 403, and returns 503 when verification fails. Successful paid responses,
denials and errors use `Cache-Control: no-store`. Client tier headers, query
parameters, preview/internal flags and local StoreKit state do not authorize
the backend. There is no HTTP or environment preview bypass in this gate.

The existing server-owned `OWNER_ACCESS_GRANTS` registry is reused, with the
same token hashes, expiry and revocation rules. An AI grant can satisfy Pro,
but a Pro grant cannot satisfy AI. These records are **not Apple receipt
verification**. Subscription transaction fields must only come from a trusted
verifier, which is still missing. No grants, secrets, sessions or new services
were provisioned by this patch. Registry changes require service restart;
this is not yet immediate account-bound revocation administration.

## Integration and rollout gates

- The current mobile candidate does not attach an authenticated session to
  paid data requests. Deploying this source alone would deny those requests,
  including Debug crime/records previews. Do not deploy as a working purchase
  or preview integration. Complete secure account/session transport and the
  Apple verification exchange first, then verify controlled test access.
- Existing staging is a different commit. Anonymous data responses there are
  read-only data checks, **not** evidence of this gate or paid-launch readiness.
  The existing checker deliberately checks that older staging data contract;
  it does not test authenticated success or unauthorized denial. A future
  protected deployment needs separate denied/authorized staging acceptance.
- Preserve the current working staging crime investigation until authenticated
  test access is available. No staging or production deployment in this run.
- Test Apple sandbox purchase/restore, server account binding, expired/revoked
  sessions and owner entitlement before enabling paid use. Then run the
  owner-approved iPhone 17 Pro Max scenarios under #19/#26, including all three
  mandatory fixes. No simulator artifact is produced by these Node tests.

## Automated evidence scope

`test/paidAccess.test.js` mounts the actual route factories with stubbed public
data and exercises the real grant resolver. It covers all eight paid paths,
legacy aliases, case/trailing-slash variants, client spoofing, Pro versus AI,
expiry, revocation between requests, verifier outage/malformed approval, and
Free lookup/ownership fields/summaries. Data-contract suites explicitly supply
test authorization so their existing matching/error/zero-count assertions
continue to exercise data behavior. Fixture grants are not real subscriptions.


## November 10 — malformed/conflicting registry protection

A duplicated token hash now returns verification-unavailable (503) regardless
of record ordering, including when an active record precedes a revoked record.
No upstream query occurs. Correct the server registry to one authoritative
record per token; do not resolve conflicts by keeping the first entry.
Malformed subjects, revocation values, expiry types and subscription verification
fields deny access (403). Subscription verification cannot be future-dated.
Omitted `revoked` remains compatible; explicit `false` is accepted, while string
or numeric values are rejected. Valid Pro/AI tier behavior is unchanged.

These are registry integrity checks, not Apple signature verification or secure
account/session issuance. Subscription timestamps/IDs still must be populated
by the future trusted verifier. No live grants were changed or provisioned.
