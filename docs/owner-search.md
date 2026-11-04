# Owner search implementation checkpoint — November 4, 2026

Issue #34; P0 Free + Pro release, then AI Premium. Internal data adapter only:
`src/lib/ownerSearch.js`. No HTTP route or mobile card is connected yet.

The adapter queries actual `opa_properties_public.owner_1` and `owner_2` using
full normalized names. It reverses simple two-part names, preserves punctuation,
and does not permute complex names or recognized entity names. This deliberately
misses some aliases/complex ordering rather than broadening to substring matches.
All results are possible same-name matches, never verified personal identity.

Pages contain address, OPA, and both recorded names. SQL deduplicates by OPA
before paging. Keyset cursor is the last displayed OPA; pages are bounded to
50 results, with one lookahead row. Source updates can change results between
pages; this is not a snapshot or an exhaustive ownership portfolio. Conflicting
duplicate OPA records select the first lexical address/name row, not a claim
that conflicting source records have been reconciled.

Before exposure:
- Authenticate account and verify current Pro/AI/complimentary entitlement
  server-side (#2/#28). No client flags, typed email, or debug query parameter.
- Resolve selected parcel's actual recorded owners, let users choose one, then
  call this adapter. Do not use this module as an unauthenticated HTTP handler.
- Replace tax card in its current position; visible Free lock with Upgrade;
  preserve free copy actions. Navigate by returned OPA, not address guessing.
- Add authorization integration tests plus owner-approved simulator coverage
  for primary/co-owner, reordered/similar names, pagination, empty/error, exact
  navigation, revocation and preview-disabled behavior.

Source tests exercise matching, query escaping, bounds, paging, duplicate and
identity rejection, empty/outage distinctions. These are not Apple purchase,
UI, simulator, or hosted acceptance. Existing release gates remain open.
