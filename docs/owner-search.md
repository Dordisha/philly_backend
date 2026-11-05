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

## November 4 interactive owner-search increment

Tax website card replaced in place. Result screen paginates, shows both recorded names and opens the selected exact OPA. Copy actions unchanged. No preview bypass on `/api/owners`; Free/invalid/expired/revoked access returns 403 `Pro subscription required.` before any upstream query. Errors distinguish access denial from unavailability. User will perform interactive acceptance.

Access provisioning: server-only `OWNER_ACCESS_GRANTS` is a JSON array of `{token_sha256, subject, tier: "pro"|"ai", source: "complimentary"|"subscription", expires_at, revoked}`. Generate a cryptographically random >=32-byte base64url credential outside the app, store only its SHA256 on the server and deliver the private credential securely to its intended recipient. Never commit credentials or embed them in builds. App granted-access entry keeps the credential in memory only. Removing/revoking a record denies subsequent requests; environment changes require service restart. No public grant-creation endpoint exists.

Subscription records additionally require `verified_transaction_id` and `verified_at`. These are trusted server records, NOT receipt verification: the Apple verifier and authenticated account binding are not implemented by this increment. Do not manually assert that a subscription was verified without purchase evidence. Production account login, automated purchase verification/restore and persistent secure session storage remain release gates. This registry is suitable for controlled individually provisioned complimentary access, not a complete subscription system.

No credentials were provisioned and no backend was deployed in this change. Until provisioning/deployment, a live authorized lookup cannot be claimed. Upgrade explicitly reports purchases unavailable in this preview rather than granting access. No change to production, pricing, crime deployment or Schools acceptance.
