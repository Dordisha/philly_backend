# Staging negative-access audit

Run `node scripts/check-staging-access.mjs <full-candidate-commit-sha>` after the
candidate is deployed to the existing staging service. The optional SHA makes a
hosted/source mismatch fail. Omitting it leaves `source_match: null`, not verified.

This tool makes exactly 20 sequential GET requests to the fixed staging host,
with 15-second request deadlines, no retries and no redirects. It sends no real
credential and writes nothing. It does not modify a deployment or start a schedule.
Four controls check the deployed commit, exact Lambert OPA lookup and Free
complaint/violation summaries. Eight paid routes (including legacy detail routes)
are each probed anonymously and with a malformed bearer plus untrusted tier and
preview claims. Pro and AI denial codes are checked separately. A denial must be
HTTP 403, no-store, and contain only the exact generic denial envelope.

HTTP 200/204 is a failure even when empty. HTTP 404 means the route is missing,
not protected. Outages/timeouts are unavailable, never passing denials. Results
omit raw bodies and exception details. A nonzero exit means failure/unavailability.

The existing `check-staging.mjs` checks data quality on public debug staging; its
12 passes do not prove entitlement protection. This separate audit is expected to
fail on that older public deployment. Do not remove these failures or deploy gates
alone to make this audit green: authenticated app transport must be completed first.

Even all 20 passes prove only these negative-access and Free controls. They do not
prove valid subscriber access, Apple purchase/restore, account binding, expiry,
revocation, owner access, release preview exclusion or simulator acceptance. Those
remain separate gates in #2/#10/#19/#26. Crime remains AI, not Pro. The checker
implements the current #3/#26 Pro-first contract; reconcile PR #48's different
tier direction before choosing a public release candidate.

Owner simulator steps remain the proposed scenarios in `staging-checks.md` and
mobile acceptance documents, awaiting #19 approval. No new artifact is justified
by this backend audit alone.
