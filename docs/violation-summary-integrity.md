# Free L&I summary: recorded facts, not inferred condition

The legacy summary counted every non-CLOSED case as an active violation and supplied Low risk/zero counts when an aggregate was missing. Case and violation statuses are different fields; neither current condition nor safety follows from case closure.

The version-2 contract groups exact-OPA violation records by raw `casestatus` and `violationstatus`. It returns `total_count`, `status_counts`, latest recorded activity, source and retrieval time. Counts refer to records, not unique cases. `active_count`, `historical_count` and `risk_level` are explicitly null; speculative flags are removed. Null source statuses remain null. Empty grouped results are a valid zero, with no safety assessment.

The server rejects wrong-OPA rows, duplicate groups, missing/malformed counts or dates and unsafe numeric totals. Upstream/malformed results return a generic 503 (504 for upstream timeout), without counts/risk or internal error text. Summary remains Free; detail gates are unchanged.

Flutter requires version 2 and matching OPA, validates the total against the status groups, and displays record count plus separate case/violation statuses. Missing statuses say Not reported. Retry/unavailable behavior and Free clipboard actions remain. It deliberately rejects old summaries rather than interpreting them as zero/safe.

## Deployment and acceptance

This is a coordinated server/client contract change, source only. A new app against old staging will display unavailable; an old app against this response also rejects the missing numeric active/historical fields. Do not roll out one side alone. Current staged source and authenticated test transport must be reconciled before delivering an app. This change does not solve Apple verification/sessions, owner grants or simulator acceptance.

Automated backend and Flutter cases cover malformed/legacy responses, identity mismatch, inconsistent totals, duplicate status groups, conflicting case/violation statuses, valid zero, null statuses, Free summary and clipboard actions. These are not iOS runtime acceptance.

Proposed iPhone 17 Pro Max cases, awaiting #19 scenario approval: open Lambert (363258000) against matching staging; compare raw counts/statuses with the City response; verify no Active/History/Low-risk or speculative flag labels; test unavailable and retry, a verified zero-result property, unknown statuses, copy actions and property switching. Existing Schools/crime/owner delivery gates still apply. No replacement simulator package until #17/#26 gates are met.
