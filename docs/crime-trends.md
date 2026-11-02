# Reported crime trends — preview.8 (11)

Source: Philadelphia Police Department incident records, via OpenDataPhilly and the City's CARTO SQL API (`incidents_part1_part2`). https://opendataphilly.org/datasets/crime-incidents/

GET /api/crime?opa=782273400&radius_m=402. Allowed radii: 402, 805, 1609 meters (approximately quarter, half and one mile). Resolve the exact OPA parcel geometry; never use fuzzy address matching. Two spatial filters bound the city query, with the final radius measured in meters. Invalid parcels, missing coordinates and upstream failures are explicit errors, never zero results.

Counts are published incident records grouped by dispatch date and city offense category. They are not confirmed crimes, unique victims, property-specific events, population-adjusted rates or safety scores. Missing geometry is excluded. The app explains approximate locations and reporting/revision delays. No exact incident addresses or individual records are returned.

Two equal 90-day windows end on the earlier of yesterday and the latest citywide source date. A percentage is suppressed for zero baseline or a source date more than 14 days old. Show dates regardless. Monthly history covers 12 complete calendar months preceding the comparison end. Source maximum date is not proof that all reports for that period are complete. No AI model computes these counts.

Live read-only verification November 2, 2026: source latest date 2026-11-01; exact City Hall OPA 782273400 resolves to 1400 JOHN F KENNEDY BLVD (39.95249859960501, -75.16383689593911). Quarter-mile SQL returned 1,302 daily/category aggregates for 2025-11-01 through 2026-11-01. Current 90 days: 684; previous: 585; change: +16.9%. These are an observed snapshot, not fixtures or hardcoded app values.

Validation: backend boundary/zero-baseline/staleness, parcel/radius validation, missing location and upstream-error tests; Flutter selected-OPA/radius/error/retry test. Shared CARTO client enforces timeout and bounded short-lived cache.

Not deployed. The configured backend must receive this route before the simulator loads these data. Current Debug preview access is unchanged; production AI Premium requires the pending server-side entitlement system before monetized release. No new paid service or API key is needed for this public data connection.
