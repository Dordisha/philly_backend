# Simulator acceptance — preview.5 (8)

Not a production release. Preserve the working Mac project as described in START_HERE.txt.

1. Search an exact address; confirm returned address and OPA before opening records.
2. Free summary: confirm active/history/date/flags; an unavailable source must not show zero.
3. Copy address, owner and OPA; paste each into Notes to verify the actual value.
4. Open 311 and L&I: concise rows, tap a row to see source fields; paginate and check dates/statuses.
5. Open Zillow and confirm the address/unit yourself. This is an address search.
6. Resale: compare the displayed range with source sales; inspect insufficient-data state.
7. Signals: evidence must describe observed records, never establish selling intent.
8. Schools, Crime trends and Ask AI: boxes open; no fabricated data/answers; availability clearly marked.
9. Check for updates: current App Store version 1.0.3 at implementation time must not prompt this 1.1.0 preview to downgrade.
10. Tap Preview update notice in Debug; verify App Store button, Later, and example label.
11. Turn off connectivity: scan/records errors remain retryable; update check does not block the app.
12. Test a small iPhone and larger text size; inspect box wrapping and copy buttons.
13. Build with PRO_PREVIEW=false: detailed records and Premium tiles stay gated; free summary/copy stay usable.

## Push notification follow-up
In-app notices work only in a version containing this implementation and when the app opens.
Existing older installations cannot receive a new in-app notice retroactively.
Remote pushes need Apple Push Notifications capability/credentials, device-token registration,
user opt-in with an explanation, authenticated sending service, opt-out handling and real-device
end-to-end delivery tests. No pushes or permission prompts are sent in this preview.

## Remaining release dependencies
Verified purchases/restore and server authorization; school/crime data sources; AI service and usage limits.
Only CMX5 and RSA5 zoning descriptions are mapped; unknown codes say Description unavailable.
