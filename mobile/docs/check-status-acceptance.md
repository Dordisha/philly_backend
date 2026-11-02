# Check status — owner review scenarios

Issue #15. Replaces automated status collection with an outbound Zillow address search.
The current data model has no verified Zillow property URL/ZPID; do not fabricate one.
The search includes the resolved property address and any unit present in that address.
Philadelphia, PA is added when the local resolver returns only a street address.
No provider requests occur until the user taps. No listing status is inferred or stored.

## Proposed device scenarios — awaiting owner review
1. Scan a Philadelphia rowhome, tap Check on Zillow, and confirm the destination search corresponds to the scanned address.
2. Repeat with a condo/apartment; verify the unit is present and choose only the matching unit.
3. An address with #, / or punctuation stays within the search path, without becoming a URL fragment/query.
4. A missing/multiple-match Zillow result does not label the property off-market in Philly Scan.
5. A browser launch failure displays a retry message and keeps the property visible.
6. Return from Zillow to the same property; scan another address and verify the outgoing search changes.
7. Confirm the action is accessible from property results without Pro and also appears in the Pro overview.
8. Inspect the card on a narrow iPhone screen and with larger text.

These scenarios have not been executed on Xcode/iPhone. Zillow redirect behavior and exact-property resolution are not verified by Flutter analysis/tests.
