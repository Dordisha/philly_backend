# Update policy and push announcements — preview.6 (9)

Implemented in source; not deployed, not configured with Apple credentials and no announcements sent.
The green/gold logo is approved in branding/approved-logo.md. Existing app icon files remain intact
until production assets are exported from that approved design.

## Required updates
GET /api/app-policy reads server environment variables:
- IOS_REQUIRE_UPDATE=false (default). Set true only after the update is available in the US App Store.
- IOS_MINIMUM_VERSION=1.0.0
- IOS_RELEASED_VERSION=1.0.3 (must be >= minimum)
The app checks on launch and foreground. A newer required minimum must also be confirmed against
Apple's live listing before blocking. Invalid policy / initial network failure leaves the app usable.
An already verified block remains during a later network failure. Update and Check again remain available.
Setting IOS_REQUIRE_UPDATE=false allows rollback on the next successful check. No arbitrary remote URL
or remote markup is opened. This is a UI compatibility gate, not API authentication or a security boundary.
Old versions without this code cannot be blocked retroactively by this feature.
Only the US storefront is covered; expand availability checks before distributing more widely.

## Apple setup (owner account required)
Enable Push Notifications for App ID com.dorian.phillyscan in the Apple Developer account; refresh
provisioning profiles. Debug uses development (sandbox); Profile/Release use production. Confirm the
signed app's aps-environment matches this setting, especially custom signing workflows.
Create an APNs P-256 .p8 key with the appropriate topic/environment access. Put the private key in the
server secret manager, never GitHub, the mobile app, or chat. Configure APNS_KEY_ID and APNS_TEAM_ID.
No new Flutter plugin is needed: native iOS permission/device-token APIs are bridged to Dart.
User must tap Settings > Update announcements > Enable; the app never prompts on launch.
Denied permission opens iPhone Settings on request. Turn off unregisters locally and deletes the server
registration; failed deletion retries on foreground. The per-installation secret is stored in Keychain.
APNs tokens are refreshed at launch; changed tokens update the registration. Notification tap opens the
fixed, verified App Store listing. No location, property searches or owner names are registered.

## Backend setup (single-instance pilot)
Do not enable until the following storage and secrets are configured:
- PUSH_REGISTRATION_ENABLED=true
- PUSH_STORE_PATH: absolute filename on a PRIVATE PERSISTENT mounted disk
- PUSH_STORE_KEY: 32 random bytes encoded as 64 hex characters (encrypts APNs tokens at rest)
- PUSH_ADMIN_TOKEN: cryptographically random secret, at least 32 characters
- APNS_KEY_ID, APNS_TEAM_ID, APNS_PRIVATE_KEY: Apple APNs signing configuration
- PUSH_SENDING_ENABLED=false until signed-device sandbox tests are complete
Storage is atomic JSON with mode 0600. Back up the key and data securely. Run exactly one Node process;
multiple instances require transactional shared storage/queue. No disk or services were provisioned here.
Anonymous installation registration is possession-secret authenticated, not App Attest. Rate limits and
capacity caps exist; configure edge rate limits before public exposure. The app's private installation
secret permits only its own registration/opt-out, never administrative sends or other users' records.
APNs accepts only tokens for the correct app topic; a successful HTTP 200 means accepted, not delivered.

## Secure announcement workflow
On a trusted admin machine set PHILLY_ADMIN_BASE_URL=https://your-host and PUSH_ADMIN_TOKEN securely.
Create announcement.json: {"title":"Philly Scan update","body":"A new release is available.","environment":"sandbox"}
1. node tools/announcements.mjs draft announcement.json — creates a draft, sends nothing.
2. node tools/announcements.mjs preview ID — review exact text, environment and recipient count.
3. Explicitly authorize sending, set CONFIRM_ANNOUNCEMENT_ID to that ID, then run:
   node tools/announcements.mjs send ID
Production and sandbox audiences are separate. Drafts expire after one hour. Repeated send requests
are rejected. Opted-out/changed tokens are skipped. 410/invalid-token responses remove stale tokens.
Transport failures are recorded as unknown, never silently retried (to avoid duplicate alerts).
A crash leaves sending/attempting records for manual inspection; do not blindly recreate the campaign.
Pilot sender is capped at 200 recipients, expires registrations from targeting after 90 days without
refresh, and does not support background job delivery. Scale to a durable queue/database before a larger
launch. Review retention periodically; store caps require archival after 1000 announcements.
The admin CLI is the management interface for this preview; no public admin web UI is exposed.

## Acceptance before release
- Required update: older/equal/newer, invalid policy, unreleased version, offline cold start, rollback,
  resumed app, nested pages and App Store failure.
- Signed iPhone: enable, deny, Settings re-enable, device registration, token refresh, offline retry,
  opt-out, uninstall/reinstall, sandbox and production entitlements.
- Preview announcement to consenting test devices only; confirm foreground/background/tap behavior.
- Unauthorized sends, wrong device secret, duplicate send, APNs invalid token and server restart.
- Simulator compilation cannot verify APNs provisioning, authorization prompts or physical delivery.

Sources:
https://developer.apple.com/documentation/usernotifications/registering-your-app-with-apns
https://developer.apple.com/documentation/usernotifications/sending-notification-requests-to-apns
https://developer.apple.com/documentation/usernotifications/establishing-a-token-based-connection-to-apns
