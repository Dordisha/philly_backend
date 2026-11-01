# Philly Scan mobile — canonical Flutter source

This directory is the proposed canonical mobile project, recovered from the owner's
Archive.zip (Library libfile_2ff54f53d0ec81918347d8e9f5cc005e;
SHA-256 5be8b2f81e068a0f55250aef21648238c22d95e2055654ed9dbacd17f3cc7402).
The archive identifies Flutter 3.35.6 / Dart 3.9.2 and app 1.0.3+4.
Generated caches, Pods, machine-specific paths, backups and IDE files are excluded.
Bundle identifier and signing team are retained; no signing credentials are included.
The backend remains at repository root. Do not merge to deploy a mobile preview.

## Build and validate

Use Flutter 3.35.6. From this directory:

```sh
flutter pub get
flutter analyze
flutter test
flutter run --dart-define=APP_ENV=test
```

Both lookup and enforcement use `lib/config/api_config.dart`. Default is TEST,
including release builds, to prevent accidental production testing.
TEST uses the existing free service https://philly-scan-repair-test.onrender.com.
Production explicitly requires `--dart-define=APP_ENV=prod`.
An optional `--dart-define=API_BASE_URL=https://host` overrides both services.
HTTPS origins only; unknown environments fail at startup.
A 45-second client deadline allows time for free staging cold starts; timed-out
reads are not retried automatically. Editing invalidates in-flight UI updates.

## Owner iPhone acceptance (requires macOS/Xcode and device signing)

1. Run the TEST build on the iPhone. Scan City Hall by address and by OPA.
   Confirm the property facts and enforcement summary load.
2. Type quickly and backspace while suggestions load. Only current choices may appear.
3. Enter an invalid address. No neighboring property may appear automatically.
   Select an alternative only by tapping it.
4. Begin a scan and immediately change the input. The old result must not appear.
5. Turn off Wi-Fi/cellular. Scan, confirm a friendly failure and usable Scan action;
   reconnect and retry. Long requests must stop within 45 seconds.
6. Verify enforcement failure leaves property facts available and offers Retry enforcement.
7. Verify enforcement refreshes after switching properties; details remain Coming Soon
   unless the explicit debug Pro preview below is enabled.

## No-payment Pro simulator preview (issue #5)

From `mobile/` on your Mac, select your iOS simulator and run:

```sh
flutter run --debug --dart-define=APP_ENV=test --dart-define=PRO_PREVIEW=true
```

Tap **Upgrade to Pro** on the home screen (no scan needed), or tap
**View enforcement details** after a successful scan. Expand the 311, open
violation and renovation resale sections. No purchase, Apple login or payment
service is used. All records/prices are clearly fictional UI examples; no sample
is associated with a scanned property. Real data integrations and a defensible
valuation method remain backlog work. This is not a valuation engine.

Without the flag, the Coming Soon gate remains. Profile/release builds ignore
the flag through `kDebugMode`; it never grants a backend entitlement. Do not
replace real subscription verification with this preview. Validate both paths:

```sh
flutter test
flutter test --dart-define=PRO_PREVIEW=true
```

These tests do not create an installable binary. Device/signing, purchase/restore,
TestFlight and App Store validation remain separate release gates. Keep changes on
this feature branch until owner testing and explicit release direction.

## Next monetization gate

Issue #2 requests paid enforcement details. In the later owner conversation, the
owner accepted the recommendation of $2.99/month and $24.99/year. This replaces
the earlier unspecified billing-period assumption, but does not authorize activating
charges. No purchase products, entitlements, AI calls or notifications are added here.
Next: design server-verified entitlement and sandbox purchase/restore before release.
Android files are recovered scaffolding, not a verified Android release configuration.
