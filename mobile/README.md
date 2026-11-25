# Philly Scan 1.1.0 (23) — release preparation

All City property records, 311, L&I, owner/co-owner search and reported crime aggregates are free. AI is informational and coming soon, with no purchase flow. Build 23 adds the app privacy manifest and explicitly configures the privacy link while preserving the build-22 feature fixes.

## Prepared files

- The compiled simulator ZIP runs against the existing matching test server. Unzip and drag Runner.app onto the open iPhone 17 Pro Max simulator.
- The replacement workflow creates **PhillyScan-23-Xcode-Signing.zip** with **PhillyScan.xcodeproj**, precompiled release frameworks and native sources. Open this project, select the owner team under Runner > Signing & Capabilities, select Any iOS Device, and choose Product > Archive. Flutter/CocoaPods/Terminal are not required by this delivered project. Apple signing and validation must still be completed on the owner's Mac.
- **Withdrawn delivery:** the earlier unsigned `PhillyScan-1.1.0-23-Xcode-Archive.zip` failed in Organizer with “No Team Found in Archive.” Its unsigned compilation checks did not establish signing or validation readiness; do not retry it or patch its team metadata.
- A raw GitHub source ZIP is developer source. It is not the prepared archive and is not a portable, dependency-free Xcode project.

## Release hold

The production archive uses APP_ENV=prod, preview/internal testing disabled, and the owner-supplied privacy-policy URL in release-settings.json. Production still needs a compatible backend rollout and fresh verification; the policy and App Store disclosures need the reviewed changes. The exact public 1.0.3 client-to-host mapping is not established, so backward compatibility cannot be assumed.

Apple signing/export, final native acceptance, privacy/licensing/crash review, and owner authorization are still required. Do not upload or submit this preparation archive while these gates remain open. See [release packet](../docs/release-1.1.0-23.md) and tracker #18.

## Existing subscriptions

No new subscription is sold by this candidate. Existing monthly/annual products, offers, codes, customers and renewals must be inventoried read-only before proposing any change. Removing purchase UI does not cancel renewals. Nothing in this package authorizes removing products from sale, changing entitlements, refunds or customer notices.

## Developer source setup

PREPARE_TESTFLIGHT.command remains a developer convenience for a Mac with Flutter/CocoaPods configured. Its default build number comes from pubspec.yaml. It is not the owner delivery path and does not establish native or production acceptance.
