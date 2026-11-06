# Philly Scan 1.1.0 (18) — Free records, AI coming later

This replaces purchase candidate 16 / uploaded TestFlight build 17.

## What changed
- Property facts, 311, L&I, owner search and Zillow links require no subscription.
- The $2.99 Pro purchase screen and native StoreKit product integration are removed.
- AI is planned at $9.99/month, clearly unavailable for purchase. No AI entitlement is sold or granted.
- Settings explains independent operation and links to free City datasets.
- No ad SDK/account is configured. This build displays no ads and collects no ad identifiers. Advertising requires publisher IDs, consent/privacy configuration and separate device testing before activation.
- Internal debug previews remain isolated from the production configuration.

## Prepare on your Mac
1. Download this branch's complete source ZIP and unzip it.
2. Open mobile/PREPARE_TESTFLIGHT.command. It prepares production configuration with preview/internal flags false and build 18.
3. In Xcode select Runner, your signing team and Any iOS Device, then Product > Archive.
4. Distribute using App Store Connect; install 1.1.0 (18) from TestFlight after processing.

The script prepares configuration; Xcode performs the native archive. A source ZIP is not a compiled app.
If Apple has already received build 18, run with a higher BUILD_NUMBER instead.

## App Store Connect changes still required
- Stop offering com.dorian.phillyscan.pro.monthly and remove it from any pending submission. Do not repurpose its product ID for AI.
- Check whether there are any real subscribers before changing availability; removing the in-app purchase UI does not cancel existing renewals. Manage existing subscribers through Apple's supported process.
- Remove old $2.99 Pro and promo-offer claims from app metadata/screenshots.
- Submit this version without a new AI subscription. Create a separate AI product only when the promised features and purchase/restore flow work.
- The app's free public-record offer does not establish permission for commercial data reuse. Obtain City clarification before paid/ad-supported launch.

## Device acceptance checklist
- With no subscription, scan a property; open 311, L&I, Zillow and owner search.
- Verify correct property/OPA, published statuses, pagination and failure/retry behavior.
- Confirm there is no Pro upgrade, subscribe, redemption or old price flow.
- Confirm AI says coming later and cannot charge.
- Open Settings > Free access, AI & sources; verify source and privacy links.
- Verify notifications and normal navigation still work.

Backend owner API must deploy this branch's public-access change separately. The mobile owner lookup continues using its existing direct City API adapter.
