# Philly Scan mobile — canonical Flutter source

Recovered from owner Archive.zip (Library libfile_2ff54f53d0ec81918347d8e9f5cc005e;
SHA-256 5be8b2f81e068a0f55250aef21648238c22d95e2055654ed9dbacd17f3cc7402).
Flutter 3.35.6 / Dart 3.9.2; source version 1.0.3+4. Generated caches,
Pods, machine paths and signing credentials are excluded. Backend is at repo root.

## Run the live-data preview on your Mac

Download the feature/mobile-reliability-foundation branch again; the earlier sample ZIP does not update itself.
From the downloaded mobile/ directory:

```sh
flutter pub get
open -a Simulator
flutter run --debug --dart-define=APP_ENV=test --dart-define=PRO_PREVIEW=true
```

Scan a property, then tap **311 records**, **L&I records**, **Resale value** or
**Signals** below its basic facts. Upgrade to Pro also opens the records after a
property is scanned. No purchase or Apple login is needed. Home Upgrade without
a property asks you to scan first. Existing enforcement details opens the L&I tab.

- 311: public service requests at the exact published address and ZIP. No OPA field exists in this dataset. It may include shared-building/street requests and excludes private/unpublished records.
- L&I: actual violation rows matched by parcel OPA; published case/violation statuses are preserved separately.
- Resale: nearby OPA last-recorded sales, 800 m, last 24 months, same category/building code, floor area within 25%. View actual prices/dates/areas/distances and raw OPA condition codes.
- Experimental resale range: price-per-sq-ft quartiles times subject size, using displayed-page sales >= $50,000 with valid size and at least three sales. User can select a condition scenario and explicitly adjust the assumed price by -30% to +50%. Condition selection itself adds no premium. Transaction type/renovation condition remain unverified; the interval is a benchmark, not a confidence interval/appraisal/profit calculation. Refreshing the next page changes the comparison sample.
- Signals: separate transparent context rules using last recorded sale >=15 years and different published mailing street, each with underlying evidence/explanation. These yield at most weak evidence; no direct owner intentions, sale probability or acceptable offer price is invented.
- Listing status stays Unknown until an authorized listing feed is connected.
- Records are paginated (25 per page) with retry/source/retrieved date; raw missing values stay Not recorded. A failure does not become zero records.

TEST uses existing free https://philly-scan-repair-test.onrender.com.
Both property/enforcement use config/api_config.dart. APP_ENV=prod explicitly targets
https://philly-backend-main.onrender.com; API_BASE_URL may override with an HTTPS origin.
Defaults are TEST even for release. Reads have 45-second client deadlines.
PRO_PREVIEW is debug-only; profile/release ignore the bypass. This is not a real
entitlement or purchase implementation. The existing sample UI file is retained
as unused design history and is no longer navigated to.

## Checks

```sh
flutter analyze
flutter test
flutter test --dart-define=PRO_PREVIEW=true
```

Repo root: npm test. App source changes need a fresh local run; a backend redeploy
does not update an already-built simulator client.

## Owner acceptance

1. Scan City Hall and a residential property by address/OPA. Confirm property identity.
2. Tap 311 and L&I and inspect actual IDs, dates and statuses; try Previous/Next.
3. Open Resale value for a residential property; inspect each recorded sale, change
   the scenario/price assumption, and check the estimated range changes.
4. Open Signals: each factor must cite the underlying property data and its rule.
5. Try no connectivity and retry. Switching tabs/properties must not show old records.
6. Test quick input edits, invalid addresses and explicit suggestion selection.
7. Normal build without PRO_PREVIEW must keep detail access gated.

No signed binary, TestFlight upload or store submission is created by these tests.
Keep draft until owner device testing and explicit production-release direction.
Accepted planned prices: $2.99/month or $24.99/year. Charges inactive; real
server-verified entitlement/purchase/restore validation remains a release gate.
