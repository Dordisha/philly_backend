# Philly Scan preview 11 — source handoff

Two build paths from the same source. Requires Flutter 3.35.6, CocoaPods and Xcode on your Mac. Run `flutter doctor` first.

## Simulator
Run `./RUN_SIMULATOR.command` from this folder. All implemented preview features are unlocked without a code. If multiple devices appear choose the iPhone simulator. Existing compiled Preview10 remains usable with the repaired crime backend.

## iPhone / internal TestFlight
Run `./PREPARE_TESTFLIGHT.command` from this folder. This configures RELEASE compilation with the private test settings, then opens ios/Runner.xcworkspace. Do not open the .xcodeproj.
In Xcode select the existing Philly Scan signing team (currently YBKAAF3GF2) and confirm bundle ID com.dorian.phillyscan matches your existing App Store Connect app. Select Any iOS Device, Product > Archive, then Distribute App > TestFlight Internal Only. This package is not a signed IPA and has not been uploaded. Apple account signing/processing is still required. If build14 already exists use `BUILD_NUMBER=15 ./PREPARE_TESTFLIGHT.command` (or another unused higher number).
After Apple processes the build add it to your internal test group in the existing app; install using TestFlight. On the home screen choose Enter testing code and use PRIVATE-TEST-CODE.txt. Code unlock lasts for the running session; re-enter after restarting.

## Scope and boundaries
Test service https://philly-scan-repair-test.onrender.com; crime deployed separately at 415d19b. Full access means implemented preview features only, not unfinished AI or working purchases. The code is a local internal-build convenience, not a secure production subscription entitlement. Owner search uses live public OPA directly in preview. No production purchase bypass is installed. Keep the distribution INTERNAL ONLY: never submit this archive for public release. Normal builds without INTERNAL_TESTING cannot activate this code. Future production builds must be rebuilt from a clean configuration with APP_ENV=prod, PRO_PREVIEW=false, INTERNAL_TESTING=false and verified billing.

Source baseline: 8ef884170540e5fc2a1051f50eca9434c9271b20 with private preview additions. Native signing/compilation and phone interaction are not verified in this Linux environment.

Validation: Dart formatter and shell syntax checks passed. Flutter dependency setup was stopped by automatic approval review because it attempted cloud instance-metadata access. Flutter analysis/tests and native compilation were not completed. Run flutter analyze and flutter test on your Mac before archiving.
